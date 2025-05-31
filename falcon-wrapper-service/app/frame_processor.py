# falcon-wrapper-service/app/frame_processor.py
import threading
import time
import json
import base64
import uuid
import logging
from queue import Queue, Empty
from datetime import datetime, timezone # Python의 datetime 객체 사용
import cv2
from kafka import KafkaProducer
from kafka.errors import KafkaError

# 타입 힌트를 위해 Union 사용
from typing import Union, Optional, Dict, Any
from uwb_handler import UWBHandler as APIUWBHandler # 기존 API 방식 핸들러
from uwb_pg_handler import UWBPostgresHandler # 새 PostgreSQL 방식 핸들러
# from .config import AppConfig # main.py에서 AppConfig 인스턴스를 직접 주입받음

logger = logging.getLogger(__name__)

# UWB 핸들러들의 공통 인터페이스를 위한 타입 별칭
UWBHandlerType = Union[APIUWBHandler, UWBPostgresHandler]

class FrameProcessor(threading.Thread):
    def __init__(self,
                 processing_queue: Queue,
                 app_config, # AppConfig 인스턴스
                 uwb_handler_map: Dict[str, UWBHandlerType], # main.py의 active_uwb_handlers 참조
                 shared_lock: threading.Lock): # main.py의 sources_lock 참조
        super().__init__(name=f"FrameProcessor-{str(uuid.uuid4())[:4]}")
        self.processing_queue = processing_queue
        self.config = app_config
        self.output_topic = getattr(self.config, 'OUTPUT_KAFKA_TOPIC', 'default_output_topic')
        self.bootstrap_servers = getattr(self.config, 'KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        
        self.uwb_handler_map = uwb_handler_map # main.py의 active_uwb_handlers 딕셔너리 참조
        self.sources_lock = shared_lock      # main.py의 sources_lock 참조
        
        self._stop_event = threading.Event()
        self.daemon = True # main 스레드 종료 시 자동 종료되도록
        self.producer = None
        # ✨ FPS 제한 기능 추가
        self.send_max_fps = getattr(self.config, 'SEND_MAX_FPS', 3.0)
        self.send_skip_strategy = getattr(self.config, 'SEND_SKIP_STRATEGY', 'DROP_OLD').upper()
        self.last_send_time_by_camera = {}  # 카메라별 마지막 송신 시간 추적
        self.send_interval = 1.0 / self.send_max_fps if self.send_max_fps > 0 else 0

        # 통계 변수 추가
        self.frames_received = 0
        self.frames_sent = 0
        self.frames_dropped = 0
        self.last_stats_time = time.monotonic()
        self.stats_interval = getattr(self.config, 'STATS_LOG_INTERVAL_SEC', 30.0)  # 30초마다 통계 출력

        self._initialize_producer()
        logger.info(f"FrameProcessor {self.name} initialized. Outputting to topic '{self.output_topic}'.")

    def _initialize_producer(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                max_request_size=getattr(self.config, 'KAFKA_PRODUCER_MAX_REQUEST_SIZE', 1048576),
                retries=getattr(self.config, 'KAFKA_PRODUCER_RETRIES', 3),
                acks=getattr(self.config, 'KAFKA_PRODUCER_ACKS', '1')
            )
            logger.info(f"FrameProcessor {self.name}: Kafka Producer connected to: {self.bootstrap_servers}")
        except KafkaError as e:
            logger.fatal(f"FrameProcessor {self.name}: Failed to initialize Kafka Producer: {e}. This worker will not function.", exc_info=True)
            self.producer = None
        except Exception as e_init:
            logger.fatal(f"FrameProcessor {self.name}: Unexpected error initializing Kafka Producer: {e_init}. This worker will not function.", exc_info=True)
            self.producer = None

    def should_send_frame(self, camera_id: str, current_time: float) -> bool:
        """
        프레임을 인퍼런스 서비스로 송신할지 결정
        
        Args:
            camera_id: 카메라 ID
            current_time: 현재 시간 (time.monotonic())
        
        Returns:
            bool: 송신할지 여부
        """
        if self.send_max_fps <= 0:
            return True  # 제한 없음
        
        last_time = self.last_send_time_by_camera.get(camera_id, 0)
        time_elapsed = current_time - last_time
        
        if time_elapsed >= self.send_interval:
            self.last_send_time_by_camera[camera_id] = current_time
            return True
        else:
            return False

    def log_stats_if_needed(self, current_time: float):
        """주기적으로 통계 출력"""
        if current_time - self.last_stats_time >= self.stats_interval:
            time_elapsed = current_time - self.last_stats_time
            
            if time_elapsed > 0:
                receive_rate = self.frames_received / time_elapsed
                send_rate = self.frames_sent / time_elapsed
                drop_rate = self.frames_dropped / time_elapsed
                drop_ratio = (self.frames_dropped / max(self.frames_received, 1)) * 100
                
                logger.info(f"[{self.name}] STATS - Receive: {receive_rate:.1f} fps, "
                           f"Send: {send_rate:.1f} fps, Drop: {drop_rate:.1f} fps, "
                           f"Drop ratio: {drop_ratio:.1f}%")
            
            # 통계 리셋
            self.frames_received = 0
            self.frames_sent = 0
            self.frames_dropped = 0
            self.last_stats_time = current_time

    def stop(self):
        logger.info(f"FrameProcessor {self.name} stop request received.")
        self._stop_event.set()

    def run(self):
        logger.info(f"FrameProcessor {self.name} thread started.")
        if not self.producer:
            logger.error(f"FrameProcessor {self.name}: Kafka producer not initialized. Thread exiting.")
            return
            
        while not self._stop_event.is_set():
            try:
                item = self.processing_queue.get(timeout=1.0) 
                if item is None: 
                    logger.info(f"FrameProcessor {self.name} received None (shutdown signal) from queue.")
                    break 
            except Empty:
                continue 
            except Exception as e_q:
                logger.error(f"FrameProcessor {self.name}: Error getting item from queue: {e_q}", exc_info=True)
                time.sleep(0.1)
                continue

            current_time = time.monotonic()
            self.frames_received += 1

            camera_id, image_bgr, frame_timestamp_utc, source_type, source_details = item
            
            if not isinstance(frame_timestamp_utc, datetime):
                logger.error(f"[{camera_id}] Invalid frame_timestamp_utc type: {type(frame_timestamp_utc)}. Expected datetime. Skipping item.")
                self.processing_queue.task_done()
                self.frames_dropped += 1
                continue
            
            # 🎯 핵심: 송신 FPS 제한 체크
            should_send = self.should_send_frame(camera_id, current_time)
            
            if not should_send:
                if self.send_skip_strategy == 'DROP_OLD':
                    logger.debug(f"[{camera_id}] [SEND_RATE_LIMITED] Dropping frame due to send FPS limit ({self.send_max_fps} FPS)")
                    self.processing_queue.task_done()
                    self.frames_dropped += 1
                    self.log_stats_if_needed(current_time)
                    continue  # 현재 프레임 스킵
                elif self.send_skip_strategy == 'THROTTLE':
                    # 다음 송신까지 대기
                    last_time = self.last_send_time_by_camera.get(camera_id, 0)
                    wait_time = self.send_interval - (current_time - last_time)
                    if wait_time > 0:
                        logger.debug(f"[{camera_id}] [SEND_THROTTLE] Waiting {wait_time:.3f}s to maintain send FPS limit")
                        time.sleep(wait_time)
                    self.last_send_time_by_camera[camera_id] = time.monotonic()

            logger.debug(f"[{camera_id}] Processing and sending frame (send rate: {self.send_max_fps} FPS)")

            try:
                uwb_handler = None
                # 공유 딕셔너리 접근 시 Lock 사용
                with self.sources_lock:
                    uwb_handler = self.uwb_handler_map.get(camera_id) 
                
                uwb_data_payload = None
                
                if uwb_handler: # Lock 해제 후 핸들러 객체 사용
                    try:
                        retrieved_uwb = uwb_handler.get_uwb_data(frame_timestamp=frame_timestamp_utc)
                        
                        if retrieved_uwb:
                            uwb_data_payload = retrieved_uwb
                            logger.debug(f"[{camera_id}] UWB data retrieved for frame at {frame_timestamp_utc}: {uwb_data_payload}")
                        else:
                            logger.debug(f"[{camera_id}] No UWB data (or error) for frame at {frame_timestamp_utc}, handler returned None.")
                            uwb_data_payload = {"error": "UWB data not available or error in handler"}
                    except Exception as e_uwb_call:
                        logger.error(f"[{camera_id}] Error calling get_uwb_data on handler '{type(uwb_handler).__name__}': {e_uwb_call}", exc_info=True)
                        uwb_data_payload = {"error": f"Exception during UWB data fetch: {str(e_uwb_call)}"}
                else:
                    logger.debug(f"[{camera_id}] No UWBHandler configured or found for this camera_id in FrameProcessor.")
                    uwb_data_payload = {"error": "UWB handler not configured/found for this camera"}
                
                image_format_out = getattr(self.config, 'IMAGE_OUTPUT_FORMAT', 'jpeg').lower()
                encoded_image_bytes = None
                result = False

                if image_format_out == 'jpeg':
                    jpeg_quality = getattr(self.config, 'JPEG_QUALITY', 80)
                    encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), jpeg_quality]
                    result, encoded_image_bytes = cv2.imencode('.jpg', image_bgr, encode_param)
                elif image_format_out == 'png':
                    result, encoded_image_bytes = cv2.imencode('.png', image_bgr)
                else: 
                    logger.warning(f"[{camera_id}] Unsupported output format '{image_format_out}', defaulting to JPEG.")
                    image_format_out = 'jpeg'
                    jpeg_quality = getattr(self.config, 'JPEG_QUALITY', 80)
                    encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), jpeg_quality]
                    result, encoded_image_bytes = cv2.imencode('.jpg', image_bgr, encode_param)
                
                if not result or encoded_image_bytes is None:
                    logger.error(f"[{camera_id}] Failed to encode image to {image_format_out}.")
                    self.processing_queue.task_done()
                    continue
                
                image_data_b64 = base64.b64encode(encoded_image_bytes.tobytes()).decode('utf-8')

                output_payload = {
                    "message_id": str(uuid.uuid4()),
                    "wrapper_instance_id": getattr(self.config, 'WRAPPER_INSTANCE_ID', 'default_wrapper'),
                    "camera_id": camera_id,
                    "image_timestamp_utc": frame_timestamp_utc.isoformat(),
                    "image_format": image_format_out,
                    "image_resolution": f"{image_bgr.shape[1]}x{image_bgr.shape[0]}",
                    "image_data_b64": image_data_b64,
                    "uwb_data": uwb_data_payload, 
                    "source_type": source_type,
                    "source_details": source_details,
                    "processing_timestamp_utc": datetime.now(timezone.utc).isoformat()
                }

                self.producer.send(self.output_topic, value=output_payload)
                logger.debug(f"[{camera_id}] Fused data sent to Kafka. Image: {image_format_out}, UWB: {'data available' if uwb_data_payload and 'error' not in uwb_data_payload else 'no data/error'}.")

            except KafkaError as e_kafka:
                logger.error(f"[{camera_id}] Kafka send error: {e_kafka}", exc_info=True)
            except Exception as e_proc:
                logger.error(f"[{camera_id}] Error processing frame: {e_proc}", exc_info=True)
            finally:
                self.processing_queue.task_done()

        if self.producer:
            logger.info(f"FrameProcessor {self.name}: Flushing remaining messages and closing producer...")
            try:
                self.producer.flush(timeout=getattr(self.config, 'KAFKA_PRODUCER_FLUSH_TIMEOUT_SEC', 5.0))
            except Exception as e_flush:
                logger.error(f"FrameProcessor {self.name}: Error flushing Kafka producer: {e_flush}", exc_info=True)
            finally:
                try:
                    self.producer.close(timeout=getattr(self.config, 'KAFKA_PRODUCER_CLOSE_TIMEOUT_SEC', 5.0))
                except Exception as e_close:
                    logger.error(f"FrameProcessor {self.name}: Error closing Kafka producer: {e_close}", exc_info=True)
        logger.info(f"FrameProcessor {self.name} thread stopped.")
