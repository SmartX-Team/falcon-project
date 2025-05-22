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

from typing import Union, Optional, Dict, Any # Dict, Any 추가
from uwb_handler import UWBHandler as APIUWBHandler # 기존 API 방식 핸들러
from uwb_pg_handler import UWBPostgresHandler # 새 PostgreSQL 방식 핸들러
# from .config import AppConfig # main.py에서 AppConfig 인스턴스를 직접 주입받음

logger = logging.getLogger(__name__)


class FrameProcessor(threading.Thread):
    def __init__(self,
                 processing_queue: Queue,
                 app_config, # AppConfig 인스턴스
                 uwb_handler_map: Dict[str, UWBHandlerType]): # 타입 힌트 수정
        super().__init__(name=f"FrameProcessor-{str(uuid.uuid4())[:4]}")
        self.processing_queue = processing_queue
        self.config = app_config
        self.output_topic = self.config.OUTPUT_KAFKA_TOPIC
        self.bootstrap_servers = self.config.KAFKA_BOOTSTRAP_SERVERS
        self.uwb_handler_map = uwb_handler_map
        
        self._stop_event = threading.Event()
        self.daemon = True # main 스레드 종료 시 자동 종료되도록
        self.producer = None
        self._initialize_producer()
        logger.info(f"FrameProcessor {self.name} initialized. Outputting to topic '{self.output_topic}'.")

    def _initialize_producer(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                max_request_size=getattr(self.config, 'KAFKA_PRODUCER_MAX_REQUEST_SIZE', 1048576), # 기본값 추가
                retries=getattr(self.config, 'KAFKA_PRODUCER_RETRIES', 3), # 기본값 추가
                acks=getattr(self.config, 'KAFKA_PRODUCER_ACKS', '1') # 기본값 추가
            )
            logger.info(f"FrameProcessor {self.name}: Kafka Producer connected to: {self.bootstrap_servers}")
        except KafkaError as e:
            logger.fatal(f"FrameProcessor {self.name}: Failed to initialize Kafka Producer: {e}. This worker will not function.", exc_info=True)
            self.producer = None
        except Exception as e_init:
            logger.fatal(f"FrameProcessor {self.name}: Unexpected error initializing Kafka Producer: {e_init}. This worker will not function.", exc_info=True)
            self.producer = None


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
                # 큐에서 아이템 가져오기 (타임아웃 설정)
                # item: (camera_id, image_bgr, frame_timestamp_utc, source_type, source_details)
                item = self.processing_queue.get(timeout=1.0) 
                if item is None: # Shutdown signal
                    logger.info(f"FrameProcessor {self.name} received None (shutdown signal) from queue.")
                    break 
            except Empty:
                continue # 큐가 비었으면 다시 시도
            except Exception as e_q:
                logger.error(f"FrameProcessor {self.name}: Error getting item from queue: {e_q}", exc_info=True)
                # 잠시 대기 후 계속 (네트워크 문제 등 일시적일 수 있음)
                time.sleep(0.1)
                continue

            # 아이템 언패킹
            camera_id, image_bgr, frame_timestamp_utc, source_type, source_details = item
            
            # frame_timestamp_utc가 datetime 객체인지 확인 (중요)
            if not isinstance(frame_timestamp_utc, datetime):
                logger.error(f"[{camera_id}] Invalid frame_timestamp_utc type: {type(frame_timestamp_utc)}. Expected datetime. Skipping item.")
                self.processing_queue.task_done()
                continue

            try:
                uwb_handler = self.uwb_handler_map.get(camera_id)
                uwb_data_payload = None # 최종 Kafka 메시지에 포함될 UWB 데이터
                
                if uwb_handler:
                    try:
                        # *** 수정된 부분: frame_timestamp_utc를 get_uwb_data에 전달 ***
                        retrieved_uwb = uwb_handler.get_uwb_data(frame_timestamp=frame_timestamp_utc)
                        
                        if retrieved_uwb:
                            uwb_data_payload = retrieved_uwb
                            logger.debug(f"[{camera_id}] UWB data retrieved for frame at {frame_timestamp_utc}: {uwb_data_payload}")
                        else:
                            # get_uwb_data가 None을 반환하는 것은 '데이터 없음' 또는 '오류'를 의미할 수 있음
                            # 핸들러 내부에서 이미 로깅했을 것이므로 여기서는 간단히 기록
                            logger.debug(f"[{camera_id}] No UWB data (or error) for frame at {frame_timestamp_utc}, handler returned None.")
                            uwb_data_payload = {"error": "UWB data not available or error in handler"}
                    except Exception as e_uwb_call:
                        logger.error(f"[{camera_id}] Error calling get_uwb_data on handler: {e_uwb_call}", exc_info=True)
                        uwb_data_payload = {"error": f"Exception during UWB data fetch: {str(e_uwb_call)}"}
                else:
                    logger.debug(f"[{camera_id}] No UWBHandler configured for this camera_id.")
                    uwb_data_payload = {"error": "UWB handler not configured for this camera"}
                
                # 이미지 인코딩
                image_format_out = getattr(self.config, 'IMAGE_OUTPUT_FORMAT', 'jpeg').lower()
                encoded_image_bytes = None
                result = False

                if image_format_out == 'jpeg':
                    jpeg_quality = getattr(self.config, 'JPEG_QUALITY', 80)
                    encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), jpeg_quality]
                    result, encoded_image_bytes = cv2.imencode('.jpg', image_bgr, encode_param)
                elif image_format_out == 'png':
                    # PNG 압축 레벨 설정 가능 (0-9, 높을수록 압축률 좋지만 느림)
                    # encode_param = [int(cv2.IMWRITE_PNG_COMPRESSION), 3] # 예시
                    result, encoded_image_bytes = cv2.imencode('.png', image_bgr)
                else: 
                    logger.warning(f"[{camera_id}] Unsupported output format '{image_format_out}', defaulting to JPEG.")
                    image_format_out = 'jpeg' # 기본값으로 변경
                    jpeg_quality = getattr(self.config, 'JPEG_QUALITY', 80)
                    encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), jpeg_quality]
                    result, encoded_image_bytes = cv2.imencode('.jpg', image_bgr, encode_param)
                
                if not result or encoded_image_bytes is None:
                    logger.error(f"[{camera_id}] Failed to encode image to {image_format_out}.")
                    self.processing_queue.task_done()
                    continue
                
                image_data_b64 = base64.b64encode(encoded_image_bytes.tobytes()).decode('utf-8')

                # 최종 Kafka 메시지 페이로드 구성
                output_payload = {
                    "message_id": str(uuid.uuid4()),
                    "wrapper_instance_id": getattr(self.config, 'WRAPPER_INSTANCE_ID', 'default_wrapper'),
                    "camera_id": camera_id,
                    "image_timestamp_utc": frame_timestamp_utc.isoformat(), # ISO 8601 형식 문자열로 변환
                    "image_format": image_format_out,
                    "image_resolution": f"{image_bgr.shape[1]}x{image_bgr.shape[0]}", # 원본 해상도 정보 추가
                    "image_data_b64": image_data_b64,
                    "uwb_data": uwb_data_payload, 
                    "source_type": source_type,
                    "source_details": source_details,
                    "processing_timestamp_utc": datetime.now(timezone.utc).isoformat()
                }

                # Kafka로 메시지 전송
                self.producer.send(self.output_topic, value=output_payload)
                # 성공 로깅은 너무 빈번할 수 있으므로 DEBUG 레벨 또는 조건부로 변경 고려
                logger.debug(f"[{camera_id}] Fused data sent to Kafka. Image: {image_format_out}, UWB: {'data available' if uwb_data_payload and 'error' not in uwb_data_payload else 'no data/error'}.")

            except KafkaError as e_kafka:
                logger.error(f"[{camera_id}] Kafka send error: {e_kafka}", exc_info=True)
                # 재시도 로직 또는 메시지 손실 처리 방안 고려 (예: 로컬 파일에 저장)
            except Exception as e_proc:
                logger.error(f"[{camera_id}] Error processing frame: {e_proc}", exc_info=True)
            finally:
                self.processing_queue.task_done() # 큐 작업 완료 알림

        # 루프 종료 후 프로듀서 정리
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
