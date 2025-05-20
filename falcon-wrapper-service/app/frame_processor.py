# falcon-wrapper-service/app/frame_processor.py
import threading
import time
import json
import base64
import uuid
import logging
from queue import Queue, Empty
from datetime import datetime, timezone
import cv2 
from kafka import KafkaProducer
from kafka.errors import KafkaError

from .uwb_handler import UWBHandler # 타입 힌트용
# from .config import AppConfig # main.py에서 AppConfig 인스턴스를 직접 주입받음

logger = logging.getLogger(__name__)

class FrameProcessor(threading.Thread):
    def __init__(self,
                 processing_queue: Queue,
                 app_config, # AppConfig 인스턴스
                 uwb_handler_map: dict[str, UWBHandler]):
        super().__init__(name=f"FrameProcessor-{str(uuid.uuid4())[:4]}")
        self.processing_queue = processing_queue
        self.config = app_config
        self.output_topic = self.config.OUTPUT_KAFKA_TOPIC
        self.bootstrap_servers = self.config.KAFKA_BOOTSTRAP_SERVERS
        self.uwb_handler_map = uwb_handler_map
        
        self._stop_event = threading.Event()
        self.daemon = True
        self.producer = None
        self._initialize_producer()
        logger.info(f"FrameProcessor {self.name} initialized. Outputting to topic '{self.output_topic}'.")

    def _initialize_producer(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                max_request_size=self.config.KAFKA_PRODUCER_MAX_REQUEST_SIZE,
                retries=3,
                acks='1' 
            )
            logger.info(f"FrameProcessor {self.name}: Kafka Producer connected to: {self.bootstrap_servers}")
        except KafkaError as e:
            logger.fatal(f"FrameProcessor {self.name}: Failed to initialize Kafka Producer: {e}. This worker will not function.")
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
                item = self.processing_queue.get(timeout=1.0) 
                if item is None: 
                    logger.info(f"FrameProcessor {self.name} received None (shutdown signal) from queue.")
                    break 
            except Empty:
                continue 
            except Exception as e_q:
                logger.error(f"FrameProcessor {self.name}: Error getting item from queue: {e_q}", exc_info=True)
                continue

            camera_id, image_bgr, frame_timestamp_utc, source_type, source_details = item

            try:
                uwb_handler = self.uwb_handler_map.get(camera_id)
                # uwb_data_payload는 x_m, y_m, (선택적 z_m), timestamp_uwb_utc, quality 등을 포함한 dict 또는 None
                uwb_data_payload = None 
                if uwb_handler:
                    retrieved_uwb = uwb_handler.get_uwb_data() # frame_timestamp_utc 인자 제거
                    if retrieved_uwb:
                        uwb_data_payload = retrieved_uwb
                    else:
                        logger.debug(f"[{camera_id}] No UWB data retrieved (returned None) by FrameProcessor {self.name}.")
                        # UWB 데이터가 없어도 이미지 데이터는 전송할 수 있도록 빈 객체 또는 null로 설정
                        uwb_data_payload = {"error": "UWB data not available"} 
                else:
                    logger.debug(f"[{camera_id}] No UWBHandler configured for this camera_id in FrameProcessor {self.name}.")
                    uwb_data_payload = {"error": "UWB handler not configured"}
                
                if self.config.IMAGE_OUTPUT_FORMAT == 'jpeg':
                    encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), self.config.JPEG_QUALITY]
                    result, encoded_image_bytes = cv2.imencode('.jpg', image_bgr, encode_param)
                    image_format_out = "jpeg"
                elif self.config.IMAGE_OUTPUT_FORMAT == 'png':
                    result, encoded_image_bytes = cv2.imencode('.png', image_bgr)
                    image_format_out = "png"
                else: 
                    logger.warning(f"[{camera_id}] Unsupported output format '{self.config.IMAGE_OUTPUT_FORMAT}', defaulting to JPEG.")
                    encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), self.config.JPEG_QUALITY]
                    result, encoded_image_bytes = cv2.imencode('.jpg', image_bgr, encode_param)
                    image_format_out = "jpeg"
                
                if not result:
                    logger.error(f"[{camera_id}] Failed to encode image to {image_format_out} in FrameProcessor {self.name}.")
                    self.processing_queue.task_done()
                    continue
                
                image_data_b64 = base64.b64encode(encoded_image_bytes.tobytes()).decode('utf-8')

                output_payload = {
                    "message_id": str(uuid.uuid4()),
                    "wrapper_instance_id": self.config.WRAPPER_INSTANCE_ID,
                    "camera_id": camera_id,
                    "image_timestamp_utc": frame_timestamp_utc.isoformat(),
                    "image_format": image_format_out,
                    # "image_resolution": f"{image_bgr.shape[1]}x{image_bgr.shape[0]}", # 원본 해상도 정보 추가 가능
                    "image_data_b64": image_data_b64,
                    "uwb_data": uwb_data_payload, # UWBHandler가 반환한 dict (x_m, y_m, timestamp_uwb_utc 등 포함)
                    "source_type": source_type,
                    "source_details": source_details,
                    "processing_timestamp_utc": datetime.now(timezone.utc).isoformat()
                }

                self.producer.send(self.output_topic, value=output_payload)
                logger.info(f"[{camera_id}] FrameProcessor {self.name}: Fused data (Img: {image_format_out}, UWB: {uwb_data_payload.get('x_m') is not None}) sent to Kafka topic '{self.output_topic}'.")

            except KafkaError as e_kafka:
                logger.error(f"[{camera_id}] FrameProcessor {self.name}: Kafka send error: {e_kafka}")
            except Exception as e_proc:
                logger.error(f"[{camera_id}] FrameProcessor {self.name}: Error processing frame: {e_proc}", exc_info=True)
            finally:
                self.processing_queue.task_done()

        if self.producer:
            logger.info(f"FrameProcessor {self.name}: Flushing remaining messages and closing producer...")
            try:
                self.producer.flush(timeout=5.0)
            except Exception as e_flush:
                logger.error(f"FrameProcessor {self.name}: Error flushing Kafka producer: {e_flush}")
            finally:
                try:
                    self.producer.close(timeout=5.0)
                except Exception as e_close:
                    logger.error(f"FrameProcessor {self.name}: Error closing Kafka producer: {e_close}")
        logger.info(f"FrameProcessor {self.name} thread stopped.")