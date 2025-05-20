# falcon-wrapper-service/app/kafka_image_reader.py
import json
import base64
import threading
import time
import logging
from queue import Queue
from datetime import datetime, timezone
import numpy as np
import cv2 
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import uuid # 고유 group_id 생성을 위해

logger = logging.getLogger(__name__)

class KafkaImageSubscriber(threading.Thread):
    def __init__(self, camera_id: str, kafka_topic: str, bootstrap_servers: list[str], 
                 processing_queue: Queue, consumer_timeout_ms: int = 1000, consumer_group_id_prefix: str = "wrapper_img_reader"):
        super().__init__(name=f"KafkaImageReader-{camera_id}")
        self.camera_id = camera_id
        self.kafka_topic = kafka_topic
        self.bootstrap_servers = bootstrap_servers
        self.processing_queue = processing_queue
        self.consumer_timeout_ms = consumer_timeout_ms
        self.consumer_group_id = f"{consumer_group_id_prefix}_{camera_id}_{uuid.uuid4().hex[:6]}" # 더 고유한 그룹 ID
        self._stop_event = threading.Event()
        self.daemon = True
        logger.info(f"[{self.camera_id}] KafkaImageSubscriber initialized for topic '{kafka_topic}' with group_id '{self.consumer_group_id}'.")

    def stop(self):
        logger.info(f"[{self.camera_id}] Stop request received for Kafka topic: {self.kafka_topic}")
        self._stop_event.set()

    def run(self):
        logger.info(f"[{self.camera_id}] Starting Kafka image consumer for topic: {self.kafka_topic} (Servers: {self.bootstrap_servers})")
        consumer = None
        while not self._stop_event.is_set():
            try:
                if consumer is None:
                    consumer = KafkaConsumer(
                        self.kafka_topic,
                        bootstrap_servers=self.bootstrap_servers,
                        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                        auto_offset_reset='latest',
                        consumer_timeout_ms=self.consumer_timeout_ms,
                        group_id=self.consumer_group_id,
                        enable_auto_commit=True, # 명시적으로 True (기본값)
                        auto_commit_interval_ms=5000 # 명시적으로 5초 (기본값)
                    )
                    logger.info(f"[{self.camera_id}] Kafka consumer (re)connected for topic: {self.kafka_topic}")

                for message in consumer: 
                    if self._stop_event.is_set(): break
                    try:
                        data = message.value
                        image_data_b64 = data.get("image_data_b64") # 또는 "img" 등 Camera Agent 스키마에 맞춰야 함
                        image_format_from_msg = data.get("image_format", data.get("format", "jpeg")).lower() # 다양한 키 호환
                        
                        frame_timestamp_str = data.get("image_timestamp_utc", data.get("ts_cam")) # 다양한 타임스탬프 키 호환
                        if frame_timestamp_str:
                            if isinstance(frame_timestamp_str, (int, float)): # Unix timestamp
                                frame_timestamp_utc = datetime.fromtimestamp(frame_timestamp_str, tz=timezone.utc)
                            else: # ISO format string
                                frame_timestamp_utc = datetime.fromisoformat(str(frame_timestamp_str).replace("Z", "+00:00"))
                        else:
                            frame_timestamp_utc = datetime.fromtimestamp(message.timestamp / 1000.0, tz=timezone.utc)

                        msg_camera_id = data.get("camera_id", self.camera_id)
                        
                        if not image_data_b64:
                            logger.warning(f"[{msg_camera_id}] Kafka message on topic {self.kafka_topic} missing image data. Skipping.")
                            continue

                        image_bytes = base64.b64decode(image_data_b64)
                        np_image_bgr = cv2.imdecode(np.frombuffer(image_bytes, np.uint8), cv2.IMREAD_COLOR)
                        
                        if np_image_bgr is None:
                            logger.error(f"[{msg_camera_id}] Failed to decode image from Kafka (format: {image_format_from_msg}). Skipping.")
                            continue

                        if self.processing_queue.full():
                            logger.warning(f"[{msg_camera_id}] Processing queue is full. Frame from Kafka topic {self.kafka_topic} might be dropped.")
                            try: self.processing_queue.get_nowait()
                            except: pass
                        
                        self.processing_queue.put((
                            msg_camera_id, 
                            np_image_bgr, 
                            frame_timestamp_utc,
                            "KAFKA_IMAGE_TOPIC",
                            {"topic": self.kafka_topic, "original_format": image_format_from_msg, "partition": message.partition, "offset": message.offset}
                        ))
                        logger.debug(f"[{msg_camera_id}] Image from Kafka topic {self.kafka_topic} (format: {image_format_from_msg}) put to queue.")
                    except json.JSONDecodeError:
                        logger.error(f"[{self.camera_id}] Failed to decode JSON from Kafka topic {self.kafka_topic}. Value (partial): {message.value[:200] if message and message.value else 'N/A'}")
                    except Exception as e_msg:
                        logger.error(f"[{self.camera_id}] Error processing message from Kafka topic {self.kafka_topic}: {e_msg}", exc_info=True)
                
                if self._stop_event.is_set(): break
            except KafkaError as e_consumer: 
                logger.error(f"[{self.camera_id}] Kafka consumer error for topic {self.kafka_topic}: {e_consumer}. Will attempt to reconnect.")
                if consumer: 
                    try: consumer.close(autocommit=False) 
                    except: pass
                consumer = None 
            except Exception as e_outer:
                 logger.error(f"[{self.camera_id}] Unexpected error in KafkaImageSubscriber for topic {self.kafka_topic}: {e_outer}", exc_info=True)
            
            if not self._stop_event.is_set() and consumer is None:
                logger.info(f"[{self.camera_id}] Waiting {5}s before retrying Kafka connection for {self.kafka_topic}...")
                self._stop_event.wait(5)

        if consumer:
            consumer.close(autocommit=False)
        logger.info(f"[{self.camera_id}] Kafka image consumer stopped for topic: {self.kafka_topic}")