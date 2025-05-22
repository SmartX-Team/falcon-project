# falcon-wrapper-service/app/config.py
import os
import json
import logging
import uuid

logger = logging.getLogger(__name__)

class AppConfig:
    # Kafka 설정
    KAFKA_BOOTSTRAP_SERVERS: list[str] = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', '10.79.1.1:9094').split(',')
    OUTPUT_KAFKA_TOPIC: str = os.environ.get('OUTPUT_KAFKA_TOPIC', 'fused_input_for_inference')
    KAFKA_PRODUCER_MAX_REQUEST_SIZE: int = int(os.environ.get('KAFKA_PRODUCER_MAX_REQUEST_SIZE', 5 * 1024 * 1024))
    KAFKA_CONSUMER_TIMEOUT_MS: int = int(os.environ.get('KAFKA_CONSUMER_TIMEOUT_MS', 1000))

    # 입력 소스 설정
    RTSP_SOURCES_STR: str = os.environ.get('RTSP_SOURCES', '')
    KAFKA_IMAGE_SOURCES_STR: str = os.environ.get('KAFKA_IMAGE_SOURCES_STR', '')

    # UWB 설정
    UWB_GATEWAY_URL: str = os.environ.get("UWB_GATEWAY_URL", "http://10.79.1.12")
    UWB_API_KEY: str | None = os.environ.get("UWB_API_KEY")
    # UWB_CONFIGS_JSON은 camera_id를 key로, 해당 카메라에 매핑될 UWB tag_id를 value로 가짐
    # 예: {"cam_rtsp_01": "tag_for_cam01", "cam_kafka_01": "tag_for_cam02"}
    UWB_TAG_ID_MAPPING_JSON: str = os.environ.get('UWB_TAG_ID_MAPPING_JSON', '{}')
    UWB_API_TIMEOUT_SEC: float = float(os.environ.get("UWB_API_TIMEOUT_SEC", 0.2)) # 200ms (원본 Falcon 참고)

    # 이미지 처리 설정
    IMAGE_OUTPUT_FORMAT: str = os.environ.get('IMAGE_OUTPUT_FORMAT', 'jpeg').lower()
    JPEG_QUALITY: int = int(os.environ.get('JPEG_QUALITY', 80))

    # 처리 큐 및 스레드 설정
    PROCESSING_QUEUE_MAX_SIZE: int = int(os.environ.get('PROCESSING_QUEUE_MAX_SIZE', 100))
    FRAME_PROCESSOR_WORKERS: int = int(os.environ.get('FRAME_PROCESSOR_WORKERS', 2))

    WRAPPER_INSTANCE_ID: str = os.environ.get('WRAPPER_INSTANCE_ID', f"wrapper-{uuid.uuid4().hex[:6]}")

    # Redis Configuration for Dynamic Sources
    REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
    REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", None)
    REDIS_DB_CONFIGS = int(os.environ.get("REDIS_DB_CONFIGS", 0)) # 서비스 설정을 읽어올 Redis DB 번호
    REDIS_SERVICE_CONFIG_KEY_PATTERN = os.environ.get("REDIS_SERVICE_CONFIG_KEY_PATTERN", "service_configs:*")
    REDIS_POLLING_INTERVAL_SEC = int(os.environ.get("REDIS_POLLING_INTERVAL_SEC", 60)) # 60초마다 폴링

    DEFAULT_UWB_HANDLER_TYPE = os.environ.get("DEFAULT_UWB_HANDLER_TYPE", "postgresql").lower() # 'postgresql' or 'api'

    # UWB PostgreSQL Database Configuration
    POSTGRES_HOST_UWB = os.environ.get("POSTGRES_HOST_UWB", "localhost")
    POSTGRES_PORT_UWB = int(os.environ.get("POSTGRES_PORT_UWB", 5432))
    POSTGRES_DB_UWB = os.environ.get("POSTGRES_DB_UWB", "uwb_database_name")
    POSTGRES_USER_UWB = os.environ.get("POSTGRES_USER_UWB", "uwb_user")
    POSTGRES_PASSWORD_UWB = os.environ.get("POSTGRES_PASSWORD_UWB", "uwb_password")
    UWB_TABLE_NAME = os.environ.get("UWB_TABLE_NAME", "uwb_raw_data") # 실제 UWB 데이터 테이블명
    UWB_DB_MAX_CONNECTIONS = int(os.environ.get("UWB_DB_MAX_CONNECTIONS", 3)) # 풀 최대 연결 수

    def __init__(self):
        self.rtsp_sources: dict[str, str] = self._parse_key_value_pairs(self.RTSP_SOURCES_STR)
        self.kafka_image_sources: dict[str, str] = self._parse_key_value_pairs(self.KAFKA_IMAGE_SOURCES_STR)
        try:
            self.uwb_tag_id_map: dict[str, str] = json.loads(self.UWB_TAG_ID_MAPPING_JSON)
        except json.JSONDecodeError:
            logger.error(f"Failed to parse UWB_TAG_ID_MAPPING_JSON: {self.UWB_TAG_ID_MAPPING_JSON}. UWB data might not be fetched.")
            self.uwb_tag_id_map = {}
        
        if not self.UWB_API_KEY:
            logger.warning("UWB_API_KEY environment variable is not set. UWB API calls may fail if an API key is required.")

        logger.info("--- Wrapper Service Configuration Initialized ---")
        logger.info(f"Instance ID: {self.WRAPPER_INSTANCE_ID}")
        logger.info(f"Kafka Bootstrap Servers: {self.KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"Output Kafka Topic: {self.OUTPUT_KAFKA_TOPIC}")
        logger.info(f"RTSP Sources: {self.rtsp_sources}")
        logger.info(f"Kafka Image Sources: {self.kafka_image_sources}")
        logger.info(f"UWB Gateway URL: {self.UWB_GATEWAY_URL}")
        logger.info(f"UWB API Key set: {'Yes' if self.UWB_API_KEY else 'No'}")
        logger.info(f"UWB Tag ID Map: {self.uwb_tag_id_map}")
        logger.info(f"Image Output Format: {self.IMAGE_OUTPUT_FORMAT}, JPEG Quality: {self.JPEG_QUALITY}")
        logger.info("---------------------------------------------")

    def _parse_key_value_pairs(self, sources_str: str) -> dict[str, str]:
        sources = {}
        if sources_str:
            try:
                for item in sources_str.split(','):
                    item = item.strip()
                    if '=' in item:
                        key, value = item.split('=', 1)
                        sources[key.strip()] = value.strip()
                    elif item: 
                        logger.warning(f"Source item '{item}' in '{sources_str}' has no '='. Skipping.")
            except Exception as e:
                logger.error(f"Error parsing sources string '{sources_str}': {e}")
        return sources

app_config = AppConfig()