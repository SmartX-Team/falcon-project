# falcon-wrapper-service/app/config.py
import os
import json
import logging
import uuid
from typing import Optional, List, Dict, Union

logger = logging.getLogger(__name__)


class AppConfig:
    # 로깅 레벨
    LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "INFO").upper()

    # Kafka 설정
    KAFKA_BOOTSTRAP_SERVERS: List[str] = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', '10.79.1.1:9094').split(',')
    OUTPUT_KAFKA_TOPIC: str = os.environ.get('OUTPUT_KAFKA_TOPIC', 'fused_input_for_inference') # docker run에서는 falcon_output 사용
    KAFKA_PRODUCER_MAX_REQUEST_SIZE: int = int(os.environ.get('KAFKA_PRODUCER_MAX_REQUEST_SIZE', 5 * 1024 * 1024))
    KAFKA_CONSUMER_TIMEOUT_MS: int = int(os.environ.get('KAFKA_CONSUMER_TIMEOUT_MS', 1000))
    KAFKA_PRODUCER_RETRIES: int = int(os.environ.get("KAFKA_PRODUCER_RETRIES", "5"))


    _kafka_producer_acks_str = os.environ.get("KAFKA_PRODUCER_ACKS", "1")
    if _kafka_producer_acks_str.lower() == 'all':
        KAFKA_PRODUCER_ACKS: Union[str, int] = 'all'
    else:
        try:
            KAFKA_PRODUCER_ACKS: Union[str, int] = int(_kafka_producer_acks_str)
        except ValueError:
            logger.warning(f"Invalid KAFKA_PRODUCER_ACKS value '{_kafka_producer_acks_str}'. Defaulting to 1.")
            KAFKA_PRODUCER_ACKS: Union[str, int] = 1

    KAFKA_PRODUCER_FLUSH_TIMEOUT_SEC: int = int(os.environ.get("KAFKA_PRODUCER_FLUSH_TIMEOUT_SEC", "10"))
    KAFKA_PRODUCER_CLOSE_TIMEOUT_SEC: int = int(os.environ.get("KAFKA_PRODUCER_CLOSE_TIMEOUT_SEC", "10"))


    # 정적 입력 소스 설정 (환경 변수를 통해 문자열로 받고, __init__에서 파싱)
    # 이 부분은 Redis를 통한 동적 설정과 함께 사용될 경우, 우선순위나 로직을 main.py에서 명확히 해야 합니다.
    RTSP_SOURCES_STR: str = os.environ.get('RTSP_SOURCES', '')
    KAFKA_IMAGE_SOURCES_STR: str = os.environ.get('KAFKA_IMAGE_SOURCES_STR', '')

    # UWB 설정 (API 핸들러용)
    UWB_GATEWAY_URL: Optional[str] = os.environ.get("UWB_GATEWAY_URL", "http://10.79.1.12") # docker run에서는 10.63 사용
    UWB_API_KEY: Optional[str] = os.environ.get("UWB_API_KEY") # docker run에서는 your-uwb-api-key 사용
    UWB_API_TIMEOUT_SEC: float = float(os.environ.get("UWB_API_TIMEOUT_SEC", 0.2)) # docker run에서는 2초 사용

    # UWB 태그 ID 정적 매핑 (JSON 문자열)
    # 예: {"cam_rtsp_01": "tag_for_cam01", "cam_kafka_01": "tag_for_cam02"}
    # 이 또한 Redis 동적 설정과 어떻게 연동될지 고려 필요.
    UWB_TAG_ID_MAPPING_JSON: str = os.environ.get('UWB_TAG_ID_MAPPING_JSON', '{}')

    # 이미지 처리 설정
    IMAGE_OUTPUT_FORMAT: str = os.environ.get('IMAGE_OUTPUT_FORMAT', 'jpeg').lower()
    JPEG_QUALITY: int = int(os.environ.get('JPEG_QUALITY', 80)) # docker run에서는 100 사용

    # 처리 큐 및 스레드 설정
    PROCESSING_QUEUE_MAX_SIZE: int = int(os.environ.get('PROCESSING_QUEUE_MAX_SIZE', 100)) # docker run에서는 200 사용
    FRAME_PROCESSOR_WORKERS: int = int(os.environ.get('FRAME_PROCESSOR_WORKERS', 2))

    WRAPPER_INSTANCE_ID: str = os.environ.get('WRAPPER_INSTANCE_ID', f"wrapper-{uuid.uuid4().hex[:6]}")

    # Redis Configuration for Dynamic Sources
    REDIS_HOST: str = os.environ.get("REDIS_HOST", "localhost") # docker run에서는 10.79.1.14 사용
    REDIS_PORT: int = int(os.environ.get("REDIS_PORT", 6379))
    REDIS_PASSWORD: Optional[str] = os.environ.get("REDIS_PASSWORD") # docker run에서는 명시 안됨 (None으로 처리)
    REDIS_DB_CONFIGS: int = int(os.environ.get("REDIS_DB_CONFIGS", 0)) # 서비스 설정을 읽어올 Redis DB 번호
    REDIS_SERVICE_CONFIG_KEY_PREFIX: str = os.environ.get('REDIS_SERVICE_CONFIG_KEY_PREFIX', 'service_configs') # docker run에서는 service_configs:* (main.py에서 prefix만 사용)
    TARGET_SERVICE_NAME: str = os.environ.get('TARGET_SERVICE_NAME', 'falcon_service') # 이 wrapper가 구독할 서비스 이름
    REDIS_POLLING_INTERVAL_SEC: int = int(os.environ.get("REDIS_POLLING_INTERVAL_SEC", 60)) # docker run에서는 30초 사용

    # 기본 UWB 핸들러 타입
    DEFAULT_UWB_HANDLER_TYPE: str = os.environ.get("DEFAULT_UWB_HANDLER_TYPE", "postgresql").lower() # 'postgresql' or 'api'

    # UWB PostgreSQL Database Configuration
    POSTGRES_HOST_UWB: Optional[str] = os.environ.get("POSTGRES_HOST_UWB", "localhost") # docker run에서는 10.79.1.13 사용
    POSTGRES_PORT_UWB: Optional[int] = int(os.environ.get("POSTGRES_PORT_UWB", 5432)) if os.environ.get("POSTGRES_PORT_UWB") else None
    POSTGRES_DB_UWB: Optional[str] = os.environ.get("POSTGRES_DB_UWB", "uwb_database_name") # docker run에서는 uwb 사용
    POSTGRES_USER_UWB: Optional[str] = os.environ.get("POSTGRES_USER_UWB", "uwb_user") # docker run에서는 myuser 사용
    POSTGRES_PASSWORD_UWB: Optional[str] = os.environ.get("POSTGRES_PASSWORD_UWB", "uwb_password") # docker run에서는 netAi007! 사용
    UWB_TABLE_NAME: str = os.environ.get("UWB_TABLE_NAME", "uwb_raw_data") # docker run에서는 uwb_raw 사용
    UWB_DB_MAX_CONNECTIONS: int = int(os.environ.get("UWB_DB_MAX_CONNECTIONS", 3)) # docker run에서는 5 사용

    def __init__(self):
        # 정적 소스 설정 파싱
        self.rtsp_sources: Dict[str, str] = self._parse_key_value_pairs(self.RTSP_SOURCES_STR)
        self.kafka_image_sources: Dict[str, str] = self._parse_key_value_pairs(self.KAFKA_IMAGE_SOURCES_STR)
        
        # UWB 태그 ID 정적 매핑 파싱
        try:
            self.uwb_tag_id_map: Dict[str, str] = json.loads(self.UWB_TAG_ID_MAPPING_JSON)
        except json.JSONDecodeError:
            logger.error(f"Failed to parse UWB_TAG_ID_MAPPING_JSON: '{self.UWB_TAG_ID_MAPPING_JSON}'. UWB tag mapping will be empty.")
            self.uwb_tag_id_map = {}
        
        # 환경 변수 로드 후 로깅 (옵션)
        logger.info("--- Wrapper Service Configuration Initialized ---")
        logger.info(f"Instance ID: {self.WRAPPER_INSTANCE_ID}")
        logger.info(f"Log Level: {self.LOG_LEVEL}")
        logger.info(f"Target Service Name (for Redis): {self.TARGET_SERVICE_NAME}")
        logger.info(f"Redis Host: {self.REDIS_HOST}:{self.REDIS_PORT}, DB: {self.REDIS_DB_CONFIGS}")
        logger.info(f"Default UWB Handler: {self.DEFAULT_UWB_HANDLER_TYPE}")
        if self.DEFAULT_UWB_HANDLER_TYPE == 'postgresql':
            logger.info(f"PostgreSQL (UWB): {self.POSTGRES_HOST_UWB}:{self.POSTGRES_PORT_UWB}, DB: {self.POSTGRES_DB_UWB}")
        elif self.DEFAULT_UWB_HANDLER_TYPE == 'api':
            logger.info(f"UWB API Gateway: {self.UWB_GATEWAY_URL}, API Key Set: {'Yes' if self.UWB_API_KEY else 'No'}")
        # logger.info(f"Static RTSP Sources: {self.rtsp_sources}") # 필요시 로깅
        # logger.info(f"Static Kafka Image Sources: {self.kafka_image_sources}") # 필요시 로깅
        # logger.info(f"Static UWB Tag ID Map: {self.uwb_tag_id_map}") # 필요시 로깅
        logger.info("---------------------------------------------")

    def _parse_key_value_pairs(self, sources_str: str) -> Dict[str, str]:
        sources = {}
        if sources_str:
            try:
                for item in sources_str.split(','):
                    item = item.strip()
                    if '=' in item:
                        key, value = item.split('=', 1)
                        sources[key.strip()] = value.strip()
                    elif item: 
                        logger.warning(f"Source item '{item}' in sources string has no '='. Skipping.")
            except Exception as e:
                logger.error(f"Error parsing sources string '{sources_str}': {e}")
        return sources

# 애플리케이션 전체에서 사용할 설정 객체 인스턴스
app_config = AppConfig()