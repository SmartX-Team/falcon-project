# Falcon Wrapper 서비스 (입력 처리 및 센서 융합)

## 1. 서비스 개요

`ai-service-visibility`는 다양한 실시간 비디오/이미지 소스(RTSP 스트림, Kafka 이미지 토픽 등)로부터 데이터를 수신하고, 이와 관련된 UWB(초광대역) 위치 데이터를 각 프레임의 타임스탬프 기준으로 융합합니다. 처리된 데이터는 표준화된 JSON 형식으로 중앙 Kafka 토픽에 발행되어, 후속 AI 추론 서비스(`falcon-inference`) 및 디지털 트윈 시각화 애플리케이션에서 활용될 수 있도록 설계되었다.

이 서비스의 핵심 특징은 **Redis를 통한 동적 소스 관리**입니다. AI 대학원내 Camera Agent 특성상 동적으로 카메라 on/off 및 상태 변화가 많기 때문에, 사용자가 동적으로 Redis에 사용가능/사용할 카메라 리소스들을 저장 및 수정하며, 'wrapper-service'는 Redis 기반으로로 현재 활성화된 카메라 소스 및 관련 설정을 읽어온다. 이를 통해 서비스 재시작 없이 실시간으로 입력 소스를 추가, 제거 또는 변경할 수 있는 유연성을 제공합니다.

각 활성 소스에 대한 데이터 수신 및 UWB 데이터 융합 처리는 독립적인 스레드에서 수행되어 병렬성과 확장성을 확보한다.

쿠버네티스 배포 관련 YAML 파일 및 컨테이너 이미지는 다음 저장소에서 통합 관리됩니다:
[https://github.com/SmartX-Team/twin-datapond.git](https://github.com/SmartX-Team/twin-datapond.git)

## 2. 주요 기능

* **동적 다중 입력 소스 지원**:
    * Redis에 등록된 설정을 기반으로 RTSP 비디오 스트림 및 Kafka 이미지 토픽을 동적으로 구독하고 처리합니다.
    * `ai-service-visibility` 를 통해 Redis에 카메라 소스 정보 (스트림 URL, Kafka 토픽, UWB 태그 ID 등)를 등록/업데이트하면, `falcon-wrapper-service`가 이를 감지하여 해당 소스에 대한 데이터 처리를 시작하거나 중단한다.
    따라서 카메라 소스가 동적으로 자주 변경되는 AI 대학원 현 상황에 맞춰 대응가능하도록 설계하였다.
* **UWB 데이터 융합**:
    * 각 카메라/이미지 프레임의 타임스탬프를 기준으로, 해당 시점의 UWB 위치 데이터를 동기화한다.
    * UWB 데이터는 두 가지 방식 중 하나로 가져올 수 있으며, Redis의 개별 카메라 설정에 따라 결정된다:
        1.  **PostgreSQL DB 조회**: 지정된 UWB 태그 ID와 프레임 타임스탬프를 사용하여 PostgreSQL DB에서 가장 근접한 (해당 시점 또는 그 이전의 가장 최신) 위치 데이터를 조회한다.
        2.  **REST API 호출**: 직접 UWB 서버와 웹소켓 기반 통신으로 UWB 게이트웨이의 REST API를 호출하여 특정 태그 ID의 최신 위치 정보를 가져온다. (주기적 호출, Deprecated 예정)
* **통합 메시지 발행**:
    * 처리된 이미지 데이터 (Base64 인코딩), UWB 데이터, 카메라 식별자, 이미지 타임스탬프, UWB 타임스탬프 및 기타 메타데이터를 포함하는 표준화된 JSON 메시지를 지정된 Kafka 토픽으로 발행한다.
* **설정 유연성 (기본값 및 Redis 오버라이드)**:
    * Kafka 서버 주소, 기본 UWB 핸들러 타입, 이미지 처리 옵션 등 전역적인 기본 설정은 환경 변수를 통해 지정한다.
    * 개별 카메라 소스의 스트림 정보, 사용할 UWB 태그 ID, UWB 핸들러 타입 등은 Redis에서 동적으로 로드되어 전역 설정을 오버라이드할 수 있다.
* **병렬 처리**:
    * Redis에서 로드된 각 활성 입력 소스는 별도의 Reader 스레드(RTSP 또는 Kafka)에서 처리된다.
    * 프레임 처리 및 Kafka 발행 작업은 별도의 워커 스레드 풀(`FrameProcessor`)에서 수행되어 처리량을 높입니다.

## 3. 프로젝트 구조 (app/ 디렉토리)

* **`config.py`**:
    * 역할: 애플리케이션 실행에 필요한 모든 전역 설정을 환경 변수로부터 로드하고 관리한다.
    * 주요 내용: Kafka 브로커 주소, 출력 토픽 이름, Redis 접속 정보, UWB 핸들러 (API, PostgreSQL) 기본 설정, 이미지 압축 품질, 처리 큐 크기, 워커 스레드 수 등.
* **`main.py`**:
    * 역할: 서비스의 메인 진입점으로, `app_config`를 로드하고, Redis 클라이언트, PostgreSQL UWB 연결 풀 (필요시)을 초기화한다.
    * 주기적으로 Redis를 폴링하여 서비스 설정을 로드하고, 변경 사항에 따라 Reader 스레드 및 UWB 핸들러 인스턴스를 동적으로 생성, 시작, 중지하는 `SourceManagerThread`를 실행합니다.
    * `FrameProcessor` 스레드들을 초기화하고 실행하며, 시그널 처리를 통한 정상 종료 로직을 관리합니다.
* **`uwb_handler.py` (API 기반 - Deprecated 예정)**:
    * 역할: (구 버전) UWB 게이트웨이의 REST API를 호출하여 특정 태그 ID의 최신 UWB 데이터를 수신한다.
    * 주요 내용: `APIUWBHandler` 클래스. `get_uwb_data(frame_timestamp)` 메소드는 API를 호출하여 데이터를 가져옵니다. (frame\_timestamp 인자는 인터페이스 일관성을 위해 존재하나, API 호출 시점의 최신 데이터를 반환).
* **`uwb_pg_handler.py` (PostgreSQL 기반 - 권장)**:
    * 역할: PostgreSQL DB에 접속하여 특정 UWB 태그 ID와 주어진 이미지 프레임 타임스탬프에 가장 근접한 (해당 시점 또는 그 이전의 가장 최신) UWB 위치 데이터를 조회합니다.
    * 주요 내용: `UWBPostgresHandler` 클래스, DB 연결 풀 관리 함수 (`init_uwb_pg_pool`, `close_uwb_pg_pool` 등). `get_uwb_data(frame_timestamp)` 메소드는 DB에서 데이터를 조회합니다.
* **`rtsp_reader.py`**:
    * 역할: Redis 설정을 통해 동적으로 할당된 RTSP URL로부터 비디오 스트림을 구독하고 프레임을 추출하여 내부 처리 큐에 넣습니다.
    * 주요 내용: `RTSPStreamSubscriber` 스레드 클래스. OpenCV (`cv2.VideoCapture`)를 사용합니다.
* **`kafka_image_reader.py`**:
    * 역할: Redis 설정을 통해 동적으로 할당된 Kafka 토픽으로부터 이미지 메시지(JSON 형식, Base64 이미지 데이터 포함)를 구독하고, 디코딩하여 내부 처리 큐에 넣습니다.
    * 주요 내용: `KafkaImageSubscriber` 스레드 클래스. `kafka-python` 라이브러리를 사용합니다.
* **`frame_processor.py`**:
    * 역할: 내부 처리 큐에서 (이미지, 타임스탬프, 소스 정보) 데이터를 가져옵니다. 해당 소스에 매핑된 UWB 핸들러를 사용하여 UWB 데이터를 가져와 융합합니다. 이미지를 표준 포맷으로 압축/인코딩한 후, 최종 JSON 메시지를 구성하여 Kafka로 발행합니다.
    * 주요 내용: `FrameProcessor` 스레드 클래스. 여러 워커 스레드로 실행됩니다.

## 4. 실행 환경 및 주요 의존성

* Python 3.9+
* 주요 Python 라이브러리: 전체 목록은 `requirements.txt` 참조
* Kafka 클러스터
* Redis 서버 (서비스 설정 저장용)
* PostgreSQL 서버 (PostgreSQL UWB 핸들러 사용 시, UWB 데이터 저장용)
* (선택적) UWB 게이트웨이 API (API UWB 핸들러 사용 시)

해당 서비스는 Docker 컨테이너 기반으로, 다른 서비스들과 함께 K8S Cluster 내에서 오케스트레이션될 예정입니다.

## 5. Redis를 통한 동적 소스 설정

* **Redis 키 패턴**: 환경 변수 `REDIS_SERVICE_CONFIG_KEY_PATTERN`으로 설정 (기본값: `service_configs:*`).
* **Redis 값 (JSON 문자열 예시)**:
    ```json
    {
        "service_name": "cam_lobby_rtsp", // Wrapper 서비스 내에서 camera_id로 사용됨
        "description": "Lobby RTSP camera with UWB tag 101",
        "input_camera_id": "actual_visibility_cam_id_lobby", // 참고용 필드
        "input_uwb_tag_id": "101", // 이 카메라에 매핑될 UWB 태그 ID
        "uwb_handler_type": "postgresql", // "postgresql" 또는 "api". 없으면 DEFAULT_UWB_HANDLER_TYPE 사용.
        "visibility_camera_info": {
            "camera_name": "Lobby Cam",
            "stream_protocol": "RTSP", // "RTSP" 또는 "KAFKA"
            "stream_details": {
                // RTSP 경우:
                "rtsp_url": "rtsp://user:pass@192.168.0.10/stream1"
                // KAFKA 경우:
                // "kafka_topic": "raw_images_lobby_cam",
                // "kafka_bootstrap_servers": "10.79.1.1:9094" // 개별 설정, 없으면 전역 KAFKA_BOOTSTRAP_SERVERS 사용
            }
        }
    }
    ```
    `falcon-wrapper-service`는 위 정보를 파싱하여 `camera_id` (여기서는 "cam\_lobby\_rtsp"), 스트림 타입, 스트림 URI/토픽, UWB 태그 ID, UWB 핸들러 타입 등을 추출하여 해당 소스에 대한 처리를 동적으로 시작/중지/업데이트합니다.

## 6. 주요 환경 변수 설정

다음은 `falcon-wrapper-service` 실행 시 설정해야하는 주요 환경 변수입니다.

#### 일반 서비스 설정
* `LOG_LEVEL`: 로깅 레벨 (예: "DEBUG", "INFO", "WARNING", "ERROR"). 기본값: "INFO".
* `WRAPPER_INSTANCE_ID`: 이 Wrapper 서비스 인스턴스의 고유 ID.
* `PROCESSING_QUEUE_MAX_SIZE`: 내부 처리 큐의 최대 크기.
* `FRAME_PROCESSOR_WORKERS`: 프레임 처리 워커 스레드 수.

#### Kafka 기본 설정 (출력 및 Kafka 이미지 리더용)
* `KAFKA_BOOTSTRAP_SERVERS`: **(필수)** Kafka 브로커 주소. (예: `10.79.1.1:9094`)
* `OUTPUT_KAFKA_TOPIC`: **(필수)** 처리된 데이터가 발행될 Kafka 토픽 이름.
* `KAFKA_PRODUCER_MAX_REQUEST_SIZE`: Kafka 프로듀서 최대 요청 크기 (바이트).
* `KAFKA_PRODUCER_RETRIES`: Kafka 프로듀서 재시도 횟수.
* `KAFKA_PRODUCER_ACKS`: Kafka 프로듀서 `acks` 설정.
* `KAFKA_CONSUMER_TIMEOUT_MS`: Kafka 이미지 리더의 컨슈머 타임아웃 (밀리초).
* `KAFKA_PRODUCER_FLUSH_TIMEOUT_SEC`: Kafka 프로듀서 `flush` 타임아웃 (초).
* `KAFKA_PRODUCER_CLOSE_TIMEOUT_SEC`: Kafka 프로듀서 `close` 타임아웃 (초).

#### Redis 설정 (동적 소스 구성 로드용)
* `REDIS_HOST`: **(필수)** Redis 서버 호스트.
* `REDIS_PORT`: **(필수)** Redis 서버 포트.
* `REDIS_PASSWORD`: Redis 비밀번호 (선택 사항).
* `REDIS_DB_CONFIGS`: 서비스 설정을 읽어올 Redis DB 번호.
* `REDIS_SERVICE_CONFIG_KEY_PATTERN`: Redis에서 스캔할 키 패턴 (기본값: `service_configs:*`).
* `REDIS_POLLING_INTERVAL_SEC`: Redis 설정 폴링 간격(초) (기본값: 60).
* `DEFAULT_UWB_HANDLER_TYPE`: Redis의 개별 소스 설정에 `uwb_handler_type`이 명시되지 않았을 경우 사용할 기본 UWB 핸들러 타입 ("postgresql" 또는 "api").

#### UWB 핸들러: API 방식 (전역 기본값)
* `UWB_GATEWAY_URL`: UWB 게이트웨이 API URL (API 핸들러 사용 시).
* `UWB_API_KEY`: UWB API 키 (선택 사항).
* `UWB_API_TIMEOUT_SEC`: UWB API 호출 타임아웃(초).

#### UWB 핸들러: PostgreSQL 방식 (전역 기본값)
* `POSTGRES_HOST_UWB`: **(PostgreSQL UWB 사용 시 필수)** PostgreSQL 서버 호스트.
* `POSTGRES_PORT_UWB`: **(PostgreSQL UWB 사용 시 필수)** PostgreSQL 서버 포트.
* `POSTGRES_DB_UWB`: **(PostgreSQL UWB 사용 시 필수)** UWB 데이터베이스 이름.
* `POSTGRES_USER_UWB`: **(PostgreSQL UWB 사용 시 필수)** PostgreSQL 사용자 이름.
* `POSTGRES_PASSWORD_UWB`: **(PostgreSQL UWB 사용 시 필수)** PostgreSQL 비밀번호.
* `UWB_TABLE_NAME`: **(PostgreSQL UWB 사용 시 필수)** UWB 데이터 테이블 이름.
* `UWB_DB_MAX_CONNECTIONS`: UWB DB 연결 풀 최대 연결 수.

#### 이미지 처리 설정
* `IMAGE_OUTPUT_FORMAT`: 출력 이미지 포맷 ("jpeg" 또는 "png").
* `JPEG_QUALITY`: JPEG 압축 품질 (0-100).

## 7. Docker 실행 예시

아래는 Docker 컨테이너 실행 명령어 예시입니다. 실제 환경에 맞춰 값 수정하삼삼

```bash
docker run -d --name falcon-wrapper-dynamic \
  --network host \
  -e LOG_LEVEL="INFO" \
  -e WRAPPER_INSTANCE_ID="wrapper-main-01" \
  -e PROCESSING_QUEUE_MAX_SIZE="200" \
  -e FRAME_PROCESSOR_WORKERS="2" \
  -e KAFKA_BOOTSTRAP_SERVERS="10.79.1.1:9094" \
  -e OUTPUT_KAFKA_TOPIC="fused_vision_uwb_output" \
  -e KAFKA_PRODUCER_MAX_REQUEST_SIZE="10485760" \
  -e KAFKA_PRODUCER_RETRIES="5" \
  -e KAFKA_PRODUCER_ACKS="1" \
  -e KAFKA_CONSUMER_TIMEOUT_MS="1000" \
  -e KAFKA_PRODUCER_FLUSH_TIMEOUT_SEC="10" \
  -e KAFKA_PRODUCER_CLOSE_TIMEOUT_SEC="10" \
  -e REDIS_HOST="your-redis-host" \
  -e REDIS_PORT="6379" \
  -e REDIS_PASSWORD="your-redis-password" \
  -e REDIS_DB_CONFIGS="0" \
  -e REDIS_SERVICE_CONFIG_KEY_PATTERN="service_configs:*" \
  -e REDIS_POLLING_INTERVAL_SEC="30" \
  -e DEFAULT_UWB_HANDLER_TYPE="postgresql" \
  -e UWB_GATEWAY_URL="http://your-uwb-gateway-api-url" \
  -e UWB_API_KEY="your-uwb-api-key" \
  -e UWB_API_TIMEOUT_SEC="2" \
  -e POSTGRES_HOST_UWB="your-pg-host-for-uwb" \
  -e POSTGRES_PORT_UWB="5432" \
  -e POSTGRES_DB_UWB="uwb_database_name" \
  -e POSTGRES_USER_UWB="uwb_db_username" \
  -e POSTGRES_PASSWORD_UWB="uwb_db_password" \
  -e UWB_TABLE_NAME="uwb_raw_data_table_name" \
  -e UWB_DB_MAX_CONNECTIONS="5" \
  -e IMAGE_OUTPUT_FORMAT="jpeg" \
  -e JPEG_QUALITY="85" \
  your-repo/falcon-wrapper-service:latest