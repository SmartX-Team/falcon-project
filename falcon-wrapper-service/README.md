## Falcon Wrapper 서비스 (입력 처리 및 센서 융합)

#### 1. 해당 서비스 컨테이너 소개
falcon-wrapper-service는 다양한 소스(RTSP 스트림, Kafka 이미지 토픽 등)로부터 비디오/이미지 데이터를 수신하고, 이와 관련된 UWB(초광대역) 위치 데이터를 융합하여 표준화된 형식으로 중앙 Kafka 토픽에 발행하는 역할을 담당합니다. 이렇게 처리된 데이터는 후속 AI 추론 서비스(falcon-inference 등) 및 디지털 트윈 시각화 애플리케이션에서 활용될 수 있도록 설계되었습니다.

각 소스에 대한 처리는 독립적인 스레드로 수행되어 병렬성과 확장성을 확보하고, 쿠버네티스 환경에 맞춰 최적화 시킴
쿠버네티스에 최종 배포한 yaml 파일은 언제나 그렇듯 아래 REPO에 통합 관리중이며, 해당 REPO 에서는 해당 서비스 컨테이너 이미지만 배포
https://github.com/SmartX-Team/twin-datapond.git

#### 2. 주요 기능

다중 입력 소스 지원:

Camera Agent 가 제공해주는 두가지 방식의 RTSP / Kafka 기반 영상 스트리밍 대응 가능
RTSP 비디오 스트림 구독 및 프레임 추출.

Kafka 토픽으로 발행된 이미지 메시지 (JSON 형식, Base64 인코딩된 이미지 데이터 포함) 구독.

UWB 데이터 융합: 각 카메라/이미지 소스에서 수신된 프레임의 타임스탬프를 기준으로, 해당 시점의 UWB 위치 데이터를 동기화하고 함께 처리한다. 후에 다중 카메라 리소스 및 다중 Tag ID 와 묶어서 요청이 들어올 것을 감안해서 최종 버전에서는 PostgreSQL 과 Redis 등 서비스 활용하나 아직은 기존 원본 Falcon Project 소스처럼 REST API 를 200ms 단위로 호출하여 처리하도록 구현해둠


통합 메시지 발행: 처리된 이미지 데이터, UWB 데이터, 카메라 식별자, 타임스탬프 및 기타 메타데이터를 포함하는 표준화된 JSON 메시지를 지정된 Kafka 토픽으로 발행합니다.

설정 유연성: Kafka 서버 주소, 입력 소스 목록, 출력 토픽 이름, 이미지 처리 옵션 등을 환경 변수를 통해 설정

병렬 처리: 각 입력 소스(RTSP, Kafka 토픽)는 별도의 스레드에서 처리되며, 프레임 처리 및 Kafka 발행 작업 또한 별도의 워커 스레드 풀에서 수행되어 처리량을 높입니다.

#### 3. 프로젝트 구조 (app/ 디렉토리)

config.py:

역할: 애플리케이션 실행에 필요한 모든 설정을 환경 변수로부터 로드하고 관리

필요한 환경 변수 주요 내용: Kafka 브로커 주소, 입/출력 토픽 이름, RTSP 소스 URL 목록, Kafka 이미지 소스 토픽 목록, UWB 설정 JSON, 이미지 압축 품질, 처리 큐 크기, 워커 스레드 수 , 후에 DB 들 접속 정보 등등

uwb_handler.py:

역할: 특정 카메라 ID에 대한 UWB 데이터 수신 및 제공 인터페이스를 정의합니다.

주요 내용: UWBHandler 클래스. get_uwb_data() 메서드는 주어진 이미지 타임스탬프에 해당하는 UWB 데이터를 반환하는 로직을 포함해야 한다. 현재는 기능 구현에 중시해서 UWB 데이터에 특정 태그 ID 넣고 호출해서 현재 최신 위치정보를 200ms 단위로 갱신하도록 구현함

GET http://<uwb-gateway>/api/tags/{tag_id} 
또한 해당 Falcon 프로젝트가 Z축이 일정한 GIST AI Grad Building 에서 사용되는 것을 고려할때 사용하는 데이터는 오직 특정 태그의 x, y 만 불러옴 ;
이후 리팩토링 과정에서 새로 업데이트 예정

rtsp_reader.py:

역할: 지정된 RTSP URL로부터 비디오 스트림을 구독하고 프레임을 추출하여 내부 처리 큐에 넣는다.

주요 내용: RTSPStreamSubscriber 스레드 클래스. OpenCV (cv2.VideoCapture)를 사용하여 RTSP 스트림을 처리하고, 설정된 FPS 제한에 따라 프레임을 샘플링한다.

kafka_image_reader.py:

역할: 지정된 Kafka 토픽으로부터 이미지 메시지(JSON 형식, Base64 이미지 데이터 포함)를 구독하고, 디코딩하여 내부 처리 큐에 넣습니다.

주요 내용: KafkaImageSubscriber 스레드 클래스. kafka-python 라이브러리를 사용하여 메시지를 소비하고, 이미지 데이터를 추출한다.

frame_processor.py:

역할: 내부 처리 큐에서 (이미지, 타임스탬프, 소스 정보) 데이터를 가져와 UWB 데이터를 융합하고, 이미지를 표준 포맷으로 압축/인코딩한 후, 최종 JSON 메시지를 구성하여 Kafka로 발행합니다.

주요 내용: FrameProcessor 스레드 클래스. 여러 워커 스레드로 실행되어 병렬 처리를 수행한다.

main.py:

역할: 서비스의 메인 진입으로. AppConfig를 로드하고, 필요한 Kafka Producer, 처리 큐, 각 입력 소스별 Reader 스레드 및 FrameProcessor 스레드들을 초기화하고 실행하는 역할을 진행한다. 또한, 시그널 처리를 통한 정상 종료 로직을 관리한다.

(향후 확장) FastAPI 등을 사용하여 서비스의 상태를 조회하거나 동적으로 설정을 변경하는 API 엔드포인트를 제공하는데 UI는 별도 컨테이너에서 생성되며, 해당 로직은 DB 에 통합 후 작업 진행 예정

4. 실행 환경 및 주요 의존성

해당 서비스는 무조건 컨테이너화 시켜서 배포만 수행, 로컬에서 테스트 끝나면 쿠버네티스 클러스터내에서 상시 배포 및 오케스트레이션 예정됨

5. 주요 환경 변수 설정

Kafka 관련:

KAFKA_BOOTSTRAP_SERVERS: Kafka 브로커 주소 목록 (예: kafka1:9092,kafka2:9092)

OUTPUT_KAFKA_TOPIC: 융합된 데이터가 발행될 Kafka 토픽 이름 (예: fused_input_for_inference)

KAFKA_PRODUCER_MAX_REQUEST_SIZE: Kafka 프로듀서의 최대 요청 크기 (바이트 단위)

KAFKA_CONSUMER_TIMEOUT_MS: Kafka 이미지 리더의 컨슈머 타임아웃 (밀리초)

입력 소스 관련:

RTSP_SOURCES: 처리할 RTSP 소스 목록. 콤마로 구분하며, 각 항목은 카메라ID=RTSP_URL 형식 (예: cam_lobby=rtsp://ip/stream,cam_entrance=rtsp://ip2/path)

KAFKA_IMAGE_SOURCES_STR: 구독할 Kafka 이미지 토픽 목록. 콤마로 구분하며, 각 항목은 카메라ID=토픽이름 형식 (예: isaac_cam1=isaac_images_raw,physical_cam2_kafka=compressed_images_jpeg)

UWB 관련:

UWB_CONFIGS_JSON: 각 카메라ID별 UWB 데이터 소스 설정을 담은 JSON 문자열. (예: {"cam_lobby": {"type": "api", "url": "http://uwb-server/data"}, "isaac_cam1": {"type": "dummy"}})

이미지 처리 관련:

IMAGE_OUTPUT_FORMAT: 출력 Kafka 메시지에 포함될 이미지 데이터의 포맷 (jpeg 또는 png, 기본 jpeg)

JPEG_QUALITY: JPEG 압축 품질 (0-100, 기본 80)

성능 관련:

PROCESSING_QUEUE_MAX_SIZE: Reader와 Processor 간의 내부 처리 큐 최대 크기

FRAME_PROCESSOR_WORKERS: 이미지 처리 및 Kafka 발행을 수행할 워커 스레드 수

로깅 관련:

LOG_LEVEL: 로그 레벨 (DEBUG, INFO, WARNING, ERROR, 기본 INFO)

서비스 식별:

WRAPPER_INSTANCE_ID: (선택적) 이 Wrapper 서비스 인스턴스의 고유 ID