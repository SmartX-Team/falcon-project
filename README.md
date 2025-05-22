# falcon-project
25년도 트윈 환경에 맞춰 구조를 최적화한 Falcon Project


Falcon 프로젝트 (2025 트윈 환경 최적화)
본 문서는 2025년 디지털 트윈 환경 운영을 목표로 기존 Falcon 프로젝트의 아키텍처를 개선하고 최적한 Falcon Project 결과물을 정리하고 있습니다.
핵심 변경 사항은 클라우드 네이티브하게 카메라 스트리밍과 AI 추론 로직을 분리하여 확장성, 유연성 및 GPU 사용 효율성을 높혔습니다.

### 1. 기존 Falcon 컨테이너 방식의 한계점

https://github.com/NetAiFalcon/falcon
조민준 , 김철희 학부생 인턴분들이 작업해주신 Falcon Project 는
컨테이너 기반으로 프로젝트의 초기 목표는 성공적으로 달성하였으나

다음과 같은 주요 한계점을 가지고 있었습니다:

* 물리적 제약: 카메라 장치와 AI 추론을 위한 GPU 장비가 반드시 동일한 호스트 머신에 물리적으로 연결되어 배포 및 운영되어야 했습니다. 이는 다양한 위치에 있는 카메라들을 중앙 집중식 또는 분산된 GPU 클러스터에서 효율적으로 처리하는 데 제약이 되었습니다.

* 단일 컨테이너의 다중 책임: 하나의 컨테이너가 카메라 캡처, AI 추론 (YOLOv5, UniDepthV2 등), 결과 데이터 전송 등 여러 주요 기능을 모두 담당했습니다. 이로 인해 특정 기능의 확장이나 업데이트, 장애 발생 시 전체 시스템에 영향을 미치는 등 유연성과 안정성이 저하되었습니다.

* 서비스 확장성 제약: 여러 카메라 스트림을 처리하기 위해 다수의 Falcon 컨테이너 배포하기 위해서는 인스턴스 수 만큼 GPU 자원을 장착한 물리적인 호스트 머신이 필요하여 확장성에 제약이 있음

### 2. 목표 아키텍처: 분산 스트리밍 및 클라우드 기반 추론
이러한 한계를 극복하기 위해, 본 Repo에서는 다음과 같이 스트리밍과 추론 기능을 명확히 분리하고, 연구실 자체 Kubernetes 클러스터(클라우드 기반 인프라)를 활용하는 개선된 아키텍처는 아래와 같습니다.

![Falcon-25year](https://github.com/user-attachments/assets/b935ab3d-04b4-4502-a843-07c5b47852aa)



#### Camera Agent (컨테이너, 기존 개발된 Agent 활용):

해당 Repo 참조: https://github.com/SmartX-Team/camera-agent.git

입력: 로컬 카메라 장치 (/dev/video* 등 실제 하드웨어 카메라) 또는 ROS2 이미지 토픽 구독 (현실 허스키/Isaac Sim 내 가상 카메라 등)으로 RTSP/ Kafka 영상 스트리밍을 지원

처리:

이미지/비디오 데이터 획득.

이미지/비디오 압축: H.264 (RTSP 스트리밍 시) 또는 JPEG (Kafka 전송 시 이미지 압축) 등 코덱으로 압축.
압축된 데이터 또는 메타데이터를 포함한 JSON 페이로드 구성.

출력:

RTSP 스트리밍: H.264 등으로 인코딩된 비디오를 RTSP 프로토콜을 통해 Inference 서비스 또는 다른 RTSP 클라이언트에 직접 스트리밍.

Kafka 발행: (압축된) 이미지 데이터 또는 비디오 프레임 정보를 JSON 형태로 Kafka의 특정 토픽 (예: raw_video_frames, compressed_video_frames)으로 발행. 디지털 트윈 환경에서 직접 사용하거나, Inference 서비스가 이 토픽을 구독할 수도 있습니다.

Visibility 서버 연동:

시작 시 자신(Agent) 및 관리하는 카메라의 상세 정보를 Visibility 서버에 등록 (/agent_register).

주기적으로 자신의 실제 데이터 전송 상태(RTSP 스트리밍 상태, Kafka 발행 상태 등)를 Visibility 서버에 업데이트 (/agent_update_status).

특징: 경량화, GPU 불필요 (인코딩 시 CPU 약간 사용). 각 카메라 소스 가까이에 배포 가능. "Agent당 카메라 하나" 또는 소수 카메라 관리.

### Inference 서비스 (Kubernetes 클러스터 내 컨테이너, GPU 사용):

입력:

RTSP 구독: Camera Agent가 제공하는 RTSP 스트림을 구독하여 비디오 프레임 수신 (OpenCV + FFmpeg 등 사용).

(또는) Kafka 구독: Camera Agent가 Kafka로 발행한 video_frames 토픽을 구독하여 이미지/비디오 데이터 수신.

처리:

수신된 데이터 디코딩 및 이미지 재구성.

YOLOv5, UniDepthV2 등 AI 모델을 사용하여 객체 검출 및 깊이 추론 수행.

Kubernetes 환경의 GPU 자원을 효율적으로 사용 (예: NVIDIA Triton Inference Server 활용 고려). GPU 사용 최적화 (FP16, TensorRT 등).

출력: 추론 결과(객체 바운딩 박스, 클래스, 신뢰도, 깊이 정보 등)를 JSON 형태로 Kafka의 특정 토픽 (예: inference_results)으로 발행.

확장성: Kubernetes의 Horizontal Pod Autoscaler (HPA) 등을 활용하여 부하에 따라 자동으로 인스턴스 수 확장/축소 가능.

### 디지털 트윈 / 실시간 시각화 환경 (컨슈머):

inference_results 토픽 (Inference 서비스가 발행)을 카프카 토픽을 구독하여 Falcon이 추정한 사람 위치를  실시간으로 시각화.








