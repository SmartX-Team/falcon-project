# falcon-wrapper-service/Dockerfile

# 1. 베이스 이미지 선택 (Python 3.9 Slim 버전)
FROM python:3.9-slim-buster

# 2. 환경 변수 설정
ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    LOG_LEVEL="INFO"

# 3. 시스템 패키지 설치
# OpenCV 및 기타 라이브러리 실행에 필요한 최소한의 시스템 의존성
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    # OpenCV 실행에 필요할 수 있는 라이브러리들
    libgl1-mesa-glx \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    ffmpeg \
    curl \
    procps \
    python3-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 5. Python 의존성 설치
# 먼저 requirements.txt 파일만 복사하여 Docker 레이어 캐시 활용
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 3) AI 모델 가중치 미리 다운로드 및 저장
ENV MODEL_DIR=/models
RUN mkdir -p ${MODEL_DIR} && \
    python3 - <<'PY'
import torch, os, pathlib, urllib.request, zipfile

MODEL_DIR_PATH = pathlib.Path(os.environ.get("MODEL_DIR", "/models"))
MODEL_DIR_PATH.mkdir(parents=True, exist_ok=True)

# YOLOv5s 모델 가중치 다운로드 (.pt 파일)
yolo_model_url = "https://github.com/ultralytics/yolov5/releases/download/v7.0/yolov5s.pt"
yolo_model_path = MODEL_DIR_PATH / "yolov5s.pt"
print(f"Downloading YOLOv5s model to {yolo_model_path}...")
try:
    urllib.request.urlretrieve(yolo_model_url, yolo_model_path)
    print("YOLOv5s model downloaded successfully.")
except Exception as e:
    print(f"Error downloading YOLOv5s model: {e}")


# UniDepthV2 ONNX 모델 체크포인트 다운로드
unidepth_model_url = 'https://github.com/nv-tlabs/UniDepth/releases/download/v0.1/unidepth_v2-384x384.onnx'
unidepth_model_path = MODEL_DIR_PATH / "unidepth_v2-384x384.onnx"
print(f"Downloading UniDepthV2 model to {unidepth_model_path}...")
try:
    urllib.request.urlretrieve(unidepth_model_url, unidepth_model_path)
    print("UniDepthV2 model downloaded successfully.")
except Exception as e:
    print(f"Error downloading UniDepthV2 model: {e}")
PY

# 환경 변수 설정
ENV NVIDIA_VISIBLE_DEVICES=all \
    PYTHONUNBUFFERED=1

# 4. 작업 디렉토리 설정
WORKDIR /app

# 6. 애플리케이션 코드 복사
# app 폴더 전체를 컨테이너의 /app/app 경로로 복사
COPY ./app ./app

# 7. 실행 명령어

CMD ["python", "app/inference_main.py"]

# 8. 포트 노출
# 만약 향후 상태 조회를 위한 API 서버(FastAPI 등)를 main.py에 추가한다면 해당 포트 EXPOSE
# EXPOSE 8000
