# falcon-inference/base/Dockerfile
# 공통 베이스 이미지: CUDA 런타임, Python, 주요 AI/ML 라이브러리 및 모델 가중치 포함
FROM nvidia/cuda:12.4.1-runtime-ubuntu22.04

# APT 패키지 설치 시 상호작용 프롬프트 비활성화
ARG DEBIAN_FRONTEND=noninteractive
SHELL ["/bin/bash", "-c"]

# 1) 필수 시스템 패키지 설치
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3-pip \
        ffmpeg \
        wget \
    && rm -rf /var/lib/apt/lists/*

# 2) 파이썬 의존성 설치 (requirements.txt 기반)
COPY requirements.txt /tmp/
RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir -r /tmp/requirements.txt

# 3) AI 모델 가중치 미리 다운로드 및 저장
#    (이미지 빌드 시점에 수행되어, 런타임 시 빠른 로딩 가능)
ENV MODEL_DIR=/models
RUN mkdir -p ${MODEL_DIR} && \
    python3 - <<'PY'
import torch, os, pathlib, urllib.request, zipfile

MODEL_DIR_PATH = pathlib.Path(os.environ.get("MODEL_DIR", "/models"))
MODEL_DIR_PATH.mkdir(parents=True, exist_ok=True)

# YOLOv5s 모델 가중치 다운로드 (.pt 파일)
# torch.hub.load는 캐시 디렉토리에 저장하므로, 직접 다운로드하여 /models에 저장하는 것이 명확함
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

# 작업 디렉토리 설정
WORKDIR /app