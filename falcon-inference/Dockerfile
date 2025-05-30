
FROM nvidia/cuda:12.4.1-runtime-ubuntu22.04

# APT 패키지 설치 시 상호작용 프롬프트 비활성화
ARG DEBIAN_FRONTEND=noninteractive
SHELL ["/bin/bash", "-c"]

# 1) 필수 시스템 패키지 설치 (Consolidate and ensure gnupg is included)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3-pip \
        ffmpeg \
        wget \
        gnupg \
        software-properties-common && \
    rm -rf /var/lib/apt/lists/*


# 2) CUDA 저장소 설정 및 cuDNN 설치 (올바른 방법)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget \
        gnupg \
        software-properties-common && \
    # CUDA 저장소 키링 추가 (새로운 방법)
    wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/cuda-keyring_1.0-1_all.deb && \
    dpkg -i cuda-keyring_1.0-1_all.deb && \
    rm cuda-keyring_1.0-1_all.deb && \
    # 저장소 업데이트
    apt-get update && \
    # cuDNN 9 for CUDA 12 설치
    apt-get install -y --no-install-recommends cudnn9-cuda-12 && \
    # TensorRT 설치 (정상적으로 빌드 및 성능 향상은 확인했는데 K8S 환경에서 아직 테스트를 못해봤기에 다시 주석처리해둠 필요시 해제해서 다시 설치하삼)
    #apt-get install -y --no-install-recommends \
    #    tensorrt \
    #    libnvinfer8 \
    #    libnvinfer-plugin8 \
    #    libnvonnxparsers8 \
    #    libnvparsers8 \
    #    python3-libnvinfer && \
    # 정리
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


# 2) 파이썬 의존성 설치 (requirements.txt 기반)
COPY ./requirements.txt /app/requirements.txt
RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir -r /app/requirements.txt

# 3) AI 모델 가중치 미리 다운로드 및 저장
ENV MODEL_DIR=/models
RUN mkdir -p ${MODEL_DIR} && \
    python3 - <<'PY'
import os
import sys 
import pathlib 
import urllib.request 
from urllib.error import URLError, HTTPError 
import traceback 
try:
    from tqdm import tqdm
except ImportError:
    print("tqdm 라이브러리를 찾을 수 없습니다. 진행률 표시줄이 나타나지 않습니다.")
    def tqdm_mock(iterable, *args, **kwargs):
        return iterable 
    tqdm = tqdm_mock 

MODEL_DIR_PATH = pathlib.Path(os.environ.get("MODEL_DIR", "/models"))
MODEL_DIR_PATH.mkdir(parents=True, exist_ok=True) 

def download_with_progress(src_url: str, dst_path: pathlib.Path, model_name: str, chunk_size: int = 8192, timeout_seconds: int = 60) -> bool:
    print(f"{model_name} ({dst_path.name}) 다운로드 중: {src_url} -> {dst_path}")
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        req = urllib.request.Request(src_url, headers=headers)

        with urllib.request.urlopen(req, timeout=timeout_seconds) as response, \
             open(dst_path, "wb") as out_file:
            
            total_size_str = response.headers.get("Content-Length")
            total_size = int(total_size_str) if total_size_str else 0
            
            if 'tqdm_mock' not in globals() or tqdm != globals()['tqdm_mock']: 
                 with tqdm(total=total_size, unit="B", unit_scale=True, desc=dst_path.name) as pbar:
                    while True:
                        chunk = response.read(chunk_size)
                        if not chunk:
                            break
                        out_file.write(chunk)
                        pbar.update(len(chunk))
            else: 
                print(f"{dst_path.name} 다운로드 시작 (크기: {total_size} 바이트, 확인된 경우)...")
                bytes_so_far = 0
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    out_file.write(chunk)
                    bytes_so_far += len(chunk)
                    if total_size > 0:
                        print(f"다운로드 진행: {bytes_so_far}/{total_size} 바이트 ({bytes_so_far*100/total_size:.2f}%)", end='\r')
                    else:
                        print(f"다운로드 진행: {bytes_so_far} 바이트", end='\r')
                print("\n다운로드 완료 (tqdm 진행률 표시줄 없음).")

        print(f"{model_name} ({dst_path.name}) 다운로드 성공.")
        return True
    except HTTPError as e:
        print(f"{model_name} ({dst_path.name}) 다운로드 중 HTTP 오류 {e.code}: {e.reason}")
    except URLError as e:
        print(f"{model_name} ({dst_path.name}) 다운로드 중 URL 오류: {e.reason}")
    except TimeoutError: 
        print(f"{model_name} ({dst_path.name}) 다운로드 중 {timeout_seconds}초 후 타임아웃 오류 발생.")
    except Exception as e_general:
        print(f"{model_name} ({dst_path.name}) 다운로드 중 예상치 못한 오류 발생: {e_general}")
    
    if dst_path.exists():
        try:
            dst_path.unlink()
            print(f"부분적으로 다운로드된 파일 정리: {dst_path.name}")
        except OSError as e_clean:
            print(f"파일 정리 중 오류 {dst_path.name}: {e_clean}")
    return False

# --- YOLOv5s 모델 가중치 다운로드 ---
yolo_model_url = "https://github.com/ultralytics/yolov5/releases/download/v7.0/yolov5s.pt"
yolo_model_path = MODEL_DIR_PATH / "yolov5s.pt"
yolo_downloaded_successfully = False
if not yolo_model_path.exists(): 
    if download_with_progress(yolo_model_url, yolo_model_path, "YOLOv5s"):
        yolo_downloaded_successfully = True
    else:
        print("YOLOv5s 모델 다운로드 실패. 빌드를 중단합니다.")
        sys.exit(1) 
else:
    print(f"YOLOv5s 모델이 이미 {yolo_model_path}에 존재합니다. 다운로드를 건너<0xEB><0x9B><0x84>니다.")
    yolo_downloaded_successfully = True


# --- UniDepthV2 ONNX 모델 체크포인트 다운로드 ---
unidepth_model_url = 'https://huggingface.co/ibaiGorordo/unidepth-v2-vits14-onnx/resolve/main/unidepthv2_vits14_simp.onnx'
unidepth_model_path = MODEL_DIR_PATH / "unidepth_v2-384x384.onnx" 
unidepth_downloaded_successfully = False
if not unidepth_model_path.exists(): 
    if download_with_progress(unidepth_model_url, unidepth_model_path, "UniDepthV2"):
        unidepth_downloaded_successfully = True
    else:
        print("UniDepthV2 모델 다운로드 실패. 빌드를 중단합니다.")
        sys.exit(1) 
else:
    print(f"UniDepthV2 모델이 이미 {unidepth_model_path}에 존재합니다. 다운로드를 건너<0xEB><0x9B><0x84>니다.")
    unidepth_downloaded_successfully = True

if not (yolo_downloaded_successfully and unidepth_downloaded_successfully):
    print("하나 이상의 필수 모델 다운로드에 실패했습니다. 빌드를 중단합니다.")
    sys.exit(1)

print("모든 필수 모델이 성공적으로 준비되었습니다.")
PY

# 환경 변수 설정
ENV NVIDIA_VISIBLE_DEVICES=all \
    PYTHONUNBUFFERED=1

# 작업 디렉토리 설정
WORKDIR /app

# 4) 애플리케이션 코드 복사
COPY ./app /app 

# 5) 컨테이너 실행 시 실행될 명령어
CMD ["python3", "inference_main.py"]
