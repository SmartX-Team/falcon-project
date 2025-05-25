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
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 4. 작업 디렉토리 설정
WORKDIR /app

# 5. Python 의존성 설치
# 먼저 requirements.txt 파일만 복사하여 Docker 레이어 캐시 활용
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 6. 애플리케이션 코드 복사
# app 폴더 전체를 컨테이너의 /app/app 경로로 복사
COPY ./app ./app

# 7. 실행 명령어

CMD ["python", "app/main.py"]
