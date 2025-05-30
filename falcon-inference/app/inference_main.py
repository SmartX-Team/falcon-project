
import os
import json
import logging
import signal
import threading
import base64
import time
from datetime import datetime, timezone 

import cv2
import numpy as np
import torch
from ultralytics import YOLO
import onnxruntime as ort
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import math

# --- 로깅 설정 ---
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - %(name)s - [%(threadName)s] - %(message)s'
)
logger = logging.getLogger(__name__)

# --- 환경 변수에서 설정값 로드 ---
KAFKA_BROKER_LIST = os.environ.get('KAFKA_BROKER_LIST', '10.79.1.1:9094').split(',')
INPUT_TOPIC = os.environ.get('INPUT_TOPIC', 'fused_input_for_inference') 
OUTPUT_TOPIC_INFERENCE = os.environ.get('OUTPUT_TOPIC_INFERENCE', 'inference_results') 
GROUP_ID = os.environ.get('GROUP_ID', 'falcon-inference-group')

MODEL_DIR = os.environ.get('MODEL_DIR', '/models')
YOLO_MODEL_PATH = os.path.join(MODEL_DIR, 'yolov5s.pt') 
UNIDEPTH_MODEL_PATH = os.path.join(MODEL_DIR, 'unidepth_v2-384x384.onnx') 

ORT_PROVIDERS = ['CUDAExecutionProvider', 'CPUExecutionProvider']

X_CORRECTION_LEFT_THRESHOLD = float(os.environ.get('X_CORRECTION_LEFT_THRESHOLD', 384.0))
X_CORRECTION_RIGHT_THRESHOLD = float(os.environ.get('X_CORRECTION_RIGHT_THRESHOLD', 1536.0))
X_CORRECTION_OFFSET = float(os.environ.get('X_CORRECTION_OFFSET', 1.0)) 
DEPTH_OFFSET_FACTOR = float(os.environ.get('DEPTH_OFFSET_FACTOR', -1.0)) 

CAMERA_YAW_DEGREES = float(os.environ.get('CAMERA_YAW_DEGREES', 0.0))  # 카메라 Yaw각 (degree)
CAMERA_DEGREE_RESERVED = float(os.environ.get('CAMERA_DEGREE_RESERVED', 0.0))  # 향후 확장용

# 보정 계수 추가
DEPTH_SCALING_FACTOR = float(os.environ.get('DEPTH_SCALING_FACTOR', 0.9))  # 깊이값 스케일링
LATERAL_SCALING_FACTOR = float(os.environ.get('LATERAL_SCALING_FACTOR', 0.3))  # 좌우 오프셋 스케일링

CAMERA_HORIZONTAL_FOV_DEGREES = float(os.environ.get('CAMERA_HORIZONTAL_FOV_DEGREES', 77.0))  # 수평 FOV (degree)
CAMERA_VERTICAL_FOV_DEGREES = float(os.environ.get('CAMERA_VERTICAL_FOV_DEGREES', 45.0))    # 수직 FOV (degree, 선택사항)

# UWB 공간 좌표계 정의 혹시 이후에 변경되면 적당히 수정
UWB_SPACE_X_MIN = float(os.environ.get('UWB_SPACE_X_MIN', 5.4))     # 왼쪽 경계
UWB_SPACE_X_MAX = float(os.environ.get('UWB_SPACE_X_MAX', 34.29))   # 오른쪽 경계  
UWB_SPACE_Y_MIN = float(os.environ.get('UWB_SPACE_Y_MIN', 7.29))    # 위쪽(북쪽) 경계
UWB_SPACE_Y_MAX = float(os.environ.get('UWB_SPACE_Y_MAX', 34.0))    # 아래쪽(남쪽) 경계

# 환경변수 추가 (기존 환경변수 섹션에 추가)
INFERENCE_MAX_FPS = float(os.environ.get('INFERENCE_MAX_FPS', '3.0'))  # 기본값: 초당 2프레임
INFERENCE_SKIP_STRATEGY = os.environ.get('INFERENCE_SKIP_STRATEGY', 'DROP_OLD').upper()  # DROP_OLD 또는 THROTTLE

# 전역 변수 추가 (기존 전역 변수 섹션에 추가)
last_inference_time_by_camera = {}  # 카메라별 마지막 인퍼런스 시간 추적
inference_interval = 1.0 / INFERENCE_MAX_FPS if INFERENCE_MAX_FPS > 0 else 0
message_consume_count = 0  # 소비한 메시지 수 (디버깅용)
inference_perform_count = 0  # 실제 수행한 인퍼런스 수 (디버깅용)
# --- 전역 변수 ---
stop_event = threading.Event()
yolo_model = None
unidepth_session = None
# kafka_producer는 consume_messages 함수 내에서 지역 변수로 관리하고, 필요시 클래스 멤버 등으로 변경 가능
# 여기서는 consume_messages 함수 내에서만 사용되도록 수정
KAFKA_PRODUCER_ACKS_ENV = os.environ.get("KAFKA_PRODUCER_ACKS", "1")
if KAFKA_PRODUCER_ACKS_ENV.lower() == 'all':
    KAFKA_ACKS_CONFIG = 'all'
else:
    try:
        KAFKA_ACKS_CONFIG = int(KAFKA_PRODUCER_ACKS_ENV)
    except ValueError:
        logger.warning(f"Invalid KAFKA_PRODUCER_ACKS value '{KAFKA_PRODUCER_ACKS_ENV}'. Defaulting to 1.")
        KAFKA_ACKS_CONFIG = 1
KAFKA_RETRIES_CONFIG = int(os.environ.get("KAFKA_PRODUCER_RETRIES", "5"))



def should_process_inference(camera_id, current_time):
    """
    인퍼런스를 수행할지 결정하는 함수
    
    Args:
        camera_id: 카메라 ID
        current_time: 현재 시간 (time.monotonic())
    
    Returns:
        bool: 인퍼런스를 수행할지 여부
    """
    global last_inference_time_by_camera
    
    if INFERENCE_MAX_FPS <= 0:
        return True  # 제한 없음
    
    last_time = last_inference_time_by_camera.get(camera_id, 0)
    time_elapsed = current_time - last_time
    
    if time_elapsed >= inference_interval:
        last_inference_time_by_camera[camera_id] = current_time
        return True
    else:
        return False


def calculate_position_with_yaw_and_fov(base_uwb_x, base_uwb_y, depth_value, cx, cy, image_width, image_height, yaw_degrees, horizontal_fov_degrees):
    """
    카메라의 Yaw축을 고려하여 실제 월드 좌표를 계산

    UWB 좌표계 특성:
    - X: 5.4(서쪽) ~ 34.29(동쪽)
    - Y: 7.29(북쪽) ~ 34.0(남쪽) - Y값이 작을수록 북쪽

    # UWB 좌표계에 맞춘 Yaw 각도 변환
    # 0도 = 북쪽(Y 감소 방향), 90도 = 동쪽(X 증가 방향)
    # 180도 = 남쪽(Y 증가 방향), 270도 = 서쪽(X 감소 방향)

    Args:
        base_uwb_x, base_uwb_y: UWB로부터 얻은 카메라 위치
        depth_value: 깊이 값 (미터)
        cx: 이미지 내 객체의 X 중심 좌표
        image_width: 이미지 전체 너비
        yaw_degrees: 카메라 Yaw 각도 (도 단위)
        horizontal_fov_degrees: 카메라 수평 FOV (도 단위)
    
    Returns:
        tuple: (calculated_x, calculated_y)


    """

    # 깊이값 보정 적용
    corrected_depth = depth_value * DEPTH_SCALING_FACTOR

    # Yaw를 라디안으로 변환
    yaw_rad = math.radians(yaw_degrees)
    horizontal_fov_rad = math.radians(horizontal_fov_degrees)

     # 이미지 중심으로부터의 픽셀 오프셋 계산
    center_x = image_width / 2
    center_y = image_height / 2
    pixel_offset_x = cx - center_x
    pixel_offset_y = cy - center_y
    
    # FOV를 사용하여 픽셀 오프셋을 실제 각도로 변환
    # 수평 각도: 이미지 가장자리에서 FOV의 절반만큼 벗어남
    angle_per_pixel_horizontal = horizontal_fov_rad / image_width
    horizontal_angle_offset = pixel_offset_x * angle_per_pixel_horizontal   
    # 이미지 중심 기준으로 객체의 상대적 위치 계산 (-1 ~ 1 범위)
    # cx가 이미지 중앙이면 0, 왼쪽 끝이면 -1, 오른쪽 끝이면 1
    relative_x = (cx - image_width / 2) / (image_width / 2)
    
    # 카메라 시야각을 고려한 실제 각도 오프셋 계산
    # 실제 물리적 오프셋 계산 (삼각법 사용) + 스케일링 적용
    lateral_distance_raw = corrected_depth * math.tan(horizontal_angle_offset)
    lateral_distance = lateral_distance_raw * LATERAL_SCALING_FACTOR
    
    # Yaw 각도를 고려하여 월드 좌표계로 변환
    # 카메라가 북쪽(Y+)을 향하고 있다면 yaw=0
    # 시계방향으로 회전시 yaw 증가
    world_x_offset = lateral_distance  * math.cos(yaw_rad + math.pi/2) + corrected_depth * math.cos(yaw_rad)
    world_y_offset = lateral_distance  * math.sin(yaw_rad + math.pi/2) + corrected_depth * math.sin(yaw_rad)
    
    calculated_x = float(base_uwb_x) + world_x_offset
    calculated_y = float(base_uwb_y) + world_y_offset

    angle_from_center_degrees = math.degrees(horizontal_angle_offset)
    
    # UWB 공간 경계 체크 및 경고
    if not (UWB_SPACE_X_MIN <= calculated_x <= UWB_SPACE_X_MAX):
        logger.warning(f"Calculated X coordinate ({calculated_x:.2f}) is outside UWB space bounds "
                      f"[{UWB_SPACE_X_MIN} ~ {UWB_SPACE_X_MAX}]")
    
    if not (UWB_SPACE_Y_MIN <= calculated_y <= UWB_SPACE_Y_MAX):
        logger.warning(f"Calculated Y coordinate ({calculated_y:.2f}) is outside UWB space bounds "
                      f"[{UWB_SPACE_Y_MIN} ~ {UWB_SPACE_Y_MAX}]")
    
    return calculated_x, calculated_y, angle_from_center_degrees

# --- 모델 로딩 함수 --- (이전과 동일)
def load_yolo_model():
    global yolo_model
    try:
        logger.info(f"Attempting to load YOLOv5 model from: {YOLO_MODEL_PATH}")
        model = YOLO(YOLO_MODEL_PATH)

        if torch.cuda.is_available():
            logger.info("YOLO model will attempt to use GPU.")
            # CUDA 최적화 설정
            #model.model.half()  # FP16 사용
            model.model.cuda()
            
        else:
            logger.info("YOLO model will use CPU.")

      
        yolo_model = model
        logger.info(f"YOLOv5 model loaded successfully from {YOLO_MODEL_PATH}")

        # ---클래스 이름 로깅 추가 지점 시작 ---
        if yolo_model and hasattr(yolo_model, 'names'):
            logger.info(f"YOLO model class names (ID: Name): {yolo_model.names}")
            if 'person' in yolo_model.names.values():
                logger.info("The 'person' class IS PRESENT in the loaded YOLO model's names.")
            else:
                logger.warning("The 'person' class IS MISSING from the loaded YOLO model's names.")
        else:
            logger.warning("Could not retrieve class names from the loaded YOLO model (model or names attribute not found).")
        # --- 클래스 이름 로깅 추가 지점 끝 ---

    except Exception as e:
        logger.error(f"Error loading YOLOv5 model: {e}", exc_info=True)
        yolo_model = None

def load_unidepth_model():
    global unidepth_session
    try:
        sess_options = ort.SessionOptions()

        #성능 최적화 옵션들
        sess_options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL

        # 🎯 Provider 순서 최적화 (TensorRT 우선) -> TensorRT 잘 동작하는거 확인했는데, 대신 K8S 에서 캐쉬 잘 저장하고 불러오는지 체크 필요
        providers = [
            #('TensorrtExecutionProvider', {
            #    'trt_max_workspace_size': 4 * 1024 * 1024 * 1024,  # 4GB
            #    'trt_fp16_enable': True,  # Mixed precision
            #    'trt_engine_cache_enable': True,
            #    'trt_engine_cache_path': '/tmp/trt_cache',
            #    'trt_timing_cache_enable': True,
            #}),            
            ('CUDAExecutionProvider', {
                'device_id': 0,
                'arena_extend_strategy': 'kSameAsRequested',
                'gpu_mem_limit': 8 * 1024 * 1024 * 1024,  # 8GB
                'cudnn_conv_algo_search': 'EXHAUSTIVE',
            }),
            'CPUExecutionProvider'
        ]
        
        unidepth_session = ort.InferenceSession(
            UNIDEPTH_MODEL_PATH, 
            sess_options=sess_options, 
            providers=providers
        )


        logger.info(f"UniDepthV2 ONNX model loaded successfully from {UNIDEPTH_MODEL_PATH} using providers: {unidepth_session.get_providers()}")
        if "CUDAExecutionProvider" not in unidepth_session.get_providers():
            logger.warning("UniDepthV2: CUDAExecutionProvider not available or not used. Check ONNXRuntime-GPU installation and CUDA setup.")
    except Exception as e:
        logger.error(f"Error loading UniDepthV2 ONNX model: {e}", exc_info=True)
        unidepth_session = None

# --- 이미지 전처리 및 추론 함수 --- (이전과 동일)
def preprocess_image_for_yolo(image_np):
    return image_np

def preprocess_image_for_unidepth(image_np, target_height=364, target_width=644): # 기본값 사용
    logger.debug(f"Preprocessing image for UniDepth. Original shape: {image_np.shape}, Target HxW: {target_height}x{target_width}")
    img_rgb = cv2.cvtColor(image_np, cv2.COLOR_BGR2RGB)
    
    # 💡 cv2.resize에는 (너비, 높이) 순서로 전달
    # 새로운 매개변수명을 사용하여 (target_width, target_height) 튜플을 만듭니다.
    dsize = (target_width, target_height) 
    img_resized = cv2.resize(img_rgb, dsize, interpolation=cv2.INTER_AREA)
    
    logger.debug(f"Resized image shape for UniDepth: {img_resized.shape}")
    img_normalized = img_resized.astype(np.float32) / 255.0
    img_chw = np.transpose(img_normalized, (2, 0, 1))  # HWC to CHW
    img_nchw = np.expand_dims(img_chw, axis=0)      # Add batch dimension: NCHW
    logger.debug(f"Final preprocessed shape for UniDepth: {img_nchw.shape}")
    return img_nchw

def run_yolo_inference(image_np):
    if yolo_model is None:
        logger.warning("YOLO model is not loaded. Skipping detection.")
        return []
    try:
        results = yolo_model.predict(source=image_np, verbose=False) 
        detections = []
        if results and results[0].boxes:
            logger.info(f"YOLO raw detection count: {len(results[0].boxes)}")
            for box in results[0].boxes:
                xyxy = box.xyxy[0].cpu().numpy().tolist()
                conf = float(box.conf[0].cpu().numpy())
                cls_id = int(box.cls[0].cpu().numpy())
                cls_name = yolo_model.names[cls_id]

                 # 'person' 클래스에 대한 추가적인 로깅
                if cls_name == 'person':
                    logger.info(f"'person' detected with confidence: {conf:.4f}. Box: {xyxy}")

                detections.append({
                    "box_xyxy": xyxy,
                    "confidence": conf,
                    "class_id": cls_id,
                    "class_name": cls_name
                })
        return detections
    except Exception as e:
        logger.error(f"Error during YOLO inference: {e}", exc_info=True)
        return []

def run_unidepth_inference(image_np_nchw):
    if unidepth_session is None:
        logger.warning("UniDepth model is not loaded. Skipping depth estimation.")
        return None
    try:
        input_name = unidepth_session.get_inputs()[0].name
        #depth_map_onnx = unidepth_session.run(None, {input_name: image_np_nchw})[0]
        outputs     = unidepth_session.run(None, {input_name: image_np_nchw})

        depth_map   = np.squeeze(outputs[1])  
        K_matrix    = outputs[0] 
        logger.debug(f"UniDepth outputs — K:{K_matrix.shape}, depth:{depth_map.shape}")
        #depth_map_hw = np.squeeze(depth_map_onnx)
        return depth_map

    except Exception as e:
        logger.error(f"Error during UniDepth inference: {e}", exc_info=True)
        return None

# --- Kafka 컨슈머 및 메시지 처리 ---
def consume_messages():
    if not yolo_model or not unidepth_session: 
        logger.error("One or both AI models (YOLO, UniDepth) are not loaded. Cannot start consuming messages.")
        return

    consumer = None # finally 블록에서 사용하기 위해 초기화
    kafka_producer_instance = None # finally 블록에서 사용하기 위해 초기화

    try: # Main try block for resource management
        # KafkaConsumer 초기화
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BROKER_LIST,
                group_id=GROUP_ID,
                auto_offset_reset='latest', 
                consumer_timeout_ms=1000, 
                value_deserializer=lambda v: json.loads(v.decode('utf-8', 'ignore'))
            )
            logger.info(f"Kafka consumer initialized for topic '{INPUT_TOPIC}' with group_id '{GROUP_ID}'.")
        except KafkaError as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}", exc_info=True)
            return # 컨슈머 초기화 실패 시 함수 종료

        # KafkaProducer 초기화
        try:
            kafka_producer_instance = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER_LIST,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks=KAFKA_ACKS_CONFIG, 
                retries=KAFKA_RETRIES_CONFIG 
            )
            logger.info(f"Kafka producer initialized for output topic '{OUTPUT_TOPIC_INFERENCE}'.")
        except KafkaError as e:
            logger.error(f"Failed to initialize Kafka producer: {e}", exc_info=True)
            kafka_producer_instance = None # 프로듀서 없이 진행 가능하도록 설정

        logger.info("Starting message consumption loop...")
        while not stop_event.is_set():
            try: # 메시지 폴링 및 처리를 위한 내부 try 블록
                for message in consumer: # consumer_timeout_ms 동안 블록
                    if stop_event.is_set():
                        break # 외부 종료 신호 감지 시 루프 탈출
                    current_process_start_time = time.monotonic()
                    logger.debug(f"Received message: {message.topic}, partition={message.partition}, offset={message.offset}")
                    
                    # --- 개별 메시지 처리 로직 ---
                    try: # 개별 메시지 처리에 대한 try-except
                        fused_data = message.value
                        if not isinstance(fused_data, dict):
                            logger.warning(f"Skipping message, not a valid JSON object: Received type: {type(fused_data)}")
                            continue

                        camera_id = fused_data.get("camera_id")
                        image_base64 = fused_data.get("image_data_b64") 
                        original_timestamp_str = fused_data.get("image_timestamp_utc") 
                        uwb_data_from_wrapper = fused_data.get("uwb_data") 
                        # Kafka 메시지에서 YAW 값 추출 (없으면 환경변수 기본값 사용)
                        message_yaw_degrees = fused_data.get("camera_yaw_degrees")
                        if message_yaw_degrees is not None:
                            try:
                                effective_yaw_degrees = float(message_yaw_degrees)
                                logger.debug(f"[{camera_id}] Using YAW from message: {effective_yaw_degrees}°")
                            except (ValueError, TypeError):
                                effective_yaw_degrees = CAMERA_YAW_DEGREES
                                logger.warning(f"[{camera_id}] Invalid YAW in message ({message_yaw_degrees}). Using default: {CAMERA_YAW_DEGREES}°")
                        else:
                            effective_yaw_degrees = CAMERA_YAW_DEGREES
                            logger.debug(f"[{camera_id}] No YAW in message. Using default: {CAMERA_YAW_DEGREES}")

                        # Kafka 메시지에서 FOV 값 추출 (없으면 환경변수 기본값 사용)
                        message_horizontal_fov = fused_data.get("camera_horizontal_fov_degrees")
                        if message_horizontal_fov is not None:
                            try:
                                effective_horizontal_fov = float(message_horizontal_fov)
                                logger.debug(f"[{camera_id}] Using horizontal FOV from message: {effective_horizontal_fov}°")
                            except (ValueError, TypeError):
                                effective_horizontal_fov = CAMERA_HORIZONTAL_FOV_DEGREES
                                logger.warning(f"[{camera_id}] Invalid FOV in message ({message_horizontal_fov}). Using default: {CAMERA_HORIZONTAL_FOV_DEGREES}°")
                        else:
                            effective_horizontal_fov = CAMERA_HORIZONTAL_FOV_DEGREES
                            logger.debug(f"[{camera_id}] No FOV in message. Using default: {CAMERA_HORIZONTAL_FOV_DEGREES}°")


                        missing_fields = []
                        if not camera_id: missing_fields.append(f"camera_id(value:{camera_id})")
                        if not image_base64: missing_fields.append(f"image_base64(status:{'None or Empty' if not image_base64 else 'Present'})")
                        if not original_timestamp_str: missing_fields.append(f"image_timestamp_utc(value:{original_timestamp_str})")

                        if missing_fields: 
                            logger.warning(f"[{camera_id if camera_id else 'UnknownCam'}] [MISSING_FIELDS_SKIP] Skipping message. Missing/invalid fields: {'; '.join(missing_fields)}")
                            continue

                        logger.debug(f"[{camera_id}] All required fields present. Processing message.")

                        should_infer = should_process_inference(camera_id, current_process_start_time)

                        if not should_infer:
                                # 메시지는 소비했지만 인퍼런스는 스킵
                            logger.debug(f"[{camera_id}] [RATE_LIMITED] Message consumed but inference skipped (FPS limit: {INFERENCE_MAX_FPS})")
                            continue


                        img_bytes = base64.b64decode(image_base64)
                        img_np_bgr = cv2.imdecode(np.frombuffer(img_bytes, np.uint8), cv2.IMREAD_COLOR)
                        if img_np_bgr is None:
                            logger.warning(f"Failed to decode image for camera_id '{camera_id}'. Skipping.")
                            continue
                        
                        # ... (YOLO, UniDepth 추론 및 위치 계산 로직 -
                        yolo_input_img = preprocess_image_for_yolo(img_np_bgr)
                        detections = run_yolo_inference(yolo_input_img)
                        logger.info(f"[{camera_id}] YOLO detected {len(detections)} objects.")
                        num_persons_detected_yolo = sum(1 for d in detections if d['class_name'] == 'person')
                        unidepth_input_img_nchw = preprocess_image_for_unidepth(img_np_bgr)
                        depth_map_hw = run_unidepth_inference(unidepth_input_img_nchw)
                        
                        person_locations = [] 
                        num_persons_located = len(person_locations)
                        if depth_map_hw is not None:
                            logger.info(f"[{camera_id}] UniDepth estimation successful. Original depth map shape: {depth_map_hw.shape}")
                            
                            # --- 💡 변경/추가 지점 시작 (깊이맵 업샘플링) ---
                            depth_map_full_resolution = None # 업샘플링된 깊이맵을 저장할 변수
                            try:
                                orig_h, orig_w = img_np_bgr.shape[:2] # 원본 이미지의 높이, 너비
                                # cv2.resize는 (너비, 높이) 순서로 인자를 받습니다.
                                depth_map_full_resolution = cv2.resize(depth_map_hw, (orig_w, orig_h), interpolation=cv2.INTER_CUBIC) # 또는 cv2.INTER_LINEAR
                                logger.debug(f"[{camera_id}] Upsampled depth map to original resolution: {depth_map_full_resolution.shape}")
                            except Exception as e_resize:
                                logger.error(f"[{camera_id}] Error upsampling depth map: {e_resize}", exc_info=True)
                                # 업샘플링 실패 시 depth_map_full_resolution는 None으로 유지됩니다.
                            # --- 💡 변경/추가 지점 끝 (깊이맵 업샘플링) ---

                            # 업샘플링된 깊이맵이 유효할 때만 다음 로직 진행
                            if depth_map_full_resolution is not None:
                                base_uwb_x, base_uwb_y, base_uwb_tag_id = None, None, None
                                if uwb_data_from_wrapper is None:
                                    logger.debug(f"[{camera_id}] [UWB_IS_NULL] UWB data from wrapper is null.")
                                elif isinstance(uwb_data_from_wrapper, dict):
                                    base_uwb_x = uwb_data_from_wrapper.get('x_m') 
                                    base_uwb_y = uwb_data_from_wrapper.get('y_m')
                                    base_uwb_tag_id = uwb_data_from_wrapper.get('tag_id') 
                                    if base_uwb_x is None or base_uwb_y is None:
                                        logger.warning(f"[{camera_id}] [UWB_MISSING_XY] UWB data from wrapper is missing 'x_m' or 'y_m'. UWB data: {format_data_for_logging(uwb_data_from_wrapper)}") # format_data_for_logging 함수 필요
                                else:
                                    logger.warning(f"[{camera_id}] [UWB_INVALID_TYPE] UWB data from wrapper is not a dictionary or null. Type: {type(uwb_data_from_wrapper)}. Data preview: {format_data_for_logging(uwb_data_from_wrapper)}")


                                for detection in detections:
                                    if detection['class_name'] == 'person': 
                                        box = detection['box_xyxy']
                                        xmin, ymin, xmax, ymax = box[0], box[1], box[2], box[3]
                                        cx = (xmin + xmax) / 2
                                        cy = (ymin + ymax) / 2

                                        depth_value = None # 초기화
                                        # 💡 변경/추가 지점: 업샘플링된 깊이맵(depth_map_full_resolution)에서 깊이 값 가져오기
                                        # 좌표 범위 검사도 원본 이미지 크기(orig_h, orig_w) 기준
                                        if 0 <= int(cy) < orig_h and 0 <= int(cx) < orig_w:
                                            depth_value = depth_map_full_resolution[int(cy), int(cx)] + DEPTH_OFFSET_FACTOR 
                                        else:
                                            logger.warning(f"[{camera_id}] Person center ({cx:.2f},{cy:.2f}) out of upsampled depth map bounds ({orig_w},{orig_h}). Skipping depth for this person.")
                                        
                                        # 💡 디버그 로그 추가
                                        logger.debug(f"[{camera_id}] Upsampled depth map shape: {depth_map_full_resolution.shape}, Person center in original image: ({cx:.2f}, {cy:.2f}), Calculated depth_value: {depth_value}")

                                        calculated_x, calculated_y = None, None
                                        if base_uwb_x is not None and base_uwb_y is not None and depth_value is not None:
                                            # Yaw축을 고려한 위치 계산 (메시지의 YAW 값 또는 환경변수 기본값 사용)
                                            calculated_x, calculated_y, angle_from_center = calculate_position_with_yaw_and_fov(
                                                        base_uwb_x, base_uwb_y, depth_value, cx, cy, orig_w, orig_h, 
                                                        effective_yaw_degrees, effective_horizontal_fov
                                            )
                                            
                                            person_locations.append({
                                                "person_id": f"person_{len(person_locations)+1}", 
                                                "box_xyxy": [round(coord, 2) for coord in box],
                                                "confidence": round(detection['confidence'], 4),
                                                "center_image_coord": (round(cx,2), round(cy,2)),
                                                "depth_value_at_center": round(depth_value,3) if depth_value is not None else None,
                                                "estimated_world_x": round(calculated_x,3) if calculated_x is not None else None,
                                                "estimated_world_y": round(calculated_y,3) if calculated_y is not None else None,
                                                "base_uwb_used": {"tag_id": base_uwb_tag_id, "x": base_uwb_x, "y": base_uwb_y},
                                                "camera_yaw_degrees": effective_yaw_degrees,  # 실제 사용된 YAW 값
                                                "relative_x_in_image": round((cx - orig_w / 2) / (orig_w / 2), 3)
                                            })
                                        else:
                                            logger.debug(f"[{camera_id}] [LOC_CALC_SKIP] Skipping location calculation for person (box: {[round(c,1) for c in box]}). UWB_X: {base_uwb_x}, UWB_Y: {base_uwb_y}, Depth: {depth_value}")
                            else: # depth_map_full_resolution is None (업샘플링 실패 시)
                                logger.warning(f"[{camera_id}] Depth map upsampling failed. Cannot calculate person locations.")
                        else: # depth_map_hw is None (UniDepth 추론 실패 또는 모델 미로드)
                            logger.warning(f"[{camera_id}] UniDepth estimation failed or model not loaded. Cannot calculate person locations.")

                        # --- 최종 출력 메시지 생성 ---
                        inference_output = {
                            "camera_id": camera_id,
                            "image_timestamp_utc": original_timestamp_str,
                            "timestamp_inference_utc": datetime.now(timezone.utc).isoformat(),
                            "uwb_data_received": uwb_data_from_wrapper, 
                            "detections_yolo": detections, 
                            "person_locations_estimated": person_locations 
                        }
                        
                        log_summary_keys = ["camera_id", "image_timestamp_utc", "detections_yolo", "person_locations_estimated"]
                        log_output_summary = {
                            k: (f"{len(v)} items" if isinstance(v, list) else v) 
                            for k, v in inference_output.items() if k in log_summary_keys
                        }
                        log_output_summary["processing_time_ms"] = round((time.monotonic() - current_process_start_time) * 1000, 2)
                        logger.info(f"[{camera_id}] Inference Complete. Output Summary: {json.dumps(log_output_summary)}")

                        log_output_summary["yolo_total_detections"] = len(detections)
                        log_output_summary["yolo_persons_detected"] = num_persons_detected_yolo
                        log_output_summary["persons_located_final"] = num_persons_located
                        log_output_summary["unidepth_map_shape_raw"] = depth_map_hw.shape if depth_map_hw is not None else "N/A"

                        log_output_summary["processing_time_ms"] = round((time.monotonic() - current_process_start_time) * 1000, 2)
                        logger.info(f"[{camera_id}] Inference Complete. Output Summary: {json.dumps(log_output_summary)}")


                        if kafka_producer_instance:
                            try:
                                future = kafka_producer_instance.send(OUTPUT_TOPIC_INFERENCE, value=inference_output)
                                future.get(timeout=5) 
                                logger.debug(f"[{camera_id}] Successfully sent inference result to Kafka topic '{OUTPUT_TOPIC_INFERENCE}'.")
                            except KafkaError as ke_prod:
                                logger.error(f"[{camera_id}] Failed to send message to Kafka topic '{OUTPUT_TOPIC_INFERENCE}': {ke_prod}", exc_info=True)
                            except Exception as e_send:
                                logger.error(f"[{camera_id}] An unexpected error occurred while sending message to Kafka: {e_send}", exc_info=True)
                        else:
                            logger.warning(f"[{camera_id}] Kafka producer is not available. Skipping message sending for output topic {OUTPUT_TOPIC_INFERENCE}.")



                    except Exception as e_proc:
                        logger.error(f"Error processing message for camera_id '{camera_id if 'camera_id' in locals() else 'unknown'}': {e_proc}", exc_info=True)
                
                # consumer_timeout_ms가 만료되면 for 루프가 자연스럽게 종료되고,
                # while 루프의 다음 반복으로 넘어감 (stop_event 체크)
                # 여기서 별도의 time.sleep은 필요 없음.

            except KafkaError as ke: # Kafka 컨슈머 자체의 오류 (예: 연결 문제)
                logger.error(f"KafkaError in consumer polling loop: {ke}", exc_info=True)
                if not stop_event.is_set():
                    time.sleep(5) # 잠시 대기 후 재시도
            except Exception as e_loop: # 메시지 폴링/처리 루프의 기타 예외
                logger.error(f"Unexpected error in consumer polling loop: {e_loop}", exc_info=True)
                if not stop_event.is_set():
                    time.sleep(5)
        # while 루프 종료 (stop_event 설정됨)
    finally: # Main try block에 대한 finally
        if consumer: 
            logger.info("Closing Kafka consumer.")
            consumer.close()
        if kafka_producer_instance:
            logger.info("Flushing and closing Kafka producer.")
            kafka_producer_instance.flush(timeout=10) 
            kafka_producer_instance.close(timeout=10)
    
    logger.info("Message consumption loop_thread stopped.")


# --- 서비스 종료 처리 ---
def shutdown_handler(signum, frame):
    logger.info(f"Signal {signal.Signals(signum).name} received. Initiating graceful shutdown...")
    stop_event.set()

# --- 메인 실행 ---
if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    logger.info("Starting Falcon Inference Service...")
    
    load_yolo_model()
    load_unidepth_model()

    if yolo_model is None and unidepth_session is None:
        logger.fatal("Neither YOLO nor UniDepth models could be loaded. Exiting service.")
        import sys 
        sys.exit(1)
    elif yolo_model is None:
        logger.warning("YOLO model failed to load. Service will run without object detection.")
    elif unidepth_session is None:
        logger.warning("UniDepth model failed to load. Service will run without depth estimation.")

    consumer_thread = threading.Thread(target=consume_messages, name="KafkaConsumerThread", daemon=True)
    consumer_thread.start()

    try:
        while not stop_event.is_set():
            if not consumer_thread.is_alive() and not stop_event.is_set():
                logger.error("KafkaConsumerThread died unexpectedly! Initiating shutdown.")
                stop_event.set() 
                break
            stop_event.wait(timeout=1.0) 
    except KeyboardInterrupt: 
        logger.info("KeyboardInterrupt in main thread. Initiating shutdown...")
        stop_event.set()
    
    logger.info("Waiting for consumer thread to finish...")
    if consumer_thread.is_alive():
        consumer_thread.join(timeout=10) 
        if consumer_thread.is_alive():
            logger.warning("Consumer thread did not stop gracefully.")

    logger.info("Falcon Inference Service stopped.")