
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

X_CORRECTION_LEFT_THRESHOLD = float(os.environ.get('X_CORRECTION_LEFT_THRESHOLD', 170.0))
X_CORRECTION_RIGHT_THRESHOLD = float(os.environ.get('X_CORRECTION_RIGHT_THRESHOLD', 470.0))
X_CORRECTION_OFFSET = float(os.environ.get('X_CORRECTION_OFFSET', 1.0)) 
DEPTH_OFFSET_FACTOR = float(os.environ.get('DEPTH_OFFSET_FACTOR', -1.0)) 

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


# --- 모델 로딩 함수 --- (이전과 동일)
def load_yolo_model():
    global yolo_model
    try:
        model = YOLO(YOLO_MODEL_PATH) 
        if torch.cuda.is_available():
            logger.info("YOLO model will attempt to use GPU.")
        else:
            logger.info("YOLO model will use CPU.")
        yolo_model = model
        logger.info(f"YOLOv5 model loaded successfully from {YOLO_MODEL_PATH}")
    except Exception as e:
        logger.error(f"Error loading YOLOv5 model: {e}", exc_info=True)
        yolo_model = None

def load_unidepth_model():
    global unidepth_session
    try:
        sess_options = ort.SessionOptions()
        unidepth_session = ort.InferenceSession(UNIDEPTH_MODEL_PATH, sess_options=sess_options, providers=ORT_PROVIDERS)
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
            for box in results[0].boxes:
                xyxy = box.xyxy[0].cpu().numpy().tolist()
                conf = float(box.conf[0].cpu().numpy())
                cls_id = int(box.cls[0].cpu().numpy())
                cls_name = yolo_model.names[cls_id]
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
        depth_map_onnx = unidepth_session.run(None, {input_name: image_np_nchw})[0]
        depth_map_hw = np.squeeze(depth_map_onnx)
        return depth_map_hw
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

                        if not camera_id or not image_base64 or not original_timestamp_str:
                            logger.warning(f"Skipping message, missing required fields (camera_id, image_data_b64, image_timestamp_utc):")
                            continue

                        img_bytes = base64.b64decode(image_base64)
                        img_np_bgr = cv2.imdecode(np.frombuffer(img_bytes, np.uint8), cv2.IMREAD_COLOR)
                        if img_np_bgr is None:
                            logger.warning(f"Failed to decode image for camera_id '{camera_id}'. Skipping.")
                            continue
                        
                        # ... (YOLO, UniDepth 추론 및 위치 계산 로직 - 이전과 동일하게 유지)
                        yolo_input_img = preprocess_image_for_yolo(img_np_bgr)
                        detections = run_yolo_inference(yolo_input_img)
                        logger.info(f"[{camera_id}] YOLO detected {len(detections)} objects.")

                        unidepth_input_img_nchw = preprocess_image_for_unidepth(img_np_bgr)
                        depth_map_hw = run_unidepth_inference(unidepth_input_img_nchw)
                        
                        person_locations = [] 

                        if depth_map_hw is not None:
                            logger.info(f"[{camera_id}] UniDepth estimation successful. Depth map shape: {depth_map_hw.shape}")
                            
                            base_uwb_x, base_uwb_y, base_uwb_tag_id = None, None, None
                            if isinstance(uwb_data_from_wrapper, dict):
                                base_uwb_x = uwb_data_from_wrapper.get('x_m') 
                                base_uwb_y = uwb_data_from_wrapper.get('y_m')
                                base_uwb_tag_id = uwb_data_from_wrapper.get('tag_id') 
                                if base_uwb_x is None or base_uwb_y is None:
                                    logger.warning(f"[{camera_id}] UWB data from wrapper is missing 'x_m' or 'y_m'. Location calculation might be inaccurate. UWB data: {uwb_data_from_wrapper}")
                            else:
                                logger.warning(f"[{camera_id}] UWB data from wrapper is not a dictionary or is missing. Location calculation will be skipped or use defaults. UWB data: {uwb_data_from_wrapper}")

                            for detection in detections:
                                if detection['class_name'] == 'person': 
                                    box = detection['box_xyxy']
                                    xmin, ymin, xmax, ymax = box[0], box[1], box[2], box[3]
                                    cx = (xmin + xmax) / 2
                                    cy = (ymin + ymax) / 2

                                    depth_h, depth_w = depth_map_hw.shape
                                    if 0 <= int(cy) < depth_h and 0 <= int(cx) < depth_w:
                                        depth_value = depth_map_hw[int(cy), int(cx)] + DEPTH_OFFSET_FACTOR 
                                    else:
                                        logger.warning(f"[{camera_id}] Person center ({cx},{cy}) out of depth map bounds ({depth_w},{depth_h}). Skipping depth for this person.")
                                        depth_value = None 

                                    calculated_x, calculated_y = None, None
                                    if base_uwb_x is not None and base_uwb_y is not None and depth_value is not None:
                                        calculated_x = float(base_uwb_x)
                                        if cx < X_CORRECTION_LEFT_THRESHOLD:
                                            calculated_x -= X_CORRECTION_OFFSET
                                        elif cx > X_CORRECTION_RIGHT_THRESHOLD:
                                            calculated_x += X_CORRECTION_OFFSET
                                        
                                        calculated_y = float(base_uwb_y) + float(depth_value)
                                        
                                        person_locations.append({
                                            "person_id": f"person_{len(person_locations)+1}", 
                                            "box_xyxy": box,
                                            "confidence": detection['confidence'],
                                            "center_image_coord": (round(cx,2), round(cy,2)),
                                            "depth_value_at_center": round(depth_value,3) if depth_value is not None else None,
                                            "estimated_world_x": round(calculated_x,3) if calculated_x is not None else None,
                                            "estimated_world_y": round(calculated_y,3) if calculated_y is not None else None,
                                            "base_uwb_used": {"tag_id": base_uwb_tag_id, "x": base_uwb_x, "y": base_uwb_y} if base_uwb_x is not None else None
                                        })
                                    else:
                                         logger.warning(f"[{camera_id}] Skipping location calculation for a person due to missing UWB base or depth. UWB: {base_uwb_x},{base_uwb_y} Depth: {depth_value}")
                        else:
                            logger.warning(f"[{camera_id}] UniDepth estimation failed or model not loaded. Cannot calculate person locations.")

                        inference_output = {
                            "camera_id": camera_id,
                            "timestamp_camera_utc": original_timestamp_str,
                            "timestamp_inference_utc": datetime.now(timezone.utc).isoformat(),
                            "uwb_data_received": uwb_data_from_wrapper, 
                            "detections_yolo": detections, 
                            "person_locations_estimated": person_locations 
                        }
                        log_output_summary = {k: (v if not isinstance(v, list) or len(v) < 3 else f"{len(v)} items") for k,v in inference_output.items()}
                        logger.info(f"[{camera_id}] Inference Complete. Output Summary: {json.dumps(log_output_summary)}")

                        if kafka_producer_instance:
                            try:
                                kafka_producer_instance.send(OUTPUT_TOPIC_INFERENCE, value=inference_output)
                                logger.debug(f"[{camera_id}] Sent inference results to Kafka topic '{OUTPUT_TOPIC_INFERENCE}'.")
                            except KafkaError as ke:
                                logger.error(f"[{camera_id}] Failed to send inference results to Kafka: {ke}", exc_info=True)
                            except Exception as e_prod:
                                logger.error(f"[{camera_id}] Unexpected error sending to Kafka: {e_prod}", exc_info=True)

                    except Exception as e_proc: # 개별 메시지 처리 중 오류 발생 시
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


# --- 서비스 종료 처리 --- (이전과 동일)
def shutdown_handler(signum, frame):
    logger.info(f"Signal {signal.Signals(signum).name} received. Initiating graceful shutdown...")
    stop_event.set()

# --- 메인 실행 --- (이전과 동일, sys 임포트 확인)
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