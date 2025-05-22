# falcon-wrapper-service/app/main.py
import threading
import time
import logging
import signal
from queue import Queue, Empty, Full # Full 추가
import os
import json
import redis 
import traceback 
import copy 
import atexit

# --- Logging Configuration ---
log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
numeric_level = getattr(logging, log_level_str, logging.INFO)
logging.basicConfig(
    level=numeric_level,
    format='%(asctime)s - %(levelname)s - %(name)s - [%(threadName)s] - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Application Imports ---
try:
    from config import app_config
    from rtsp_reader import RTSPStreamSubscriber
    from kafka_image_reader import KafkaImageSubscriber
    from uwb_handler import UWBHandler as APIUWBHandler
    # uwb_pg_handler에서 pg_connection_pool_uwb 변수도 가져오도록 수정 (manage_sources에서 사용)
    from uwb_pg_handler import UWBPostgresHandler, init_uwb_pg_pool, close_uwb_pg_pool, pg_connection_pool_uwb 
    from frame_processor import FrameProcessor
except ImportError as e:
    logger.fatal(f"Failed to import necessary modules: {e}. Service exiting.", exc_info=True)
    import sys
    sys.exit(1)

# --- Global Variables ---
processing_queue = None
stop_event = threading.Event() 
active_source_threads = {} 
active_uwb_handlers = {} 
frame_processor_threads = [] 
uwb_handlers_lock = threading.Lock()

# --- Redis Client ---
redis_client_instance = None # 변수명 변경 (전역 redis_client와 구분)

def get_redis_client():
    global redis_client_instance
    if redis_client_instance is None:
        try:
            redis_host = getattr(app_config, 'REDIS_HOST', 'localhost')
            redis_port = int(getattr(app_config, 'REDIS_PORT', 6379))
            redis_db = int(getattr(app_config, 'REDIS_DB_CONFIGS', 0)) 
            redis_password = getattr(app_config, 'REDIS_PASSWORD', None)
            
            logger.info(f"Initializing Redis client for service configs: Host={redis_host}, Port={redis_port}, DB={redis_db}")
            redis_client_instance = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                password=redis_password,
                socket_connect_timeout=5,
                decode_responses=True 
            )
            redis_client_instance.ping()
            logger.info("Successfully connected to Redis for service configs.")
        except redis.exceptions.RedisError as e:
            logger.error(f"Failed to connect to Redis for service configs: {e}", exc_info=True)
            redis_client_instance = None 
        except Exception as e_init:
            logger.error(f"Unexpected error initializing Redis client: {e_init}", exc_info=True)
            redis_client_instance = None
    return redis_client_instance

def load_sources_from_redis() -> dict:
    """
    Redis의 특정 서비스 키에서 카메라 설정 리스트를 로드하여,
    falcon-wrapper가 사용할 소스 정보 딕셔셔너리로 변환합니다.
    반환 값: { "camera_unique_id1": {config_dict1}, "camera_unique_id2": {config_dict2}, ... }
    여기서 camera_unique_id는 각 카메라 설정 내의 input_camera_id 또는 visibility_camera_info의 ID를 사용합니다.
    """
    r = get_redis_client()
    if not r:
        logger.warning("Redis client not available. Cannot load sources.")
        return {}

    sources = {}
    try:
        # app_config에서 TARGET_SERVICE_NAME을 가져옴 (환경 변수 등으로 설정)
        target_service_name = getattr(app_config, 'TARGET_SERVICE_NAME', None)
        if not target_service_name:
            logger.error("TARGET_SERVICE_NAME is not configured in app_config. Cannot load sources.")
            return {}

        redis_key_prefix = getattr(app_config, 'REDIS_SERVICE_CONFIG_KEY_PREFIX', 'service_configs')
        service_specific_key = f"{redis_key_prefix}:{target_service_name}"
        
        logger.info(f"Loading camera configurations for service '{target_service_name}' from Redis key '{service_specific_key}'.")
        raw_data = r.get(service_specific_key)

        if not raw_data:
            logger.info(f"No configuration data found in Redis for key '{service_specific_key}'.")
            return {}

        # 값은 카메라 설정 객체들의 리스트여야 함
        camera_config_list = json.loads(raw_data)
        if not isinstance(camera_config_list, list):
            logger.error(f"Data for key '{service_specific_key}' is not a list. Found: {type(camera_config_list)}. Skipping.")
            return {}

        if not camera_config_list:
            logger.info(f"Camera configuration list for service '{target_service_name}' is empty.")
            return {}
            
        logger.info(f"Retrieved {len(camera_config_list)} camera configurations for service '{target_service_name}'.")

        for camera_config in camera_config_list:
            if not isinstance(camera_config, dict):
                logger.warning(f"Skipping non-dictionary item in camera config list for service '{target_service_name}': {camera_config}")
                continue

            # 각 카메라 설정을 wrapper가 사용할 형태로 변환
            # 카메라 식별자로 input_camera_id 또는 visibility_camera_info 내의 ID 사용
            camera_unique_id = camera_config.get("input_camera_id")
            vis_cam_info = camera_config.get("visibility_camera_info", {})
            if not camera_unique_id and vis_cam_info:
                camera_unique_id = vis_cam_info.get("camera_id_from_visibility_server")

            if not camera_unique_id:
                logger.warning(f"Camera config item for service '{target_service_name}' is missing a usable camera ID. Skipping: {camera_config}")
                continue

            stream_protocol = vis_cam_info.get("stream_protocol", "").upper()
            stream_details = vis_cam_info.get("stream_details", {})
            
            source_uri = None
            kafka_topic = None
            kafka_brokers = stream_details.get("kafka_bootstrap_servers", app_config.KAFKA_BOOTSTRAP_SERVERS)

            if stream_protocol == "RTSP":
                source_uri = stream_details.get("rtsp_url") # visibility_server는 rtsp_uri 대신 rtsp_url 사용 가능성 있음
                if not source_uri: # fallback
                     source_uri = stream_details.get("rtsp_uri")
                if not source_uri:
                    logger.warning(f"RTSP source '{camera_unique_id}' for service '{target_service_name}' missing 'rtsp_url' or 'rtsp_uri'. Skipping.")
                    continue
            elif stream_protocol == "KAFKA":
                kafka_topic = stream_details.get("kafka_topic")
                if not kafka_topic:
                    logger.warning(f"Kafka source '{camera_unique_id}' for service '{target_service_name}' missing 'kafka_topic'. Skipping.")
                    continue
            else:
                logger.warning(f"Unsupported stream_protocol '{stream_protocol}' for '{camera_unique_id}' in service '{target_service_name}'. Skipping.")
                continue
            
            uwb_tag_id = camera_config.get("input_uwb_tag_id")
            uwb_handler_type = camera_config.get("uwb_handler_type", getattr(app_config, 'DEFAULT_UWB_HANDLER_TYPE', 'postgresql')).lower()

            sources[camera_unique_id] = {
                "camera_id": camera_unique_id, # wrapper 내부에서 사용할 ID
                "source_type": stream_protocol,
                "source_uri": source_uri,
                "kafka_topic": kafka_topic,
                "kafka_brokers": kafka_brokers,
                "uwb_tag_id": uwb_tag_id,
                "uwb_handler_type": uwb_handler_type,
                "original_service_name": target_service_name, # 어떤 서비스에 속한 카메라인지 추적
                "full_config_from_redis": copy.deepcopy(camera_config) # 원본 설정 복사 (디버깅/확장용)
            }
            logger.debug(f"Loaded source config for camera '{camera_unique_id}' (service: '{target_service_name}').")

        logger.info(f"Successfully loaded {len(sources)} individual camera source configurations for service '{target_service_name}'.")
        return sources

    except redis.exceptions.RedisError as e_redis:
        logger.error(f"Redis error while loading sources for service '{getattr(app_config, 'TARGET_SERVICE_NAME', 'N/A')}': {e_redis}", exc_info=True)
        return {}
    except json.JSONDecodeError as e_json:
        logger.error(f"Failed to decode JSON for service '{getattr(app_config, 'TARGET_SERVICE_NAME', 'N/A')}': {e_json}. Raw data: {raw_data[:200] if 'raw_data' in locals() else 'N/A'}", exc_info=True)
        return {}
    except Exception as e_general:
        logger.error(f"Unexpected error loading sources from Redis for service '{getattr(app_config, 'TARGET_SERVICE_NAME', 'N/A')}': {e_general}", exc_info=True)
        return {}

def manage_sources(current_sources_config: dict):
    global active_source_threads, active_uwb_handlers, processing_queue, uwb_handlers_lock
    # uwb_pg_handler.pg_connection_pool_uwb를 직접 사용하거나, 
    # init_uwb_pg_pool()이 설정하는 전역 변수를 정확히 참조해야 합니다.
    # 여기서는 uwb_pg_handler.pg_connection_pool_uwb를 사용한다고 가정 (import 필요)
    # 또는, UWBPostgresHandler 생성자에서 풀 가용성을 내부적으로 확인하도록 수정.
    # 현재 코드는 from uwb_pg_handler import pg_connection_pool_uwb 를 가정하고 있음.

    logger.info(f"Managing sources. Current config has {len(current_sources_config)} sources. "
                f"Active threads: {len(active_source_threads)}, Active UWB handlers: {len(active_uwb_handlers)}")

    latest_source_ids = set(current_sources_config.keys())
    current_active_ids = set(active_source_threads.keys())

    # 1. 중지할 소스
    ids_to_stop = current_active_ids - latest_source_ids
    for source_id in ids_to_stop:
        logger.info(f"Source camera '{source_id}' no longer in current service config. Stopping associated thread and UWB handler.")
        if source_id in active_source_threads:
            thread_to_stop = active_source_threads.pop(source_id)
            if hasattr(thread_to_stop, 'stop') and callable(thread_to_stop.stop):
                try:
                    thread_to_stop.stop()
                    thread_to_stop.join(timeout=5.0)
                    if thread_to_stop.is_alive():
                        logger.warning(f"Thread for source camera '{source_id}' did not stop gracefully.")
                except Exception as e_stop:
                    logger.error(f"Error stopping thread for source camera '{source_id}': {e_stop}", exc_info=True)

        with uwb_handlers_lock:        
            if source_id in active_uwb_handlers:
                handler_to_remove = active_uwb_handlers.pop(source_id)
                if hasattr(handler_to_remove, 'shutdown') and callable(handler_to_remove.shutdown):
                    try:
                        handler_to_remove.shutdown()
                    except Exception as e_shutdown_uwb:
                        logger.error(f"Error shutting down UWB handler for '{source_id}': {e_shutdown_uwb}", exc_info=True)

    # 2. 시작/업데이트할 소스
    for source_id, config_item in current_sources_config.items(): # 변수명 변경 (config -> config_item)
        if source_id not in current_active_ids: 
            logger.info(f"New source camera '{source_id}' detected. Starting associated thread and UWB handler.")
            
            uwb_handler = None
            if config_item.get("uwb_tag_id"):
                if config_item["uwb_handler_type"] == "postgresql":
                    # uwb_pg_handler.pg_connection_pool_uwb 가 초기화되었는지 확인
                    if pg_connection_pool_uwb: 
                        uwb_handler = UWBPostgresHandler(camera_id=source_id, tag_id_for_camera=config_item["uwb_tag_id"])
                    else:
                        logger.error(f"PostgreSQL UWB pool (uwb_pg_handler.pg_connection_pool_uwb) not available. Cannot create UWBPostgresHandler for '{source_id}'.")
                elif config_item["uwb_handler_type"] == "api":
                    uwb_handler = APIUWBHandler(camera_id=source_id, tag_id_for_camera=config_item["uwb_tag_id"])
                else:
                    logger.warning(f"Unknown UWB handler type '{config_item['uwb_handler_type']}' for '{source_id}'.")
            
            if uwb_handler:
                active_uwb_handlers[source_id] = uwb_handler

            new_thread = None
            if config_item["source_type"] == "RTSP" and config_item["source_uri"]:
                new_thread = RTSPStreamSubscriber(
                    camera_id=source_id,
                    rtsp_url=config_item["source_uri"],
                    processing_queue=processing_queue
                )
            elif config_item["source_type"] == "KAFKA" and config_item["kafka_topic"]:
                new_thread = KafkaImageSubscriber(
                    camera_id=source_id,
                    topic=config_item["kafka_topic"],
                    bootstrap_servers=config_item["kafka_brokers"],
                    processing_queue=processing_queue,
                    consumer_timeout_ms=getattr(app_config, 'KAFKA_CONSUMER_TIMEOUT_MS', 1000)
                )
            
            if new_thread:
                try:
                    new_thread.start()
                    active_source_threads[source_id] = new_thread
                except Exception as e_start:
                    logger.error(f"Failed to start thread for new source camera '{source_id}': {e_start}", exc_info=True)
                    if source_id in active_uwb_handlers: 
                        active_uwb_handlers.pop(source_id)
            else:
                logger.warning(f"Could not create reader thread for source camera '{source_id}' due to missing info or unsupported type.")
        else: 
            logger.debug(f"Source camera '{source_id}' already active. Config update handling (if any) can be added here.")
            # TODO: 설정 변경 시 재시작 로직 추가 (예: UWB 태그 변경, 스트림 URI 변경 등)

def source_management_loop():
    polling_interval = int(getattr(app_config, 'REDIS_POLLING_INTERVAL_SEC', 60))
    logger.info(f"Source management loop_thread started. Polling Redis every {polling_interval} seconds.")
    
    global active_uwb_handlers, frame_processor_threads, processing_queue
    
    num_fp_workers = getattr(app_config, 'FRAME_PROCESSOR_WORKERS', 1)
    if num_fp_workers > 0 and not frame_processor_threads: 
        for i in range(num_fp_workers):
            try:
                fp_thread = FrameProcessor(
                    processing_queue=processing_queue,
                    app_config=app_config,
                    uwb_handler_map=active_uwb_handlers, # FrameProcessor가 이 딕셔너리를 직접 참조
                    shared_lock=uwb_handlers_lock
                )
                fp_thread.start()
                frame_processor_threads.append(fp_thread)
            except Exception as e_fp_start:
                logger.fatal(f"Failed to start FrameProcessor thread {i+1}: {e_fp_start}. Service might not function.", exc_info=True)
        
    if not frame_processor_threads and num_fp_workers > 0:
        logger.error("FrameProcessor threads could not be started. Source management loop will run, but no processing will occur.")

    while not stop_event.is_set():
        try:
            logger.debug("Polling Redis for source configurations...")
            latest_sources = load_sources_from_redis() # 이제 service_name에 해당하는 카메라 리스트를 처리
            manage_sources(latest_sources)
        except Exception as e_poll:
            logger.error(f"Error in source management polling loop: {e_poll}", exc_info=True)
        
        stop_event.wait(timeout=polling_interval)
    
    logger.info("Source management loop_thread stopped.")

def shutdown_service(signum=None, frame=None):
    global stop_event, active_source_threads, frame_processor_threads, processing_queue, uwb_handlers_lock

    if signum:
        logger.info(f"Signal {signal.Signals(signum).name} received. Initiating graceful shutdown...")
    else:
        logger.info("Initiating graceful shutdown...")

    if stop_event.is_set():
        logger.info("Shutdown already in progress.")
        return
    stop_event.set() 

    logger.info("Stopping all source reader threads...")
    for source_id, thread in list(active_source_threads.items()): 
        if hasattr(thread, 'stop') and callable(thread.stop):
            try:
                thread.stop()
            except Exception as e:
                logger.error(f"Error calling stop() on reader thread for '{source_id}': {e}", exc_info=True)
    
    time.sleep(max(1.0, getattr(app_config, 'KAFKA_CONSUMER_TIMEOUT_MS', 1000) / 1000.0 + 0.5))

    logger.info("Sending shutdown signal (None) to FrameProcessor threads via queue...")
    if processing_queue:
        num_fp_workers = len(frame_processor_threads) 
        for _ in range(num_fp_workers):
            try:
                processing_queue.put_nowait(None)
            except Full: 
                logger.warning("Processing queue is full while trying to send shutdown signal.")
            except Exception as e_q_put:
                logger.warning(f"Could not put shutdown signal to queue: {e_q_put}")
    
    all_threads_to_join = list(active_source_threads.values()) + frame_processor_threads
    logger.info(f"Waiting for {len(all_threads_to_join)} threads to complete (max 10s each)...")
    
    for thread in all_threads_to_join:
        try:
            thread.join(timeout=10.0)
            if thread.is_alive():
                logger.warning(f"Thread {thread.name} did not terminate gracefully after 10s.")
            else:
                logger.info(f"Thread {thread.name} terminated.")
        except Exception as e_join:
            logger.error(f"Error joining thread {thread.name}: {e_join}", exc_info=True)

    active_source_threads.clear()
    frame_processor_threads.clear()
    with uwb_handlers_lock: # uwb_handlers_lock 사용
        active_uwb_handlers.clear()
    
    logger.info("Falcon Wrapper Service shutdown sequence complete.")

def main():
    global processing_queue, pg_connection_pool_uwb # pg_connection_pool_uwb를 global로 선언
    
    logger.info(f"Starting Falcon Wrapper Service (Instance ID: {getattr(app_config, 'WRAPPER_INSTANCE_ID', 'N/A')})...")

    # TARGET_SERVICE_NAME 설정 확인
    if not getattr(app_config, 'TARGET_SERVICE_NAME', None):
        logger.fatal("TARGET_SERVICE_NAME is not configured. This wrapper service needs to know which service configuration to load from Redis. Exiting.")
        return

    init_uwb_pg_pool() 
    # init_uwb_pg_pool()이 uwb_pg_handler.pg_connection_pool_uwb를 설정한다고 가정
    # 또는, init_uwb_pg_pool()의 반환값을 받아 pg_connection_pool_uwb에 할당할 수 있음.
    # 여기서는 uwb_pg_handler에서 pg_connection_pool_uwb를 import 했다고 가정.
    atexit.register(close_uwb_pg_pool)
    
    if not get_redis_client():
        logger.fatal("Failed to initialize Redis client. Service cannot start dynamically loading configs. Exiting.")
        return 

    queue_max_size = getattr(app_config, 'PROCESSING_QUEUE_MAX_SIZE', 100)
    processing_queue = Queue(maxsize=queue_max_size)

    source_manager_thread = threading.Thread(target=source_management_loop, name="SourceManagerThread", daemon=True)
    source_manager_thread.start()

    try:
        while not stop_event.is_set():
            if not source_manager_thread.is_alive() and not stop_event.is_set():
                logger.error("SourceManagerThread died unexpectedly! Initiating shutdown.")
                stop_event.set() 
                break
            stop_event.wait(timeout=10.0) 
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received in main. Initiating shutdown...")
    finally:
        if not stop_event.is_set(): 
            logger.info("Main loop exited. Initiating shutdown...")
        shutdown_service() 

    logger.info("Falcon Wrapper Service main function finished.")

if __name__ == '__main__':
    signal.signal(signal.SIGINT, shutdown_service)
    signal.signal(signal.SIGTERM, shutdown_service)
    main()
