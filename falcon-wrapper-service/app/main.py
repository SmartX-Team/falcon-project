# falcon-wrapper-service/app/main.py
import threading
import time
import logging
import signal
from queue import Queue, Empty
import os
import json
import redis # Redis 사용을 위해 추가
import traceback # 상세한 예외 로깅을 위해
import copy # 설정 객체 복사를 위해
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
    from uwb_pg_handler import UWBPostgresHandler, init_uwb_pg_pool, close_uwb_pg_pool
    from frame_processor import FrameProcessor
except ImportError as e:
    logger.fatal(f"Failed to import necessary modules: {e}. Service exiting.", exc_info=True)
    import sys
    sys.exit(1)

# --- Global Variables ---
processing_queue = None
stop_event = threading.Event() # 모든 스레드의 종료를 제어하는 전역 이벤트
active_source_threads = {} # Key: source_id (camera_id), Value: thread object
active_uwb_handlers = {} # Key: source_id (camera_id), Value: handler object
frame_processor_threads = [] # FrameProcessor 스레드 목록

# --- Redis Client ---
redis_client = None

def get_redis_client():
    """ 전역 Redis 클라이언트 인스턴스를 반환하거나 초기화합니다. """
    global redis_client
    if redis_client is None:
        try:
            redis_host = getattr(app_config, 'REDIS_HOST', 'localhost')
            redis_port = int(getattr(app_config, 'REDIS_PORT', 6379))
            redis_db = int(getattr(app_config, 'REDIS_DB_CONFIGS', 0)) # 서비스 설정용 DB
            redis_password = getattr(app_config, 'REDIS_PASSWORD', None)
            
            logger.info(f"Initializing Redis client for service configs: Host={redis_host}, Port={redis_port}, DB={redis_db}")
            redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                password=redis_password,
                socket_connect_timeout=5,
                decode_responses=True # JSON 문자열을 직접 다루기 편하도록
            )
            redis_client.ping()
            logger.info("Successfully connected to Redis for service configs.")
        except redis.exceptions.RedisError as e:
            logger.error(f"Failed to connect to Redis for service configs: {e}", exc_info=True)
            redis_client = None # 연결 실패 시 None으로 유지
        except Exception as e_init:
            logger.error(f"Unexpected error initializing Redis client: {e_init}", exc_info=True)
            redis_client = None
    return redis_client

def load_sources_from_redis() -> dict:
    """
    Redis에서 'service_configs:*' 패턴으로 서비스 설정을 로드하여
    falcon-wrapper가 사용할 소스 정보 딕셔너리로 변환합니다.
    반환 값: { "camera_id1": {config_dict1}, "camera_id2": {config_dict2}, ... }
    """
    r = get_redis_client()
    if not r:
        logger.warning("Redis client not available. Cannot load sources.")
        return {}

    sources = {}
    try:
        config_keys = r.keys(getattr(app_config, 'REDIS_SERVICE_CONFIG_KEY_PATTERN', 'service_configs:*'))
        if not config_keys:
            logger.info("No service_configs found in Redis matching the pattern.")
            return {}

        for key in config_keys:
            try:
                raw_data = r.get(key)
                if raw_data:
                    config_data = json.loads(raw_data)
                    
                    # falcon-wrapper가 사용할 형태로 변환
                    # service_name을 camera_id로 사용
                    camera_id = config_data.get("service_name")
                    if not camera_id:
                        logger.warning(f"Service config from Redis key '{key}' missing 'service_name'. Skipping.")
                        continue

                    # 스트림 정보 추출
                    vis_cam_info = config_data.get("visibility_camera_info", {})
                    stream_protocol = vis_cam_info.get("stream_protocol", "").upper()
                    stream_details = vis_cam_info.get("stream_details", {})
                    
                    source_uri = None
                    kafka_topic = None
                    # Kafka 브로커는 개별 설정 또는 전역 설정 사용
                    kafka_brokers = stream_details.get("kafka_bootstrap_servers", app_config.KAFKA_BOOTSTRAP_SERVERS)


                    if stream_protocol == "RTSP":
                        source_uri = stream_details.get("rtsp_url")
                        if not source_uri:
                            logger.warning(f"RTSP source '{camera_id}' from Redis key '{key}' missing 'rtsp_url'. Skipping.")
                            continue
                    elif stream_protocol == "KAFKA":
                        kafka_topic = stream_details.get("kafka_topic")
                        if not kafka_topic:
                            logger.warning(f"Kafka source '{camera_id}' from Redis key '{key}' missing 'kafka_topic'. Skipping.")
                            continue
                    else:
                        logger.warning(f"Unsupported stream_protocol '{stream_protocol}' for '{camera_id}' from Redis key '{key}'. Skipping.")
                        continue
                    
                    uwb_tag_id = config_data.get("input_uwb_tag_id")
                    # uwb_handler_type이 설정에 없으면 기본값 'postgresql' 또는 'api' 사용
                    uwb_handler_type = config_data.get("uwb_handler_type", getattr(app_config, 'DEFAULT_UWB_HANDLER_TYPE', 'postgresql')).lower()


                    sources[camera_id] = {
                        "camera_id": camera_id,
                        "source_type": stream_protocol, # "RTSP" or "KAFKA"
                        "source_uri": source_uri,       # RTSP URL or None
                        "kafka_topic": kafka_topic,     # Kafka topic or None
                        "kafka_brokers": kafka_brokers, # For Kafka source
                        "uwb_tag_id": uwb_tag_id,
                        "uwb_handler_type": uwb_handler_type,
                        "original_config_key": key # 디버깅 및 추적용
                    }
                    logger.debug(f"Loaded source config for '{camera_id}' from Redis key '{key}'.")

            except json.JSONDecodeError:
                logger.error(f"Failed to decode JSON from Redis key '{key}'. Skipping.", exc_info=True)
            except Exception as e_parse:
                logger.error(f"Error processing config from Redis key '{key}': {e_parse}", exc_info=True)
        
        logger.info(f"Successfully loaded {len(sources)} source configurations from Redis.")
        return sources

    except redis.exceptions.RedisError as e_redis:
        logger.error(f"Redis error while loading sources: {e_redis}", exc_info=True)
        return {} # 오류 발생 시 빈 딕셔너리 반환
    except Exception as e_general:
        logger.error(f"Unexpected error loading sources from Redis: {e_general}", exc_info=True)
        return {}

def manage_sources(current_sources_config: dict):
    """
    현재 활성 스레드/핸들러와 Redis에서 가져온 최신 소스 설정을 비교하여
    필요한 스레드/핸들러를 시작하거나 중지합니다.
    """
    global active_source_threads, active_uwb_handlers, processing_queue

    logger.info(f"Managing sources. Current config has {len(current_sources_config)} sources. "
                f"Active threads: {len(active_source_threads)}, Active UWB handlers: {len(active_uwb_handlers)}")

    latest_source_ids = set(current_sources_config.keys())
    current_active_ids = set(active_source_threads.keys())

    # 1. 중지할 소스 (현재 활성이지만 최신 설정에는 없음)
    ids_to_stop = current_active_ids - latest_source_ids
    for source_id in ids_to_stop:
        logger.info(f"Source '{source_id}' no longer in Redis config. Stopping associated thread and UWB handler.")
        if source_id in active_source_threads:
            thread_to_stop = active_source_threads.pop(source_id)
            if hasattr(thread_to_stop, 'stop') and callable(thread_to_stop.stop):
                try:
                    thread_to_stop.stop()
                    thread_to_stop.join(timeout=5.0) # 종료 대기
                    if thread_to_stop.is_alive():
                        logger.warning(f"Thread for source '{source_id}' did not stop gracefully.")
                except Exception as e_stop:
                    logger.error(f"Error stopping thread for source '{source_id}': {e_stop}", exc_info=True)
        
        if source_id in active_uwb_handlers:
            handler_to_remove = active_uwb_handlers.pop(source_id)
            if hasattr(handler_to_remove, 'shutdown') and callable(handler_to_remove.shutdown):
                try:
                    handler_to_remove.shutdown()
                except Exception as e_shutdown_uwb:
                    logger.error(f"Error shutting down UWB handler for '{source_id}': {e_shutdown_uwb}", exc_info=True)

    # 2. 시작/업데이트할 소스
    for source_id, config in current_sources_config.items():
        if source_id not in current_active_ids: # 새로운 소스
            logger.info(f"New source '{source_id}' detected. Starting associated thread and UWB handler.")
            
            # UWB 핸들러 생성
            uwb_handler = None
            if config.get("uwb_tag_id"):
                if config["uwb_handler_type"] == "postgresql":
                    if pg_connection_pool_uwb: # PostgreSQL UWB 풀이 초기화되었는지 확인
                        uwb_handler = UWBPostgresHandler(camera_id=source_id, tag_id_for_camera=config["uwb_tag_id"])
                    else:
                        logger.error(f"PostgreSQL UWB pool not available. Cannot create UWBPostgresHandler for '{source_id}'.")
                elif config["uwb_handler_type"] == "api":
                    uwb_handler = APIUWBHandler(camera_id=source_id, tag_id_for_camera=config["uwb_tag_id"])
                else:
                    logger.warning(f"Unknown UWB handler type '{config['uwb_handler_type']}' for '{source_id}'.")
            
            if uwb_handler:
                active_uwb_handlers[source_id] = uwb_handler

            # Reader 스레드 생성
            new_thread = None
            if config["source_type"] == "RTSP" and config["source_uri"]:
                new_thread = RTSPStreamSubscriber(
                    camera_id=source_id,
                    rtsp_url=config["source_uri"],
                    processing_queue=processing_queue
                )
            elif config["source_type"] == "KAFKA" and config["kafka_topic"]:
                new_thread = KafkaImageSubscriber(
                    camera_id=source_id,
                    topic=config["kafka_topic"],
                    bootstrap_servers=config["kafka_brokers"], # 개별 또는 전역 Kafka 서버 사용
                    processing_queue=processing_queue,
                    consumer_timeout_ms=getattr(app_config, 'KAFKA_CONSUMER_TIMEOUT_MS', 1000)
                )
            
            if new_thread:
                try:
                    new_thread.start()
                    active_source_threads[source_id] = new_thread
                except Exception as e_start:
                    logger.error(f"Failed to start thread for new source '{source_id}': {e_start}", exc_info=True)
                    if source_id in active_uwb_handlers: # 스레드 시작 실패 시 핸들러도 제거
                        active_uwb_handlers.pop(source_id)
            else:
                logger.warning(f"Could not create reader thread for source '{source_id}' due to missing info or unsupported type.")

        else: # 기존 소스 (설정 변경 확인 - 단순화를 위해 여기서는 재시작 안함, 필요시 구현)
            logger.debug(f"Source '{source_id}' already active. Configuration update handling (if any) can be added here.")
            # TODO: 소스 설정(URI, topic 등)이 변경되었는지 확인하고, 변경되었다면 기존 스레드 중지 후 새 스레드 시작 로직 추가

    # FrameProcessor에 업데이트된 UWB 핸들러 맵 전달 (FrameProcessor가 이를 동적으로 사용하도록 수정 필요)
    # 또는 FrameProcessor는 시작 시 한 번만 핸들러 맵을 받음. 이 경우, FrameProcessor 재시작 또는
    # FrameProcessor 내부에서 active_uwb_handlers를 직접 참조하도록 변경 필요.
    # 여기서는 FrameProcessor가 시작 시 받은 맵을 계속 사용한다고 가정.
    # 더 나은 방법은 FrameProcessor가 get_uwb_handler(camera_id) 같은 함수를 통해 동적으로 조회하는 것.
    # 이 예제에서는 FrameProcessor가 시작 시 전달받은 uwb_handler_map을 사용한다고 가정하고,
    # manage_sources가 active_uwb_handlers를 업데이트하면, FrameProcessor가 이를 참조하도록 해야 함.
    # 가장 간단한 방법은 FrameProcessor 생성 시 active_uwb_handlers 자체를 넘기는 것.

def source_management_loop():
    """ 주기적으로 Redis에서 소스 설정을 로드하고 관리합니다. """
    polling_interval = int(getattr(app_config, 'REDIS_POLLING_INTERVAL_SEC', 60))
    logger.info(f"Source management loop_thread started. Polling Redis every {polling_interval} seconds.")
    
    # FrameProcessor에 전달할 UWB 핸들러 맵 (초기에는 비어있음)
    # 이 맵은 manage_sources 함수에 의해 업데이트됨.
    # FrameProcessor는 이 공유 객체를 참조해야 함.
    global active_uwb_handlers 

    # FrameProcessor 스레드 시작 (UWB 핸들러 맵을 전달)
    # FrameProcessor는 active_uwb_handlers 딕셔너리를 직접 참조하도록 수정하는 것이 좋음.
    # 또는, FrameProcessor가 UWB 핸들러를 요청할 때마다 이 맵에서 조회하도록.
    global frame_processor_threads, processing_queue
    num_fp_workers = getattr(app_config, 'FRAME_PROCESSOR_WORKERS', 1)
    if num_fp_workers > 0 and not frame_processor_threads: # 한 번만 시작
        for i in range(num_fp_workers):
            try:
                fp_thread = FrameProcessor(
                    processing_queue=processing_queue,
                    app_config=app_config,
                    # FrameProcessor가 이 active_uwb_handlers 딕셔너리를 직접 참조하도록 수정 필요
                    uwb_handler_map=active_uwb_handlers
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
            latest_sources = load_sources_from_redis()
            manage_sources(latest_sources)
        except Exception as e_poll:
            logger.error(f"Error in source management polling loop: {e_poll}", exc_info=True)
        
        # 폴링 간격만큼 대기 (stop_event가 설정되면 즉시 종료)
        stop_event.wait(timeout=polling_interval)
    
    logger.info("Source management loop_thread stopped.")


def shutdown_service(signum=None, frame=None):
    """ 서비스의 모든 스레드를 정상적으로 종료합니다. """
    global stop_event, active_source_threads, frame_processor_threads, processing_queue

    if signum:
        logger.info(f"Signal {signal.Signals(signum).name} received. Initiating graceful shutdown...")
    else:
        logger.info("Initiating graceful shutdown...")

    if stop_event.is_set():
        logger.info("Shutdown already in progress.")
        return
    stop_event.set() # 모든 루프 및 스레드에 종료 신호 전파

    # 1. Source Reader 스레드 중지
    logger.info("Stopping all source reader threads...")
    for source_id, thread in list(active_source_threads.items()): # 복사본으로 반복
        if hasattr(thread, 'stop') and callable(thread.stop):
            try:
                thread.stop()
            except Exception as e:
                logger.error(f"Error calling stop() on reader thread for '{source_id}': {e}", exc_info=True)
    
    # Reader 스레드가 메시지 발행을 중단할 시간을 줌
    time.sleep(max(1.0, getattr(app_config, 'KAFKA_CONSUMER_TIMEOUT_MS', 1000) / 1000.0 + 0.5))


    # 2. Frame Processor 스레드에 종료 신호 전달 (큐에 None 삽입)
    logger.info("Sending shutdown signal (None) to FrameProcessor threads via queue...")
    if processing_queue:
        num_fp_workers = len(frame_processor_threads) # 실제 실행 중인 FP 스레드 수 기준
        for _ in range(num_fp_workers):
            try:
                processing_queue.put_nowait(None)
            except Full: # 큐가 가득 찼을 경우 (이론상 발생 안해야 함)
                logger.warning("Processing queue is full while trying to send shutdown signal.")
            except Exception as e_q_put:
                logger.warning(f"Could not put shutdown signal to queue: {e_q_put}")
    
    # 3. 모든 스레드 (Reader + Processor) join
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

    # 4. UWB 핸들러 개별 종료 (필요시) - PostgreSQL 풀은 atexit으로 관리
    # for handler in active_uwb_handlers.values():
    # if hasattr(handler, 'shutdown'): handler.shutdown()
    active_uwb_handlers.clear()
    
    # PostgreSQL UWB 연결 풀 닫기 (atexit으로도 등록되어 있지만, 명시적 호출도 가능)
    # close_uwb_pg_pool() # atexit이 처리하므로 중복 호출 피할 수 있음

    logger.info("Falcon Wrapper Service shutdown sequence complete.")

def main():
    global processing_queue
    
    logger.info(f"Starting Falcon Wrapper Service (Instance ID: {getattr(app_config, 'WRAPPER_INSTANCE_ID', 'N/A')})...")

    # PostgreSQL UWB 연결 풀 초기화 (필수, source_management_loop 시작 전)
    init_uwb_pg_pool()
    # 프로그램 종료 시 PostgreSQL 풀 자동 닫기 등록
    atexit.register(close_uwb_pg_pool)
    # Redis 클라이언트 초기화 (필수, source_management_loop 시작 전)
    if not get_redis_client():
        logger.fatal("Failed to initialize Redis client. Service cannot start dynamically loading configs. Exiting.")
        return # Redis 연결 실패 시 서비스 시작 불가

    # 처리 큐 생성
    queue_max_size = getattr(app_config, 'PROCESSING_QUEUE_MAX_SIZE', 100)
    processing_queue = Queue(maxsize=queue_max_size)

    # 소스 관리 루프를 별도 스레드로 시작
    source_manager_thread = threading.Thread(target=source_management_loop, name="SourceManagerThread", daemon=True)
    source_manager_thread.start()

    # 메인 스레드는 시그널 대기 또는 다른 작업 수행 가능
    # 여기서는 stop_event가 설정될 때까지 대기 (실질적으로 source_manager_thread가 종료를 제어)
    try:
        while not stop_event.is_set():
            # 주기적으로 메인 스레드 상태 확인 또는 다른 작업
            # 예: source_manager_thread가 살아있는지 확인
            if not source_manager_thread.is_alive() and not stop_event.is_set():
                logger.error("SourceManagerThread died unexpectedly! Initiating shutdown.")
                # shutdown_service() # 여기서 직접 호출하면 이중 호출될 수 있음. stop_event 설정으로 충분.
                stop_event.set() # SourceManagerThread가 죽었으므로 서비스 종료
                break
            stop_event.wait(timeout=10.0) # 10초마다 깨어나서 확인
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received in main. Initiating shutdown...")
        # shutdown_service()는 시그널 핸들러에 의해 호출됨
    finally:
        if not stop_event.is_set(): # KeyboardInterrupt 외의 이유로 루프 종료 시
            logger.info("Main loop exited. Initiating shutdown...")
        shutdown_service() # 어떤 이유로든 메인 루프가 끝나면 항상 종료 시퀀스 실행

    logger.info("Falcon Wrapper Service main function finished.")

if __name__ == '__main__':
    signal.signal(signal.SIGINT, shutdown_service)
    signal.signal(signal.SIGTERM, shutdown_service)
    main()