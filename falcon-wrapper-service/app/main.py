# falcon-wrapper-service/app/main.py
import threading
import time
import logging
import signal
from queue import Queue
import os

log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
numeric_level = getattr(logging, log_level_str, logging.INFO)
logging.basicConfig(
    level=numeric_level, 
    format='%(asctime)s - %(levelname)s - %(name)s - [%(threadName)s] - %(message)s'
)
logger = logging.getLogger(__name__)

from .config import app_config 
from .rtsp_reader import RTSPStreamSubscriber
from .kafka_image_reader import KafkaImageSubscriber
from .uwb_handler import UWBHandler
from .frame_processor import FrameProcessor


active_threads = [] 
processing_queue = None 
stop_main_loop = threading.Event()

def shutdown_service(signum=None, frame=None):
    if signum:
        logger.info(f"Signal {signal.Signals(signum).name} received. Initiating graceful shutdown of Wrapper Service...")
    else:
        logger.info("Initiating graceful shutdown of Wrapper Service...")
    
    stop_main_loop.set()

    logger.info("Requesting all reader threads to stop...")
    for t in active_threads:
        if isinstance(t, (RTSPStreamSubscriber, KafkaImageSubscriber)):
            if hasattr(t, 'stop') and callable(t.stop):
                t.stop() 
    
    time.sleep(max(1.0, app_config.KAFKA_CONSUMER_TIMEOUT_MS / 1000.0 + 0.5))

    logger.info("Sending shutdown signal (None) to frame processor threads via queue...")
    if processing_queue and hasattr(app_config, 'FRAME_PROCESSOR_WORKERS'):
        for _ in range(app_config.FRAME_PROCESSOR_WORKERS):
            try:
                processing_queue.put_nowait(None) 
            except Exception as e_q_put:
                logger.warning(f"Could not put shutdown signal (None) to queue: {e_q_put}")
    
    logger.info("Waiting for all threads to complete (max 15s each)...")
    threads_to_join = list(active_threads) 
    active_threads.clear() 

    for t in threads_to_join:
        t.join(timeout=15) 
        if t.is_alive():
            logger.warning(f"Thread {t.name} did not terminate gracefully after 15s.")
        else:
            logger.info(f"Thread {t.name} terminated.")
            
    # UWB 핸들러 종료 (필요시)
    # for handler in uwb_handler_map.values():
    # if hasattr(handler, 'shutdown') and callable(handler.shutdown):
    # handler.shutdown()
            
    logger.info("Wrapper Service shutdown sequence complete.")


def run_wrapper_service():
    global active_threads, processing_queue 

    logger.info(f"Wrapper Service (Instance ID: {app_config.WRAPPER_INSTANCE_ID}) starting...")

    processing_queue = Queue(maxsize=app_config.PROCESSING_QUEUE_MAX_SIZE)

    uwb_handler_map = {}
    for cam_id, individual_uwb_config_dict in app_config.uwb_configs.items():
        # individual_uwb_config_dict 예시: {"type": "api", "api_url_template": "http://gateway/api/tags/{tag_id}", "tag_id": "actual_tag_19"}
        # 또는 단순히 tag_id만 있는 경우: {"tag_id": "actual_tag_19"} (이 경우 UWBHandler가 기본 URL 사용)
        try:
            # UWBHandler 생성 시, AppConfig의 UWB_GATEWAY_URL과 UWB_API_KEY를 내부적으로 사용하고,
            # 여기서는 각 카메라에 특화된 설정(예: tag_id)만 전달
            specific_config_for_handler = {
                "type": individual_uwb_config_dict.get("type", "api"), # 기본 타입 'api'
                "api_url_template": individual_uwb_config_dict.get("api_url_template", f"{app_config.UWB_GATEWAY_URL}/sensmapserver/api/tags/{{tag_id}}"),
                "tag_id": individual_uwb_config_dict.get("tag_id") # 이 카메라에 대한 실제 UWB 태그 ID
            }
            if not specific_config_for_handler["tag_id"]:
                logger.warning(f"No 'tag_id' found in UWB_CONFIGS_JSON for camera_id '{cam_id}'. UWB data will not be fetched for this camera.")
                uwb_handler_map[cam_id] = UWBHandler(cam_id, {"type": "dummy"}) # 더미 핸들러
            else:
                uwb_handler_map[cam_id] = UWBHandler(cam_id, specific_config_for_handler)

        except Exception as e:
            logger.error(f"Failed to initialize UWBHandler for camera_id '{cam_id}' with config {individual_uwb_config_dict}: {e}", exc_info=True)


    processor_threads_list = []
    for i in range(app_config.FRAME_PROCESSOR_WORKERS):
        try:
            processor_thread = FrameProcessor(
                processing_queue=processing_queue,
                app_config=app_config,
                uwb_handler_map=uwb_handler_map
            )
            processor_threads_list.append(processor_thread)
            processor_thread.start()
        except Exception as e:
            logger.fatal(f"Failed to start FrameProcessor thread {i+1}: {e}. Service might not function correctly.", exc_info=True)
    active_threads.extend(processor_threads_list)


    reader_threads_list = []
    for cam_id, rtsp_url_val in app_config.rtsp_sources.items():
        try:
            rtsp_thread = RTSPStreamSubscriber(cam_id, rtsp_url_val, processing_queue) 
            reader_threads_list.append(rtsp_thread)
            rtsp_thread.start()
        except Exception as e:
            logger.error(f"Failed to start RTSPStreamSubscriber for camera_id '{cam_id}' ({rtsp_url_val}): {e}", exc_info=True)

    for cam_id, kafka_topic_val in app_config.kafka_image_sources.items():
        try:
            kafka_reader_thread = KafkaImageSubscriber(
                cam_id, 
                kafka_topic_val, 
                app_config.KAFKA_BOOTSTRAP_SERVERS, 
                processing_queue,
                app_config.KAFKA_CONSUMER_TIMEOUT_MS
            )
            reader_threads_list.append(kafka_reader_thread)
            kafka_reader_thread.start()
        except Exception as e:
            logger.error(f"Failed to start KafkaImageSubscriber for camera_id '{cam_id}' ({kafka_topic_val}): {e}", exc_info=True)
    active_threads.extend(reader_threads_list)


    if not reader_threads_list:
        logger.warning("No data reader sources (RTSP or Kafka Image Topics) configured.")
        if not processor_threads_list:
             logger.info("No readers and no processors configured. Shutting down service.")
             shutdown_service() # 명시적으로 종료 호출
             return 

    try:
        while not stop_main_loop.is_set():
            if active_threads:
                alive_count = sum(1 for t in active_threads if t.is_alive())
                logger.debug(f"Main loop: {alive_count} out of {len(active_threads)} worker threads are alive.")
                if alive_count == 0 and not stop_main_loop.is_set(): # 명시적 종료 요청이 없었는데 모든 스레드가 죽음
                    logger.error("All worker threads seem to have stopped unexpectedly! Initiating shutdown.")
                    stop_main_loop.set() 
            elif not reader_threads_list and not processor_threads_list: # 시작된 스레드가 아예 없었으면
                logger.info("No active threads configured or started. Shutting down.")
                stop_main_loop.set()
            stop_main_loop.wait(timeout=10.0)
    except KeyboardInterrupt: 
        logger.info("KeyboardInterrupt in main_service_loop. shutdown_handler should manage this.")
    finally:
        if not stop_main_loop.is_set():
            logger.info("Main loop exited without explicit shutdown signal. Initiating cleanup.")
            shutdown_service()
        logger.info("Wrapper Service main_service_loop finished.")


if __name__ == '__main__':
    signal.signal(signal.SIGINT, shutdown_service)
    signal.signal(signal.SIGTERM, shutdown_service)
    run_wrapper_service()