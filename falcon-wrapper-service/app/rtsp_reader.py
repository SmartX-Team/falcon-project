# falcon-wrapper-service/app/rtsp_reader.py
import cv2
import threading
import time
import logging
from queue import Queue
from datetime import datetime, timezone
import os

logger = logging.getLogger(__name__)

class RTSPStreamSubscriber(threading.Thread):
    def __init__(self, camera_id: str, rtsp_url: str, processing_queue: Queue, max_fps: int = 0):
        super().__init__(name=f"RTSPReader-{camera_id}")
        self.camera_id = camera_id
        self.rtsp_url = rtsp_url
        self.processing_queue = processing_queue
        self._stop_event = threading.Event()
        self.daemon = True
        self.max_fps = max_fps
        self.min_frame_interval = 1.0 / max_fps if max_fps > 0 else 0.0
        self.last_processed_time = 0.0
        logger.info(f"[{self.camera_id}] RTSPStreamSubscriber initialized for URL: {rtsp_url}. Max FPS: {max_fps if max_fps > 0 else 'Unlimited'}")

    def stop(self):
        logger.info(f"[{self.camera_id}] Stop request received for RTSP stream: {self.rtsp_url}")
        self._stop_event.set()

    def run(self):
        logger.info(f"[{self.camera_id}] Starting RTSP stream capture from: {self.rtsp_url}")
        cap = None
        retry_delay_seconds = 5
        while not self._stop_event.is_set():
            try:
                if cap is None or not cap.isOpened():
                    logger.info(f"[{self.camera_id}] Opening RTSP stream: {self.rtsp_url}...")
                    os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;tcp"
                    cap = cv2.VideoCapture(self.rtsp_url, cv2.CAP_FFMPEG)
                    if not cap.isOpened():
                        logger.error(f"[{self.camera_id}] Failed to open RTSP stream: {self.rtsp_url}. Retrying in {retry_delay_seconds}s...")
                        if self._stop_event.wait(retry_delay_seconds): break
                        continue
                    logger.info(f"[{self.camera_id}] RTSP stream opened successfully: {self.rtsp_url}")

                ret, frame_bgr = cap.read()
                if not ret:
                    logger.warning(f"[{self.camera_id}] Failed to read frame from RTSP stream. Re-opening...")
                    if cap: cap.release()
                    cap = None 
                    if self._stop_event.wait(1): break
                    continue

                current_time = time.monotonic()
                if self.min_frame_interval > 0 and (current_time - self.last_processed_time < self.min_frame_interval):
                    continue 

                frame_timestamp_utc = datetime.now(timezone.utc)
                if self.processing_queue.full():
                    logger.warning(f"[{self.camera_id}] Processing queue is full. Frame from RTSP might be dropped.")
                    try: self.processing_queue.get_nowait()
                    except: pass
                
                self.processing_queue.put((
                    self.camera_id, 
                    frame_bgr, 
                    frame_timestamp_utc, 
                    "RTSP", 
                    {"url": self.rtsp_url}
                ))
                self.last_processed_time = current_time
                logger.debug(f"[{self.camera_id}] Frame read from RTSP (shape: {frame_bgr.shape}) and put to queue.")
            except Exception as e:
                logger.error(f"[{self.camera_id}] Error in RTSP reading loop: {e}", exc_info=True)
                if cap: cap.release()
                cap = None
                if self._stop_event.wait(retry_delay_seconds): break
        if cap: cap.release()
        logger.info(f"[{self.camera_id}] RTSP stream capture stopped for: {self.rtsp_url}")