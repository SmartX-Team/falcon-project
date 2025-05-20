# falcon-wrapper-service/app/uwb_handler.py
import time
import logging
from datetime import datetime, timezone
import requests
from .config import app_config # UWB_GATEWAY_URL, UWB_API_KEY, UWB_API_TIMEOUT_SEC 사용

logger = logging.getLogger(__name__)

class UWBHandler:
    def __init__(self, camera_id: str, tag_id_for_camera: str | None):
        self.camera_id = camera_id
        self.tag_id = tag_id_for_camera
        self.uwb_gateway_url = app_config.UWB_GATEWAY_URL
        self.api_key = app_config.UWB_API_KEY
        self.api_timeout = app_config.UWB_API_TIMEOUT_SEC

        if self.tag_id:
            logger.info(f"[{self.camera_id}] UWBHandler initialized. Will fetch data for Tag ID '{self.tag_id}' from Gateway '{self.uwb_gateway_url}'. API Key {'IS' if self.api_key else 'IS NOT'} configured.")
        else:
            logger.info(f"[{self.camera_id}] UWBHandler initialized. No Tag ID configured for this camera. UWB data will not be fetched.")

    def get_uwb_data(self) -> dict | None:
        if not self.tag_id or not self.uwb_gateway_url:
            return None 

        # API 경로: /sensmapserver/api/tags/{tag_id}
        api_url = f"{self.uwb_gateway_url}/sensmapserver/api/tags/{self.tag_id}"
        
        headers = {}
        if self.api_key:
            headers['X-Apikey'] = self.api_key
        else:
            logger.warning(f"[{self.camera_id}] UWB_API_KEY is not set. Attempting UWB API call without API key.")

        logger.debug(f"[{self.camera_id}] Fetching UWB for tag_id {self.tag_id} from {api_url}")
        try:
            response = requests.get(api_url, headers=headers, timeout=self.api_timeout)
            response.raise_for_status() 
            
            data = response.json()
            
            pos_x_val, pos_y_val, pos_z_val = None, None, None
            pos_timestamp_utc = None # 가장 최신 위치 정보의 타임스탬프를 찾기 위함
            quality = None

            if "datastreams" in data and isinstance(data["datastreams"], list):
                for stream in data["datastreams"]:
                    stream_id = stream.get("id")
                    current_value_str = stream.get("current_value")
                    timestamp_str = stream.get("at") # 예: "2025-05-03T14:18:28.002000Z"

                    if current_value_str is None or timestamp_str is None:
                        continue

                    try:
                        current_value = float(current_value_str)
                        # 타임스탬프 파싱 (Z는 UTC를 의미, fromisoformat은 Python 3.7+ 에서 Z 직접 처리)
                        current_ts_dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                    except ValueError:
                        logger.warning(f"[{self.camera_id}] Could not parse UWB value '{current_value_str}' or timestamp '{timestamp_str}' for stream_id '{stream_id}'")
                        continue
                    
                    if stream_id == "posX":
                        pos_x_val = current_value
                        if pos_timestamp_utc is None or current_ts_dt > pos_timestamp_utc:
                             pos_timestamp_utc = current_ts_dt
                    elif stream_id == "posY":
                        pos_y_val = current_value
                        if pos_timestamp_utc is None or current_ts_dt > pos_timestamp_utc:
                             pos_timestamp_utc = current_ts_dt
                    elif stream_id == "posZ": # Z축 값도 가져옴 (그러나 README: GIST AI Grad Building에서 일정하다고 가정)
                        pos_z_val = current_value 
                        # Z의 타임스탬프도 고려할 수 있으나, 보통 X,Y와 함께 업데이트됨
                    elif stream_id == "numberOfAnchors": # 예시로 품질 지표로 사용
                        try: quality = int(current_value)
                        except: pass


            if pos_x_val is not None and pos_y_val is not None:
                processed_uwb_data = {
                    "x_m": pos_x_val,
                    "y_m": pos_y_val,
                    "z_m": pos_z_val if pos_z_val is not None else 0.0, # Z 없으면 0.0 또는 None
                    "timestamp_uwb_utc": pos_timestamp_utc.isoformat() if pos_timestamp_utc else datetime.now(timezone.utc).isoformat(),
                    "quality": quality
                }
                logger.debug(f"[{self.camera_id}] UWB data parsed for tag {self.tag_id}: {processed_uwb_data}")
                return processed_uwb_data
            else:
                logger.warning(f"[{self.camera_id}] UWB API response for tag {self.tag_id} missing posX or posY in datastreams: {data.get('datastreams')}")
                return None # 필수 값 누락 시 None 반환
        
        except requests.exceptions.Timeout:
            logger.error(f"[{self.camera_id}] UWB API call to {api_url} timed out after {self.api_timeout}s.")
        except requests.exceptions.RequestException as e:
            logger.warning(f"[{self.camera_id}] UWB fetch failed for tag {self.tag_id} (RequestException): {e}")
        except json.JSONDecodeError as e_json:
            logger.warning(f"[{self.camera_id}] UWB API response for tag {self.tag_id} not valid JSON: {e_json}. Response text (first 200 chars): {response.text[:200] if 'response' in locals() and hasattr(response, 'text') else 'N/A'}")
        except Exception as e_other:
            logger.error(f"[{self.camera_id}] Unexpected error fetching or parsing UWB for tag {self.tag_id}: {e_other}", exc_info=True)
        
        return None # 오류 발생 또는 데이터 없음

    def shutdown(self):
        logger.info(f"[{self.camera_id}] Shutting down UWBHandler for tag {self.tag_id or 'N/A'}...")