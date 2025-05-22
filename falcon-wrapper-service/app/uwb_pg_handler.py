# falcon-wrapper-service/app/uwb_pg_handler.py
import psycopg2
from psycopg2 import pool
from psycopg2.extras import DictCursor
import logging
import datetime # Python의 datetime 사용
from typing import Optional, Dict, Any

# Falcon Wrapper 서비스의 config 모듈 임포트
# from .config import app_config # 또는 실제 app_config 객체를 가져오는 방식에 맞게
from config import app_config # main.py에서 사용된 방식과 동일하게 가정

logger = logging.getLogger(__name__)

pg_connection_pool_uwb = None

def init_uwb_pg_pool():
    """PostgreSQL 연결 풀을 UWB 데이터베이스용으로 초기화합니다."""
    global pg_connection_pool_uwb
    if pg_connection_pool_uwb:
        logger.debug("UWB PostgreSQL connection pool already initialized.")
        return

    # app_config에 UWB용 PostgreSQL 설정이 있는지 확인
    required_pg_vars = [
        'POSTGRES_HOST_UWB', 'POSTGRES_PORT_UWB', 'POSTGRES_DB_UWB',
        'POSTGRES_USER_UWB', 'POSTGRES_PASSWORD_UWB', 'UWB_TABLE_NAME'
    ]
    missing_vars = [var for var in required_pg_vars if not hasattr(app_config, var) or not getattr(app_config, var)]
    if missing_vars:
        logger.error(f"UWB PostgreSQL config missing in app_config: {', '.join(missing_vars)}. Cannot initialize pool.")
        return

    try:
        logger.info(f"Initializing PostgreSQL connection pool for UWB on "
                    f"{app_config.POSTGRES_HOST_UWB}:{app_config.POSTGRES_PORT_UWB}, DB: {app_config.POSTGRES_DB_UWB}")
        pg_connection_pool_uwb = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=getattr(app_config, 'UWB_DB_MAX_CONNECTIONS', 3),
            user=app_config.POSTGRES_USER_UWB,
            password=app_config.POSTGRES_PASSWORD_UWB,
            host=app_config.POSTGRES_HOST_UWB,
            port=int(app_config.POSTGRES_PORT_UWB), # 포트는 정수여야 함
            database=app_config.POSTGRES_DB_UWB
        )
        conn = pg_connection_pool_uwb.getconn()
        logger.info("PostgreSQL connection pool for UWB successfully initialized and tested.")
        pg_connection_pool_uwb.putconn(conn)
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while connecting to UWB PostgreSQL or initializing pool: {error}", exc_info=True)
        pg_connection_pool_uwb = None

def get_uwb_pg_connection():
    """UWB PostgreSQL 연결 풀에서 연결을 가져옵니다."""
    if not pg_connection_pool_uwb:
        logger.warning("UWB PostgreSQL connection pool not initialized. Attempting to initialize now.")
        init_uwb_pg_pool()
    
    if pg_connection_pool_uwb:
        try:
            return pg_connection_pool_uwb.getconn()
        except Exception as e:
            logger.error(f"Failed to get connection from UWB pool: {e}", exc_info=True)
            return None    
    logger.error("UWB PostgreSQL connection pool is not available.")
    return None

def put_uwb_pg_connection(conn, close_conn=False):
    """사용된 PostgreSQL 연결을 풀에 반환하거나 닫습니다."""
    if pg_connection_pool_uwb and conn:
        try:
            pg_connection_pool_uwb.putconn(conn, close=close_conn)
        except Exception as e:
            logger.error(f"Error putting connection back to UWB pool: {e}", exc_info=True)

def close_uwb_pg_pool():
    """UWB PostgreSQL 연결 풀을 닫습니다."""
    global pg_connection_pool_uwb
    if pg_connection_pool_uwb:
        logger.info("Closing UWB PostgreSQL connection pool.")
        pg_connection_pool_uwb.closeall()
        pg_connection_pool_uwb = None


class UWBPostgresHandler:
    def __init__(self, camera_id: str, tag_id_for_camera: Optional[str]):
        self.camera_id = camera_id
        self.tag_id = tag_id_for_camera
        self.uwb_table_name = getattr(app_config, 'UWB_TABLE_NAME', 'uwb_raw') # config에서 테이블 이름 가져오기

        if not pg_connection_pool_uwb: # 핸들러 생성 시 풀 초기화 확인/시도
            init_uwb_pg_pool()

        if self.tag_id:
            logger.info(f"[{self.camera_id}] UWBPostgresHandler initialized for Tag ID '{self.tag_id}' using table '{self.uwb_table_name}'.")
        else:
            logger.info(f"[{self.camera_id}] UWBPostgresHandler initialized. No Tag ID configured. UWB data will not be fetched by this instance.")

    def get_uwb_data(self, frame_timestamp: datetime.datetime, time_tolerance_seconds: int = 2) -> Optional[Dict[str, Any]]:
        """
        주어진 tag_id와 프레임 타임스탬프에 가장 가까운 UWB 데이터를 PostgreSQL에서 조회합니다.
        프레임 타임스탬프와 같거나 그 이전의 가장 최신 데이터를 찾습니다.
        """
        if not self.tag_id:
            return None
        if not pg_connection_pool_uwb:
            logger.error(f"[{self.camera_id}] UWB DB pool not available for tag '{self.tag_id}'.")
            return None

        conn = None
        problematic_conn = False
        try:
            conn = get_uwb_pg_connection()
            if not conn:
                logger.error(f"[{self.camera_id}] Failed to get DB connection for UWB data (tag: {self.tag_id}).")
                return None

            with conn.cursor(cursor_factory=DictCursor) as cur:
                # 컬럼명은 실제 DB 스키마에 맞게 조정 필요 (x_position, y_position, raw_timestamp 등)
                query = f"""
                    SELECT 
                        x_position, 
                        y_position, 
                        z_position, -- 필요하다면 z_position도 포함
                        raw_timestamp::TEXT AS uwb_timestamp 
                    FROM {self.uwb_table_name}
                    WHERE tag_id = %s AND raw_timestamp <= %s
                    ORDER BY raw_timestamp DESC
                    LIMIT 1;
                """
                cur.execute(query, (self.tag_id, frame_timestamp))
                result = cur.fetchone()

            if result:
                uwb_data_db = dict(result)
                
                # UWB 데이터의 타임스탬프와 프레임 타임스탬프 간의 시간차 검증 (선택적)
                # uwb_datetime_obj = datetime.datetime.fromisoformat(uwb_data_db["uwb_timestamp"].replace("Z", "+00:00"))
                # if frame_timestamp - uwb_datetime_obj > datetime.timedelta(seconds=time_tolerance_seconds):
                #     logger.warning(f"[{self.camera_id}] Stale UWB data for tag '{self.tag_id}'. Frame ts: {frame_timestamp}, UWB ts: {uwb_data_db['uwb_timestamp']}.")
                #     return None
                
                logger.debug(f"[{self.camera_id}] UWB data from DB for tag '{self.tag_id}': {uwb_data_db}")
                return {
                    "x_m": uwb_data_db.get("x_position"),
                    "y_m": uwb_data_db.get("y_position"),
                    "z_m": uwb_data_db.get("z_position", 0.0), # z 없으면 0.0 또는 None
                    "timestamp_uwb_utc": uwb_data_db.get("uwb_timestamp"),
                    "quality": None # DB 스키마에 quality 정보가 있다면 추가
                }
            else:
                logger.debug(f"[{self.camera_id}] No suitable UWB data in DB for tag '{self.tag_id}' at/before {frame_timestamp}.")
                return None

        except psycopg2.Error as db_err:
            logger.error(f"[{self.camera_id}] DB error for tag '{self.tag_id}': {db_err}", exc_info=True)
            problematic_conn = True # 이 연결은 문제가 있을 수 있으므로 풀에 반환 시 닫도록 표시
            return None
        except Exception as e:
            logger.error(f"[{self.camera_id}] Unexpected error for tag '{self.tag_id}': {e}", exc_info=True)
            return None
        finally:
            if conn:
                put_uwb_pg_connection(conn, close_conn=problematic_conn)
                
    def shutdown(self):
        logger.info(f"[{self.camera_id}] Shutting down UWBPostgresHandler for tag {self.tag_id or 'N/A'}...")
        # close_uwb_pg_pool() # 풀 종료는 애플리케이션 전체 종료 시 한 번만 호출