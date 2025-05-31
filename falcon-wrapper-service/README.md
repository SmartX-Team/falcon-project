# Falcon Wrapper Service (Input Processing and Sensor Fusion)

## 1. Service Overview

`ai-service-visibility` receives data from various real-time video/image sources (RTSP streams, Kafka image topics, etc.) and fuses them with related UWB (Ultra-Wideband) positioning data based on the timestamp of each frame. The processed data is published to a central Kafka topic in standardized JSON format, designed to be utilized by subsequent AI inference services (`falcon-inference`) and digital twin visualization applications.

The core feature of this service is **dynamic source management through Redis**. Due to the nature of Camera Agents in the AI Graduate School where cameras are frequently turned on/off and their states change dynamically, users can dynamically store and modify available/usable camera resources in Redis. The 'wrapper-service' reads currently activated camera sources and related configurations based on Redis. This provides flexibility to add, remove, or change input sources in real-time without service restart.

Data reception and UWB data fusion processing for each active source is performed in independent threads to ensure parallelism and scalability.

Kubernetes deployment-related YAML files and container images are managed in the following repository:
[https://github.com/SmartX-Team/twin-datapond.git](https://github.com/SmartX-Team/twin-datapond.git)

## 2. Key Features

* **Dynamic Multi-Input Source Support**:
    * Dynamically subscribes to and processes RTSP video streams and Kafka image topics based on configurations registered in Redis.
    * When camera source information (stream URLs, Kafka topics, UWB tag IDs, etc.) is registered/updated in Redis through `ai-service-visibility`, `falcon-wrapper-service` detects this and starts or stops data processing for the corresponding source.
    * Designed to respond to the current situation at AI Graduate School where camera sources change dynamically and frequently.

* **UWB Data Fusion**:
    * Synchronizes UWB positioning data at the corresponding time point based on the timestamp of each camera/image frame.
    * UWB data can be retrieved in one of two ways, determined by individual camera settings in Redis:
        1. **PostgreSQL DB Query**: Uses the specified UWB tag ID and frame timestamp to query the PostgreSQL DB for the closest (at that time point or the most recent before it) positioning data.
        2. **REST API Call**: Directly calls the UWB gateway's REST API through WebSocket-based communication with the UWB server to retrieve the latest position information for a specific tag ID. (Periodic calls, planned to be deprecated)

* **Integrated Message Publishing**:
    * Publishes standardized JSON messages containing processed image data (Base64 encoded), UWB data, camera identifier, image timestamp, UWB timestamp, and other metadata to specified Kafka topics.

* **Configuration Flexibility (Defaults and Redis Override)**:
    * Global default settings such as Kafka server address, default UWB handler type, and image processing options are specified through environment variables.
    * Individual camera source stream information, UWB tag IDs to use, UWB handler types, etc., are dynamically loaded from Redis and can override global settings.

* **Parallel Processing**:
    * Each active input source loaded from Redis is processed in separate Reader threads (RTSP or Kafka).
    * Frame processing and Kafka publishing tasks are performed in separate worker thread pools (`FrameProcessor`) to increase throughput.

## 3. Project Structure (app/ directory)

* **`config.py`**:
    * Role: Loads and manages all global settings required for application execution from environment variables.
    * Main content: Kafka broker address, output topic name, Redis connection information, UWB handler (API, PostgreSQL) default settings, image compression quality, processing queue size, number of worker threads, etc.

* **`main.py`**:
    * Role: Main entry point of the service that loads `app_config` and initializes Redis client, PostgreSQL UWB connection pool (if needed).
    * Periodically polls Redis to load service configurations and runs `SourceManagerThread` that dynamically creates, starts, and stops Reader threads and UWB handler instances based on changes.
    * Initializes and runs `FrameProcessor` threads and manages graceful shutdown logic through signal handling.

* **`uwb_handler.py` (API-based - Planned to be deprecated)**:
    * Role: (Legacy version) Calls UWB gateway's REST API to receive latest UWB data for specific tag IDs.
    * Main content: `APIUWBHandler` class. The `get_uwb_data(frame_timestamp)` method calls the API to retrieve data. (The frame_timestamp argument exists for interface consistency but returns the latest data at the time of API call).

* **`uwb_pg_handler.py` (PostgreSQL-based - Recommended)**:
    * Role: Connects to PostgreSQL DB to query UWB positioning data closest to (at that time point or the most recent before it) a given image frame timestamp for a specific UWB tag ID.
    * Main content: `UWBPostgresHandler` class, DB connection pool management functions (`init_uwb_pg_pool`, `close_uwb_pg_pool`, etc.). The `get_uwb_data(frame_timestamp)` method queries data from the DB.

* **`rtsp_reader.py`**:
    * Role: Subscribes to video streams from RTSP URLs dynamically assigned through Redis configuration, extracts frames, and puts them into internal processing queues.
    * Main content: `RTSPStreamSubscriber` thread class. Uses OpenCV (`cv2.VideoCapture`).

* **`kafka_image_reader.py`**:
    * Role: Subscribes to image messages (JSON format, including Base64 image data) from Kafka topics dynamically assigned through Redis configuration, decodes them, and puts them into internal processing queues.
    * Main content: `KafkaImageSubscriber` thread class. Uses `kafka-python` library.

* **`frame_processor.py`**:
    * Role: Retrieves (image, timestamp, source information) data from internal processing queues. Uses UWB handlers mapped to the corresponding source to retrieve and fuse UWB data. Compresses/encodes images to standard format, then constructs final JSON messages and publishes them to Kafka.
    * Main content: `FrameProcessor` thread class. Runs as multiple worker threads.

## 4. Runtime Environment and Key Dependencies

* Python 3.9+
* Key Python libraries: See `requirements.txt` for complete list
* Kafka cluster
* Redis server (for service configuration storage)
* PostgreSQL server (when using PostgreSQL UWB handler, for UWB data storage)
* (Optional) UWB gateway API (when using API UWB handler)

This service is Docker container-based and will be orchestrated within a K8S cluster along with other services.

## 5. Dynamic Source Configuration via Redis

* **Redis Key Pattern**: Set by environment variable `REDIS_SERVICE_CONFIG_KEY_PATTERN` (default: `service_configs:*`).
* **Redis Value (JSON string example)**:
    ```json
    {
        "service_name": "cam_lobby_rtsp", // Used as camera_id within the Wrapper service
        "description": "Lobby RTSP camera with UWB tag 101",
        "input_camera_id": "actual_visibility_cam_id_lobby", // Reference field
        "input_uwb_tag_id": "101", // UWB tag ID to be mapped to this camera
        "uwb_handler_type": "postgresql", // "postgresql" or "api". Uses DEFAULT_UWB_HANDLER_TYPE if not specified.
        "visibility_camera_info": {
            "camera_name": "Lobby Cam",
            "stream_protocol": "RTSP", // "RTSP" or "KAFKA"
            "stream_details": {
                // For RTSP:
                "rtsp_url": "rtsp://user:pass@192.168.0.10/stream1"
                // For KAFKA:
                // "kafka_topic": "raw_images_lobby_cam",
                // "kafka_bootstrap_servers": "10.79.1.1:9094" // Individual setting, uses global KAFKA_BOOTSTRAP_SERVERS if not specified
            }
        }
    }
    ```
    `falcon-wrapper-service` parses the above information to extract `camera_id` (here "cam_lobby_rtsp"), stream type, stream URI/topic, UWB tag ID, UWB handler type, etc., and dynamically starts/stops/updates processing for the corresponding source.

## 6. Key Environment Variable Settings

The following are key environment variables that must be set when running `falcon-wrapper-service`.

#### General Service Settings
* `LOG_LEVEL`: Logging level (e.g., "DEBUG", "INFO", "WARNING", "ERROR"). Default: "INFO".
* `WRAPPER_INSTANCE_ID`: Unique ID for this Wrapper service instance.
* `PROCESSING_QUEUE_MAX_SIZE`: Maximum size of internal processing queue.
* `FRAME_PROCESSOR_WORKERS`: Number of frame processing worker threads.

#### Kafka Default Settings (for output and Kafka image reader)
* `KAFKA_BOOTSTRAP_SERVERS`: **(Required)** Kafka broker address. (e.g., `10.79.1.1:9094`)
* `OUTPUT_KAFKA_TOPIC`: **(Required)** Kafka topic name where processed data will be published.
* `KAFKA_PRODUCER_MAX_REQUEST_SIZE`: Kafka producer maximum request size (bytes).
* `KAFKA_PRODUCER_RETRIES`: Kafka producer retry count.
* `KAFKA_PRODUCER_ACKS`: Kafka producer `acks` setting.
* `KAFKA_CONSUMER_TIMEOUT_MS`: Kafka image reader consumer timeout (milliseconds).
* `KAFKA_PRODUCER_FLUSH_TIMEOUT_SEC`: Kafka producer `flush` timeout (seconds).
* `KAFKA_PRODUCER_CLOSE_TIMEOUT_SEC`: Kafka producer `close` timeout (seconds).

#### Redis Settings (for dynamic source configuration loading)
* `REDIS_HOST`: **(Required)** Redis server host.
* `REDIS_PORT`: **(Required)** Redis server port.
* `REDIS_PASSWORD`: Redis password (optional).
* `REDIS_DB_CONFIGS`: Redis DB number to read service configurations from.
* `REDIS_SERVICE_CONFIG_KEY_PATTERN`: Key pattern to scan in Redis (default: `service_configs:*`).
* `REDIS_POLLING_INTERVAL_SEC`: Redis configuration polling interval (seconds) (default: 60).
* `DEFAULT_UWB_HANDLER_TYPE`: Default UWB handler type to use when `uwb_handler_type` is not specified in individual source configurations in Redis ("postgresql" or "api").

#### UWB Handler: API Method (global defaults)
* `UWB_GATEWAY_URL`: UWB gateway API URL (when using API handler).
* `UWB_API_KEY`: UWB API key (optional).
* `UWB_API_TIMEOUT_SEC`: UWB API call timeout (seconds).

#### UWB Handler: PostgreSQL Method (global defaults)
* `POSTGRES_HOST_UWB`: **(Required when using PostgreSQL UWB)** PostgreSQL server host.
* `POSTGRES_PORT_UWB`: **(Required when using PostgreSQL UWB)** PostgreSQL server port.
* `POSTGRES_DB_UWB`: **(Required when using PostgreSQL UWB)** UWB database name.
* `POSTGRES_USER_UWB`: **(Required when using PostgreSQL UWB)** PostgreSQL username.
* `POSTGRES_PASSWORD_UWB`: **(Required when using PostgreSQL UWB)** PostgreSQL password.
* `UWB_TABLE_NAME`: **(Required when using PostgreSQL UWB)** UWB data table name.
* `UWB_DB_MAX_CONNECTIONS`: UWB DB connection pool maximum connections.

#### Image Processing Settings
* `IMAGE_OUTPUT_FORMAT`: Output image format ("jpeg" or "png").
* `JPEG_QUALITY`: JPEG compression quality (0-100).

## 7. Docker Execution Example

Below is an example Docker container execution command. Modify values according to your actual environment.

```bash
docker run -d --name falcon-wrapper-dynamic \
  --network host \
  -e LOG_LEVEL="INFO" \
  -e WRAPPER_INSTANCE_ID="wrapper-main-01" \
  -e PROCESSING_QUEUE_MAX_SIZE="200" \
  -e FRAME_PROCESSOR_WORKERS="2" \
  -e KAFKA_BOOTSTRAP_SERVERS="10.79.1.1:9094" \
  -e OUTPUT_KAFKA_TOPIC="fused_vision_uwb_output" \
  -e KAFKA_PRODUCER_MAX_REQUEST_SIZE="10485760" \
  -e KAFKA_PRODUCER_RETRIES="5" \
  -e KAFKA_PRODUCER_ACKS="1" \
  -e KAFKA_CONSUMER_TIMEOUT_MS="1000" \
  -e KAFKA_PRODUCER_FLUSH_TIMEOUT_SEC="10" \
  -e KAFKA_PRODUCER_CLOSE_TIMEOUT_SEC="10" \
  -e REDIS_HOST="your-redis-host" \
  -e REDIS_PORT="6379" \
  -e REDIS_PASSWORD="your-redis-password" \
  -e REDIS_DB_CONFIGS="0" \
  -e REDIS_SERVICE_CONFIG_KEY_PATTERN="service_configs:*" \
  -e REDIS_POLLING_INTERVAL_SEC="30" \
  -e DEFAULT_UWB_HANDLER_TYPE="postgresql" \
  -e UWB_GATEWAY_URL="http://your-uwb-gateway-api-url" \
  -e UWB_API_KEY="your-uwb-api-key" \
  -e UWB_API_TIMEOUT_SEC="2" \
  -e POSTGRES_HOST_UWB="your-pg-host-for-uwb" \
  -e POSTGRES_PORT_UWB="5432" \
  -e POSTGRES_DB_UWB="uwb_database_name" \
  -e POSTGRES_USER_UWB="uwb_db_username" \
  -e POSTGRES_PASSWORD_UWB="uwb_db_password" \
  -e UWB_TABLE_NAME="uwb_raw_data_table_name" \
  -e UWB_DB_MAX_CONNECTIONS="5" \
  -e IMAGE_OUTPUT_FORMAT="jpeg" \
  -e JPEG_QUALITY="85" \
  your-repo/falcon-wrapper-service:latest
```