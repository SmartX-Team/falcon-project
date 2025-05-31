# Falcon Project 2025
Architecture Optimization for Digital Twin Environment

## Overview

This document outlines the Falcon Project results, which has been architecturally improved and optimized for digital twin environment operations in 2025. The core enhancement involves cloud-native separation of camera streaming and AI inference logic to increase scalability, flexibility, and GPU utilization efficiency.

## 1. Limitations of the Original Falcon Container Approach

The original Falcon Project (https://github.com/NetAiFalcon/falcon), developed by undergraduate interns Minjun Jo and Cheolhee Kim, successfully achieved its initial project goals in a container-based approach. However, it had the following major limitations:

### Physical Constraints
- Camera devices and GPU equipment for AI inference had to be physically connected and deployed on the same host machine
- This constraint limited efficient processing of cameras located at various locations through centralized or distributed GPU clusters

### Single Container Multiple Responsibilities
- One container handled multiple major functions: camera capture, AI inference (YOLOv5, UniDepthV2, etc.), and result data transmission
- This reduced flexibility and stability, as scaling or updating specific functions or handling failures affected the entire system

### Service Scalability Constraints
- To process multiple camera streams by deploying multiple Falcon containers, physical host machines equipped with GPU resources equal to the number of instances were required, limiting scalability

## 2. Target Architecture: Distributed Streaming and Cloud-Based Inference

To overcome these limitations, this repository presents an improved architecture that clearly separates streaming and inference functions and utilizes our lab's own Kubernetes cluster (cloud-based infrastructure).

![Falcon-25year](https://github.com/user-attachments/assets/b935ab3d-04b4-4502-a843-07c5b47852aa)

### Camera Agent (Container, utilizing existing developed agents)

**Repository Reference**: https://github.com/SmartX-Team/camera-agent.git

#### Input
- Local camera devices (/dev/video* and other actual hardware cameras)
- ROS2 image topic subscription (real Husky/Isaac Sim virtual cameras, etc.)
- Supports RTSP/Kafka video streaming

#### Processing
- **Image/Video Data Acquisition**: Captures raw video/image data from input sources
- **Image/Video Compression**: 
  - H.264 compression for RTSP streaming
  - JPEG compression for Kafka transmission
- **Payload Construction**: Creates JSON payloads containing compressed data or metadata

#### Output
- **RTSP Streaming**: Streams H.264-encoded video directly to Inference services or other RTSP clients via RTSP protocol
- **Kafka Publishing**: Publishes (compressed) image data or video frame information as JSON to specific Kafka topics (e.g., `raw_video_frames`, `compressed_video_frames`)

#### Visibility Server Integration
- **Registration**: Registers agent and managed camera details to Visibility server on startup (`/agent_register`)
- **Status Updates**: Periodically updates actual data transmission status (RTSP streaming status, Kafka publishing status, etc.) to Visibility server (`/agent_update_status`)

#### Characteristics
- Lightweight, GPU-free (minimal CPU usage for encoding)
- Deployable near each camera source
- Manages "one camera per agent" or a small number of cameras

### Inference Service (Kubernetes Cluster Container with GPU)

#### Input
- **RTSP Subscription**: Subscribes to RTSP streams provided by Camera Agents to receive video frames (using OpenCV + FFmpeg, etc.)
- **Kafka Subscription**: Subscribes to `video_frames` topics published by Camera Agents to receive image/video data

#### Processing
- **Data Decoding**: Decodes received data and reconstructs images
- **AI Inference**: Performs object detection and depth inference using AI models (YOLOv5, UniDepthV2, etc.)
- **GPU Optimization**: Efficiently uses GPU resources in Kubernetes environment (considers NVIDIA Triton Inference Server utilization)
- **Performance Optimization**: Implements GPU usage optimization (FP16, TensorRT, etc.)

#### Output
- Publishes inference results (object bounding boxes, classes, confidence scores, depth information, etc.) as JSON to specific Kafka topics (e.g., `inference_results`)

#### Scalability
- Utilizes Kubernetes Horizontal Pod Autoscaler (HPA) to automatically scale instances up/down based on load

### Digital Twin / Real-time Visualization Environment (Consumer)

#### Function
- Subscribes to `inference_results` topics (published by Inference services)
- Real-time visualization of human positions estimated by Falcon
- Integrates with digital twin environment for comprehensive spatial awareness

## 3. Architecture Benefits

### Enhanced Scalability
- **Independent Scaling**: Camera agents and inference services can be scaled independently based on demand
- **Resource Optimization**: GPU resources are centralized and shared efficiently across multiple camera streams
- **Geographic Distribution**: Camera agents can be deployed anywhere while leveraging centralized inference capabilities

### Improved Flexibility
- **Technology Stack Independence**: Each component can use optimal technology stack without affecting others
- **Deployment Flexibility**: Components can be deployed on different infrastructure types (edge devices, cloud, hybrid)
- **Maintenance Isolation**: Updates or maintenance of one component don't affect others

### Cost Efficiency
- **Shared GPU Resources**: Multiple camera streams share GPU infrastructure, reducing overall hardware costs
- **Dynamic Resource Allocation**: Resources are allocated based on actual demand rather than peak capacity
- **Cloud-Native Benefits**: Leverages Kubernetes features for automatic scaling, load balancing, and fault tolerance

## 4. Implementation Considerations

### Network Requirements
- **Bandwidth Planning**: Ensure adequate network bandwidth for RTSP streaming or Kafka message throughput
- **Latency Optimization**: Minimize network latency between camera agents and inference services
- **Reliability**: Implement network redundancy and failure handling mechanisms

### Data Management
- **Stream Quality**: Balance between image quality and network/processing efficiency
- **Buffering Strategy**: Implement appropriate buffering mechanisms to handle network variations
- **Data Persistence**: Consider data retention policies for debugging and analysis

### Security
- **Authentication**: Implement proper authentication mechanisms for RTSP streams and Kafka topics
- **Encryption**: Use TLS/SSL for data transmission security
- **Access Control**: Implement role-based access control for different system components

## 5. Future Enhancements

### Edge AI Integration
- Hybrid processing where simple inference can be done at the edge with complex processing in the cloud
- Dynamic workload distribution based on network conditions and processing requirements

### Advanced Analytics
- Integration with time-series databases for historical analysis
- Machine learning pipeline for continuous model improvement
- Real-time anomaly detection and alerting systems

### Multi-Modal Sensor Fusion
- Integration with other sensor types (LiDAR, thermal cameras, etc.)
- Enhanced spatial understanding through sensor fusion algorithms
- Improved accuracy through multi-modal data correlation

This architecture provides a robust, scalable, and flexible foundation for digital twin environments while maximizing resource utilization and operational efficiency.