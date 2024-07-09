import traceback
import hailo
from gsthailo import VideoFrame
from gi.repository import Gst
import numpy as np
from sklearn.neighbors import KNeighborsClassifier
from apscheduler.schedulers.background import BackgroundScheduler
import cv2
import json
import datetime
import sqlite3
import requests
import atexit
from kafka import KafkaProducer
import uuid
from TAPPAS_WORKSPACE import tappas_workspace
from logger import create_logger
from color_utils import detect_colors, get_dominant_color


logger, traces = create_logger("Instance_Segmentation")
logger.info("Starting Instance Segmentation module")

DB_NAME = f"{tappas_workspace}/data/config.db"

class SystemInfo:
    restarting = False
    setup_complete = False
    last_frame = datetime.datetime.now()

global sys_info
sys_info = SystemInfo()

global producer_book
producer_book = {}

global prev_time
prev_time = datetime.datetime.now()
global start_time
start_time = 0

# required for Kafka producer serialization
def serializer(message):
    return json.dumps(message).encode('utf-8')

def run(video_frame: VideoFrame):
    """Configure Instance Segmentation Analytics Engine for incoming frame
        e.g., roi and metadata attached to the incoming frame
        and associated kafka topic for the instance segmentation plugin.

    Args:
        video_frame (VideoFrame): Gstreamer video frame object 

    Returns:
         Gst Object: Gstreamer flow status (OK)
    """
    try:

        metas = video_frame.roi.get_objects_typed(hailo.HAILO_DETECTION)
        meta = metas.get_label()
        meta_json = json.loads(meta)
        # Check if meta_json has a specific key (e.g., 'label') and it has a value
        # if meta_json and 'label' in meta_json and meta_json['label']:
        if meta_json:
            kafka_topic = meta_json["color_detection"]
            
            # 1 to 1 relationship between producers and topics
            if kafka_topic in producer_book:
                producer = producer_book[kafka_topic]
            else:
                # TODO: change from localhost to an address provided in the config
                producer = KafkaProducer(
                    bootstrap_servers=['localhost:9092'],
                    value_serializer=serializer,
                    request_timeout_ms=100,
                    acks=0
                )
                producer_book[kafka_topic] = producer

    except BaseException as b:
        logger.error(f"Error configuring Detect Analytics Engine for incoming frame: {b}", exc_info=traces)

    else:
        if meta_json["label"] == True:
            kafka_send(video_frame, kafka_topic, meta)
            # det = {'label': 'person', 'confidence': 0.7058823704719543, 'bbox': {'xmin': 0.24793577194213867, 'ymin': 0.2933201789855957, 'xmax': 0.32929229736328125, 'ymax': 0.4599316418170929, 'width': 0.08135652542114258, 'height': 0.1666114628314972}, 'obj_uuid': '5bf09cef-db47-468d-8782-b9f28934395d'}
            # data = {'detection': det, 'meta': json.loads(meta)}
            # producer = producer_book[kafka_topic]
            # producer.send(kafka_topic, data)

    return Gst.FlowReturn.OK

def run_scheduler():
    """Load Count Analytics Engine background scheduler with interval of 2 seconds
     and pass over the Analytics data to the count plugin 
    """
    current_time = datetime.datetime.now()

    # handle pipeline fallover
    time_lapsed = current_time - sys_info.last_frame
    
    if sys_info.setup_complete:
        restart_timer = 10
    else:
        restart_timer = 60
    
    if time_lapsed > datetime.timedelta(seconds=restart_timer) and not sys_info.restarting:
        logger.warning(f"No frames recieved in last 10 seconds, attempting to restart pipeline")
        # make http to flask, /restart_pipeline
        url = "http://localhost:47760/restart_detection"
        response = requests.get(url, verify=False)
        sys_info.restarting = True

# run scheduler
sched_interval = 10
logger.debug(f"Loading Detect Analytics Engine background scheduler with interval of {sched_interval} second")
scheduler = BackgroundScheduler()
scheduler.add_job(run_scheduler, 'interval', seconds=sched_interval)
scheduler.start()

def shutdown():
    logger.info('Shutting down Detect Analytics Engine')

atexit.register(shutdown)

def kafka_send(video_frame: VideoFrame, topic, meta):
    try:
        global sys_info
        
        current_time = datetime.datetime.now()
        sys_info.last_frame = current_time
        sys_info.setup_complete = True
        
        meta_json = json.loads(meta)

        camera_id = meta_json["camera_id"]
        selected_object_list = []
        
        connection = sqlite3.connect(DB_NAME)
        cursor = connection.cursor()        
        query = f"SELECT detect_con_threshold, detect_objects from Sources WHERE camera_id = '{camera_id}' "
        cursor.execute(query)        
        fetch_figure_data = cursor.fetchone()

        nx_con_thresh = fetch_figure_data[0]
        selected_objects = fetch_figure_data[1]
        
        if str(selected_objects) != '' and selected_objects != None:
            selected_object_list = json.loads(selected_objects)

        roi = video_frame.roi
        if video_frame.video_info:
            frame_width = video_frame.video_info.width
            frame_height = video_frame.video_info.height

        if frame_width <= 0 or frame_height <= 0:
            print("Error: Invalid frame dimensions. Unable to proceed.")
            return Gst.FlowReturn.ERROR

        # Extract frame data
        with video_frame.map_buffer() as map_info:
            frame = VideoFrame.numpy_array_from_buffer(map_info, video_info=video_frame.video_info)
            frame_height, frame_width = frame.shape[:2]

        for obj in roi.get_objects_typed(hailo.HAILO_DETECTION):
            label = obj.get_label()
            confidence = obj.get_confidence()
            bbox = obj.get_bbox()  

            # # Skip objects with low confidence
            # if confidence < 0.5:
            #     continue

            if label.lower() in [obj.lower() for obj in selected_object_list]:
                if confidence > nx_con_thresh:
                    for sub_obj in obj.get_objects():
                        if isinstance(sub_obj, hailo.HailoConfClassMask):
                            mask_data = sub_obj.get_data()
                        
                            frame_width = video_frame.video_info.width
                            frame_height = video_frame.video_info.height

                            x_min_abs = int(bbox.xmin() * frame_width)
                            y_min_abs = int(bbox.ymin() * frame_height)
                            x_max_abs = int(bbox.xmax() * frame_width)
                            y_max_abs = int(bbox.ymax() * frame_height) 
                            xmin = (np.clip(bbox.xmin(), 0, 1))
                            ymin = (np.clip(bbox.ymin(), 0, 1))
                            xmax = (np.clip(bbox.xmax(), 0, 1))
                            ymax = (np.clip(bbox.ymax(), 0, 1))
                            width = xmax - xmin
                            height = ymax - ymin      

                            roi_frame = frame[y_min_abs:y_max_abs, x_min_abs:x_max_abs, :]

                            # Resize mask data to match ROI frame size
                            try:
                                mask_data = np.array(mask_data).reshape((roi_frame.shape[0], roi_frame.shape[1]))
                            except ValueError:
                                print(f"Resizing mask_data from shape {len(mask_data)} to fit ROI {roi_frame.shape[:2]}")
                                mask_data = np.array(mask_data).reshape((-1,))
                                mask_data = cv2.resize(mask_data, (roi_frame.shape[1], roi_frame.shape[0]))
                    
                                binary_mask = (mask_data > 0.5).astype(np.uint8)

                            if label.lower() == "person":
                                # Split the mask vertically into two parts
                                mid_point = binary_mask.shape[0] // 2
                                top_mask = binary_mask[:mid_point, :]
                                bottom_mask = binary_mask[mid_point:, :]

                                top_frame = roi_frame[:mid_point, :]
                                bottom_frame = roi_frame[mid_point:, :]

                                # Process each part separately
                                detected_colors_top = detect_colors(top_frame, top_mask)
                                # detected_colors_bottom = detect_colors(bottom_frame, bottom_mask)

                                dominant_color_top = get_dominant_color(detected_colors_top)
                                # dominant_color_bottom = get_dominant_color(detected_colors_bottom)

                                results = {
                                    "label": label,
                                    "confidence": confidence,
                                    "bbox": {'xmin': xmin, 'ymin': ymin, 'xmax': xmax, 'ymax': ymax, 'width': width, 'height': height},
                                    "dominant_color": dominant_color_top
                                    # "pant_color": dominant_color_bottom
                                }
                                # print(f"Results: {results}") 
                                kafka_topic = meta_json["color_detection"]
                                data = {'detection': results, 'meta': meta_json}

                            else:
                                masked_roi = cv2.bitwise_and(roi_frame, roi_frame, mask=binary_mask)
                                detected_colors = detect_colors(masked_roi, binary_mask)
                                dominant_color = get_dominant_color(detected_colors)

                                results = { 
                                   "label": label, 
                                   "confidence": confidence, 
                                   "bbox": {'xmin': xmin, 'ymin': ymin, 'xmax': xmax, 'ymax': ymax, 'width': width, 'height': height},
                                   "dominant_color": dominant_color }
                                # print(f"Results: {results}")
                                kafka_topic = meta_json["color_detection"]
                                data = {'detection': results, 'meta': meta_json}

                                # send data over kafka
                                logger.info(f"Sending color detect data for {results['label']} object to kafka topic: {kafka_topic} CamID: {meta_json['camera_id']}")
                                logger.debug(f"Sent data: {data}")
                                producer = producer_book[topic]
                                producer.send(kafka_topic, data)        

    except BaseException as b:
        logger.error(f"Error sending metadata via Kafka: {b}", exc_info=traces)

