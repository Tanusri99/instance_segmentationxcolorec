import traceback
import hailo
from gsthailo import VideoFrame
from gi.repository import Gst
import numpy as np
from sklearn.neighbors import KNeighborsClassifier
import cv2
import json
from color_utils import detect_colors, get_dominant_color
from kafka_producer import send_detection_results_to_kafka


def run(video_frame: VideoFrame):
    try:
        roi = video_frame.roi
        if video_frame.video_info:
            frame_width = video_frame.video_info.width
            frame_height = video_frame.video_info.height
        else:
            frame_width = 0
            frame_height = 0

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

            # Skip objects with low confidence
            if confidence < 0.5:
                continue
        
            for sub_obj in obj.get_objects():
                if isinstance(sub_obj, hailo.HailoConfClassMask):
                    mask_data = sub_obj.get_data()
                        
                    frame_width = video_frame.video_info.width
                    frame_height = video_frame.video_info.height

                    x_min_abs = int(bbox.xmin() * frame_width)
                    y_min_abs = int(bbox.ymin() * frame_height)
                    x_max_abs = int(bbox.xmax() * frame_width)
                    y_max_abs = int(bbox.ymax() * frame_height)        

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
                        detected_colors_bottom = detect_colors(bottom_frame, bottom_mask)

                        dominant_color_top = get_dominant_color(detected_colors_top)
                        dominant_color_bottom = get_dominant_color(detected_colors_bottom)

                        results = {
                            "label": label,
                            "confidence": confidence,
                            "top_colors": dominant_color_top,
                            "pant_colors": dominant_color_bottom
                        }
                        print(f"Results: {results}") 
                        send_detection_results_to_kafka(label, confidence, dominant_color_top, dominant_color_bottom, "")

                    else:
                        masked_roi = cv2.bitwise_and(roi_frame, roi_frame, mask=binary_mask)
                        detected_colors = detect_colors(masked_roi, binary_mask)
                        dominant_color = get_dominant_color(detected_colors)

                        results = { 
                                   "label": label, 
                                   "confidence": confidence, 
                                   "dominant_colors": dominant_color }
                        print(f"Results: {results}")
                        send_detection_results_to_kafka(label, confidence, "", "", dominant_color)

            return Gst.FlowReturn.OK

    except Exception as e:
        print("Error in Python function:")
        traceback.print_exc()
        return Gst.FlowReturn.ERROR



    return Gst.FlowReturn.OK

