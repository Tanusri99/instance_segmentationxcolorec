import numpy as np
import cv2
   

def process_mask(mask_data, roi_frame):
    try:
        mask_data = np.array(mask_data).reshape((roi_frame.shape[0], roi_frame.shape[1]))
    except ValueError:
        print(f"Resizing mask_data from shape {len(mask_data)} to fit ROI {roi_frame.shape[:2]}")
        mask_data = np.array(mask_data).reshape((-1,))
        mask_data = cv2.resize(mask_data, (roi_frame.shape[1], roi_frame.shape[0]))
                    
    binary_mask = (mask_data > 0.5).astype(np.uint8)
    return binary_mask

