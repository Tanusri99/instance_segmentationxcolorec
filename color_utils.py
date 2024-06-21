import cv2
import numpy as np
import joblib
from sklearn.neighbors import KNeighborsClassifier
from collections import Counter

# Function to extract color histogram from an image
def extract_color_histogram(image, bins=(8, 8, 8)):
    hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
    hist = cv2.calcHist([hsv], [0, 1, 2], None, bins, [0, 180, 0, 256, 0, 256])
    hist = cv2.normalize(hist, hist).flatten()
    return hist
    
# Define updated color ranges in HSV space
color_ranges = {
        'red': ([0, 120, 70], [10, 255, 255]),
        'red2': ([170, 120, 70], [180, 255, 255]),         # Red spans two ranges
        'cyan': [(85, 50, 50), (105, 255, 255)],           # Cyan
        'magenta': [(145, 50, 50), (165, 255, 255)],       # Magenta
        'orange': [(10, 50, 50), (25, 255, 255)],          # Orange
        'yellow': [(25, 50, 50), (35, 255, 255)],          # Yellow
        'dark green': [(35, 50, 50), (55, 255, 255)],      # Dark Green
        'light green': [(55, 50, 50), (85, 255, 255)],     # Light Green
        'dark blue': [(85, 50, 50), (105, 255, 255)],      # Dark Blue
        'light blue': [(105, 50, 50), (125, 255, 255)],    # Light Blue
        'purple': [(125, 50, 50), (145, 255, 255)],        # Purple
        'pink': [(145, 50, 50), (165, 255, 255)],          # Pink
        'dark brown': [(10, 50, 50), (20, 255, 150)],      # Dark Brown
        'light brown': [(20, 50, 150), (30, 255, 200)],    # Light Brown
        'dark grey': [(0, 0, 50), (180, 50, 100)],         # Dark Grey
        'light grey': [(0, 0, 100), (180, 50, 200)],       # Light Grey
        'black': [(0, 0, 0), (180, 255, 50)],              # Black
        'white': [(0, 0, 200), (180, 20, 255)],            # White
        'light_pink': [(170, 50, 50), (180, 255, 255)],    # Light Pink
        'dark_pink': [(140, 50, 50), (170, 255, 255)],     # Dark Pink
        'beige': [(0, 50, 50), (20, 150, 255)],            # Beige
        'navy_blue': [(95, 50, 50), (125, 255, 255)],      # Navy Blue
        'light_red': [(0, 50, 50), (10, 255, 255)],        # Light Red
        'dark_red': [(170, 50, 50), (180, 255, 255)],       # Dark Red
        'teal': [(85, 50, 50), (125, 255, 255)],           # Teal
        'lime': [(35, 50, 50), (85, 255, 255)],            # Lime
        'silver': [(0, 0, 180), (180, 25, 255)],           # Silver
        'lilac': [(140, 50, 50), (170, 255, 255)]          # Lilac
    }
    
    # Function to detect colors in a given frame
def detect_colors(frame_rgb, mask):
    # Convert the RGB frame to HSV
    frame_hsv = cv2.cvtColor(frame_rgb, cv2.COLOR_RGB2HSV)

    detected_colors = {}

    for color, (lower, upper) in color_ranges.items():
        lower = np.array(lower, dtype=np.uint8)
        upper = np.array(upper, dtype=np.uint8)

        # Create a mask for the current color
        color_mask = cv2.inRange(frame_hsv, lower, upper)
        
        # Apply the binary mask to the color mask
        combined_mask = cv2.bitwise_and(color_mask, mask)
        
        # Count non-zero pixels in the mask to determine the presence of the color
        count = cv2.countNonZero(combined_mask)
        
        if count > 1000:
            detected_colors[color] = count
            print(f"Detected color: {color}, Count: {count}")

    return detected_colors

# Function to get the dominant color from detected colors
def get_dominant_color(detected_colors):
    if detected_colors:
        return max(detected_colors, key=detected_colors.get)
    return None
