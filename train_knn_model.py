import cv2
import numpy as np
from sklearn.neighbors import KNeighborsClassifier
import os
import joblib

# Directory containing the training images organized by color labels
dataset_path = "/local/workspace/tappas/apps/h8/gstreamer/general/instance_segmentation/training_dataset"

def extract_color_histogram(image, bins=(8, 8, 8)):
    hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
    hist = cv2.calcHist([hsv], [0, 1, 2], None, bins, [0, 180, 0, 256, 0, 256])
    hist = cv2.normalize(hist, hist).flatten()
    return hist

def load_dataset(dataset_path):
    data = []
    labels = []
    
    for label in os.listdir(dataset_path):
        label_path = os.path.join(dataset_path, label)
        if not os.path.isdir(label_path):
            continue
        
        for image_name in os.listdir(label_path):
            image_path = os.path.join(label_path, image_name)
            image = cv2.imread(image_path)
            if image is None:
                continue
            
            hist = extract_color_histogram(image)
            data.append(hist)
            labels.append(label)
    
    return np.array(data), np.array(labels)

print("Loading dataset...")
data, labels = load_dataset(dataset_path)

print("Training KNN model...")
knn_model = KNeighborsClassifier(n_neighbors=3)
knn_model.fit(data, labels)

model_path = "/local/workspace/tappas/apps/h8/gstreamer/general/instance_segmentation/knn_color_model.joblib"
joblib.dump(knn_model, model_path)
print(f"Model saved to {model_path}")
