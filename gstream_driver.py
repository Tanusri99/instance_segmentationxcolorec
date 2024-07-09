import os
import subprocess
import signal
import atexit
import sqlite3
import argparse
import sys  

from pytz import UTC

# import faulthandler
from TAPPAS_WORKSPACE import tappas_workspace, workspace_stub
from logger import create_logger
import NxAPI as nx
import db_handler as db

logger, traces = create_logger("Gstreamer_Driver")
DB_NAME = f"{tappas_workspace}/data/config.db"

class DetectAndColorPipeline:
    """This class is responsible for generating the Instance Segmentation & Color pipeline, this pipeline does instance segmentation 
    for object detection & color detection as python post-process."""
    
    def __init__(self):
        global postprocess_so, network_name, video_source, hef_path, print_gst_launch_only, additional_parameters, video_sink_element, python_module, json_config_path
        self.workspace_stub = workspace_stub
        self.resource_dir = f"{self.workspace_stub}/apps/h8/gstreamer/general/instance_segmentation/resources"
        self.postprocess_dir = f"{self.workspace_stub}/apps/h8/gstreamer/libs/post_processes"
        self.postprocess_so = f"{self.postprocess_dir}/libyolov5seg_post.so"
        self.hef_path = f"{self.resource_dir}/yolov5n_seg.hef"
        self.json_config_path = f"{self.resource_dir}/configs/yolov5seg.json"
        self.network_name = "yolov5seg"
        self.python_module = f"{self.workspace_stub}/apps/h8/gstreamer/general/instance_segmentation/main.py"
        

    def build_pipeline(self, source_count: int, enable_detect: bool, enable_count: bool) -> str:
        self.__select_model()

        pipeline = ""

        if os.environ["CENTURY_PCIE"] == "true":
            logger.info("Enabling inferencing using multiple Hailo-8s on the Century Pcie platform")
            hailonet_pipeline = f"hailonet hef-path={self.hef_path} vdevice-key=1 scheduling-algorithm=1 scheduler-threshold=1 scheduler-timeout-ms=100 !"
        else:
            hailonet_pipeline = f"hailonet hef-path={self.hef_path} is-active=true batch-size=16 !"

        kafka_pipeline = ""
       
        kafka_pipeline += f"hailopython name=hailopython_instance_segmentation qos=false \
                                module={self.python_module} function=run !"


        pipeline += f"hailoroundrobin name=detect_rr ! \
            queue name=hailo_pre_infer0_q leaky=no max-size-buffers={50*source_count} max-size-bytes=0 max-size-time=0 ! \
            {hailonet_pipeline} \
            queue leaky=no max-size-buffers=100 max-size-bytes=0 max-size-time=0 ! \
            hailofilter function-name={self.network_name} so-path={self.postprocess_so} config-path={self.json_config_path} qos=false ! \
            queue leaky=no max-size-buffers=100 max-size-bytes=0 max-size-time=0 ! \
            {kafka_pipeline} \
            queue leaky=no max-size-buffers=100 max-size-bytes=0 max-size-time=0 ! \
            hailooverlay qos=false ! \
            queue leaky=no max-size-buffers=100 max-size-bytes=0 max-size-time=0 ! \
            videoconvert ! \
            fakesink name=hailo_display sync=false "
        

class gStreamHandler:
    """ Gstreamer handles class to initialize GST environment related to hailo """
    def __init__(self):
        self.workspace_stub = workspace_stub

        self.sources = ""
        self.pipeline = ""

        self.additonal_parameters = "&"
        self.is_running = False

    def __get_camera_details(self) -> list:
        """Fetches the camera details from a database.

        Returns:
            list: [ ( rtsp_address, rowid, source_type, object_detected, color_detected ) ]
        """
        connection = sqlite3.connect(DB_NAME)
        cursor = connection.cursor()
 
        cursor.execute("SELECT rtsp_address, rowid, source_type, object_detected, color_detected FROM Sources ORDER BY rowid ASC")
        rows = cursor.fetchall()

        cursor.close()
        connection.close()

        return rows
            
    def build_pipeline(self, num_streams: int) -> None:
        """build the Gstreamer pipeline

        Args:
            num_streams (int): total number of streams running
        """
        # clear sources
        self.sources = ""
        
        if num_streams == 0:
            logger.error(f"No camera source detected, cannot build Gstreamer Pipeline")
            return
        
        enable_detect = False
      
        logger.info("Constructing Gstreamer Pipeline")
        
        rows = self.__get_camera_details()
        
        # default source codec type as .H264 no HW acceleration
        codec_depay = 'rtph264depay ! h264parse'
        decoder = "avdec_h264"
        rescaler = "videoscale ! videoconvert"

        for row in rows:
            # if detect_flag == True
            if row[3] == 1:
                enable_detect = True
        
        source_count = 0
        detect_rr_sink_index = 0

        for row in rows:
            if source_count >= num_streams:
                logger.warning(f"More sources than licenses, only activating {source_count} streams. Please disable {len(rows) - num_streams} Nx Witness plugins")
                break
            if source_count >= 64:
                logger.warning(f"Cannot support more than 64 sources, only activating {source_count} streams. Please disable {len(rows) - num_streams} Nx Witness plugins")
                break
            
            no_analytics_enabled = row[3] == 0  
            if no_analytics_enabled: continue
            
            rtsp_source = row[0]
            source_type = row[2]

            if source_type == 'H265':
                codec_depay = 'rtph265depay ! h265parse'
                if os.environ["ENABLE_VAAPI"] == "true":
                    decoder = "vaapih265dec"
                elif os.environ["ENABLE_IMX8"] == "true":
                    decoder = "imxvpudec_h265"
                else:
                    decoder = "avdec_h265"
            if source_type == 'H264':
                codec_depay = 'rtph264depay ! h264parse'
                if os.environ["ENABLE_VAAPI"] == "true":
                    decoder = "vaapih264dec"
                elif os.environ["ENABLE_IMX8"] == "true":
                    decoder = "imxvpudec_h264"
                else:
                    decoder = "avdec_h264"
            if source_type == 'MJPEG':
                codec_depay = 'rtpjpegdepay ! jpegparse'
                decoder = "jpegdec"
            
            if os.environ["ENABLE_VAAPI"] == "true":
                # untested, my require additional plugins
                rescaler = "vaapipostproc"
            elif os.environ["ENABLE_IMX8"] == "true":
                # reminder, does not do videorate if needed
                rescaler = "imxvideoconvert_g2d"
            else:
                rescaler = "videoscale ! videoconvert"

            self.sources += f"rtspsrc location={rtsp_source} name=source_{source_count} ! \
                            rtponvifparse ! \
                            {codec_depay} ! \
                            queue name=hailo_decode{source_count}_q leaky=no max-size-buffers=50 max-size-bytes=0 max-size-time=0 ! \
                            {decoder} ! \
                            queue name=hailo_scale{source_count}_q leaky=no max-size-buffers=50 max-size-bytes=0 max-size-time=0 ! \
                            {rescaler} ! \
                            queue name=hailo_metadata{source_count}_q leaky=no max-size-buffers=50 max-size-bytes=0 max-size-time=0 ! \
                            hailopython qos=false module={self.workspace_stub}/apps/h8/gstreamer/general/aol_detector/add_metadata.py function=cam_{source_count} ! \
                            queue name=source_tee{source_count}_q leaky=no max-size-buffers=50 max-size-bytes=0 max-size-time=0 ! \
                            tee name=source_{source_count}_tee "
            
            # leaky=downstream prevents a sub-pipeline affecting other sub-pipelines
            # use of this tee assumes that camera will always have atleast 1 plugin enabled
            
            if row[3] == 1:
                self.sources += f"source_{source_count}_tee. ! \
                                queue leaky=downstream max-size-buffers=50 max-size-bytes=0 max-size-time=0 ! \
                                detect_rr.sink_{detect_rr_sink_index} "
                detect_rr_sink_index += 1

            source_count += 1
        
        self.pipeline = f"gst-launch-1.0 \
            \
            {DetectAndColorPipeline().build_pipeline(source_count, enable_detect)} \
            \
            {self.sources} {self.additonal_parameters}"

            
    def start_pipeline(self):
        """ start gstreamer pipeline through subprocess
        """
        # faulthandler.enable()
        logger.info(f"Opening subprocess for Gstreamer Pipeline")
        try:           
            self.proc = subprocess.Popen(self.pipeline, preexec_fn=os.setsid, shell=True)
            
            # add cleanup func to run at interpreter exit
            # implementing in this way keeps the object in memory until exit, not necessary but also not a problem
            # https://stackoverflow.com/questions/16333054/what-are-the-implications-of-registering-an-instance-method-with-atexit-in-pytho
            atexit.register(self.stop_background)
            self.is_running = True
            
        except BaseException as b:
            logger.error(f"Failed to open subprocess for Gstreamer Pipeline: {b}", exc_info=traces)

    def stop_pipeline(self):
        """ Stops the pipeline by killing the subprocess
        """
        if self.is_running:
            logger.info('Killing subprocess for Gstreamer Pipeline')
            os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
            
            self.is_running = False
        
            # remove cleanup from running at interpreter exit
            atexit.unregister(self.stop_background)

    def stop_background(self):
        """ Stop background process
        """
        if self.is_running:
            logger.info('Atexit, killing subprocess for Gstreamer Pipeline')
            os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
            
            self.is_running = False
        
            # remove cleanup from running at interpreter exit
            atexit.unregister(self.stop_background)

    def get_pipeline_status(self):
        """Returns True if pipeline is running, False if pipeline is not running."""
        return self.is_running
   
        
