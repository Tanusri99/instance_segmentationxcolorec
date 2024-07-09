
import json
from datetime import datetime, timedelta
import json
import sqlite3
import ntplib
from pytz import UTC
from apscheduler.schedulers.background import BackgroundScheduler

import hailo
# Importing VideoFrame before importing GST is must
from gsthailo import VideoFrame
from gi.repository import Gst

from TAPPAS_WORKSPACE import tappas_workspace

DB_NAME = f"{tappas_workspace}/data/config.db"
from logger import create_logger
logger, traces = create_logger("Flask_Server")

source_book = dict()

class SourceInfo:
    def __init__(self, source_num):
        connection = sqlite3.connect(DB_NAME)
        cursor = connection.cursor()
        cursor.execute(f"SELECT  detect_kafka_topic, rowid, camera_id, detect_flag FROM Sources ORDER BY rowid ASC")
        records = cursor.fetchall()
        # if there is at least one row in the table
        if records:
            record = records[source_num]

            self.detect_kafka_topic = record[0]
            self.rowid = record[1]
            self.camera_id = record[2]
            self.detect_flag = record[3]
        
        cursor.close()
        connection.close()

# connect to db
connection = sqlite3.connect(DB_NAME)
cursor = connection.cursor()

# gst health check table
table_gst_health = """CREATE TABLE IF NOT EXISTS Gst_Health (
                        gst_health DATETIME);"""
                
cursor.execute(table_gst_health)

connection.commit()
cursor.close()
connection.close()

#gst health check table update
def gst_health_update():
    try:
        logger.info(f"Schdueling gst health update restart at: {datetime.now(tz=UTC)}")
        health_check_time = datetime.now(tz=UTC)

        connection = sqlite3.connect(DB_NAME)
        cursor = connection.cursor() 
        cursor.execute("SELECT * from Gst_Health")
        data = cursor.fetchall()

        if not len(data):
            #insert
            cursor.execute("INSERT INTO Gst_Health (gst_health) VALUES (?)", (health_check_time,))
           
        else:
            #update
            cursor.execute("UPDATE Gst_Health SET gst_health = ?", (health_check_time,))

        connection.commit()
        cursor.close()
        connection.close()       
    except BaseException as b:
        logger.error(f"Gst health check updating into database failed: {b}", exc_info=traces)


# gst health check scheduler
sched = BackgroundScheduler(timezone=UTC)
freq_gen_event = 1 # minutes
sched.add_job(
    gst_health_update,
    'interval',
    minutes=freq_gen_event,
    max_instances=1
    )
sched.start()


# main func
def cam(video_frame: VideoFrame, source_num: int):
    """adding metadata to the incoming streams 
    
    Args:
        video_frame (VideoFrame): Gstreamer video frame object
        source_num (int): source number

    Returns:
        Gst Object: Gstreamer flow status (OK)
    """
    try:
        if source_num not in source_book:
            source_book[source_num] = SourceInfo(source_num)

        source = source_book[source_num]

        meta = hailo.HailoUserMeta()
        ntp_timestamp = int(str(video_frame.buffer.pts)[:16])
        ntp_converted = ntplib.ntp_to_system_time(ntp_timestamp/1000000) * 1000000

        meta_json = {
            "stream_id": f"cam_{source_num}",
            "timestamp": ntp_converted,
            "detect_flag": source.detect_flag,
            "detect_kafka_topic": source.detect_kafka_topic,
            "camera_id": source.camera_id
        }

        meta.set_user_string(json.dumps(meta_json))
        video_frame.roi.add_object(meta)
    except BaseException as b:
        logger.error(f"Failed to add meta_json to the incoming stream: {b}", exc_info=traces)

    return Gst.FlowReturn.OK

# dummy func to pass through index
def cam_0(video_frame: VideoFrame):
    cam(video_frame, 0)
    return Gst.FlowReturn.OK

def cam_1(video_frame: VideoFrame):
    cam(video_frame, 1)
    return Gst.FlowReturn.OK

def cam_2(video_frame: VideoFrame):
    cam(video_frame, 2)
    return Gst.FlowReturn.OK

def cam_3(video_frame: VideoFrame):
    cam(video_frame, 3)
    return Gst.FlowReturn.OK

def cam_4(video_frame: VideoFrame):
    cam(video_frame, 4)
    return Gst.FlowReturn.OK

def cam_5(video_frame: VideoFrame):
    cam(video_frame, 5)
    return Gst.FlowReturn.OK

def cam_6(video_frame: VideoFrame):
    cam(video_frame, 6)
    return Gst.FlowReturn.OK

def cam_7(video_frame: VideoFrame):
    cam(video_frame, 7)
    return Gst.FlowReturn.OK

def cam_8(video_frame: VideoFrame):
    cam(video_frame, 8)
    return Gst.FlowReturn.OK

def cam_9(video_frame: VideoFrame):
    cam(video_frame, 9)
    return Gst.FlowReturn.OK

def cam_10(video_frame: VideoFrame):
    cam(video_frame, 10)
    return Gst.FlowReturn.OK

def cam_11(video_frame: VideoFrame):
    cam(video_frame, 11)
    return Gst.FlowReturn.OK

def cam_12(video_frame: VideoFrame):
    cam(video_frame, 12)
    return Gst.FlowReturn.OK

def cam_13(video_frame: VideoFrame):
    cam(video_frame, 13)
    return Gst.FlowReturn.OK

def cam_14(video_frame: VideoFrame):
    cam(video_frame, 14)
    return Gst.FlowReturn.OK

def cam_15(video_frame: VideoFrame):
    cam(video_frame, 15)
    return Gst.FlowReturn.OK

def cam_16(video_frame: VideoFrame):
    cam(video_frame, 16)
    return Gst.FlowReturn.OK

def cam_17(video_frame: VideoFrame):
    cam(video_frame, 17)
    return Gst.FlowReturn.OK

def cam_18(video_frame: VideoFrame):
    cam(video_frame, 18)
    return Gst.FlowReturn.OK

def cam_19(video_frame: VideoFrame):
    cam(video_frame, 19)
    return Gst.FlowReturn.OK

def cam_20(video_frame: VideoFrame):
    cam(video_frame, 20)
    return Gst.FlowReturn.OK

def cam_21(video_frame: VideoFrame):
    cam(video_frame, 21)
    return Gst.FlowReturn.OK

def cam_22(video_frame: VideoFrame):
    cam(video_frame, 22)
    return Gst.FlowReturn.OK

def cam_23(video_frame: VideoFrame):
    cam(video_frame, 23)
    return Gst.FlowReturn.OK

def cam_24(video_frame: VideoFrame):
    cam(video_frame, 24)
    return Gst.FlowReturn.OK

def cam_25(video_frame: VideoFrame):
    cam(video_frame, 25)
    return Gst.FlowReturn.OK

def cam_26(video_frame: VideoFrame):
    cam(video_frame, 26)
    return Gst.FlowReturn.OK

def cam_27(video_frame: VideoFrame):
    cam(video_frame, 27)
    return Gst.FlowReturn.OK

def cam_28(video_frame: VideoFrame):
    cam(video_frame, 28)
    return Gst.FlowReturn.OK

def cam_29(video_frame: VideoFrame):
    cam(video_frame, 29)
    return Gst.FlowReturn.OK

def cam_30(video_frame: VideoFrame):
    cam(video_frame, 30)
    return Gst.FlowReturn.OK

def cam_31(video_frame: VideoFrame):
    cam(video_frame, 31)
    return Gst.FlowReturn.OK

def cam_32(video_frame: VideoFrame):
    cam(video_frame, 32)
    return Gst.FlowReturn.OK

def cam_33(video_frame: VideoFrame):
    cam(video_frame, 33)
    return Gst.FlowReturn.OK

def cam_34(video_frame: VideoFrame):
    cam(video_frame, 34)
    return Gst.FlowReturn.OK

def cam_35(video_frame: VideoFrame):
    cam(video_frame, 35)
    return Gst.FlowReturn.OK

def cam_36(video_frame: VideoFrame):
    cam(video_frame, 36)
    return Gst.FlowReturn.OK

def cam_37(video_frame: VideoFrame):
    cam(video_frame, 37)
    return Gst.FlowReturn.OK

def cam_38(video_frame: VideoFrame):
    cam(video_frame, 38)
    return Gst.FlowReturn.OK

def cam_39(video_frame: VideoFrame):
    cam(video_frame, 39)
    return Gst.FlowReturn.OK

def cam_40(video_frame: VideoFrame):
    cam(video_frame, 40)
    return Gst.FlowReturn.OK

def cam_41(video_frame: VideoFrame):
    cam(video_frame, 41)
    return Gst.FlowReturn.OK

def cam_42(video_frame: VideoFrame):
    cam(video_frame, 42)
    return Gst.FlowReturn.OK

def cam_43(video_frame: VideoFrame):
    cam(video_frame, 43)
    return Gst.FlowReturn.OK

def cam_44(video_frame: VideoFrame):
    cam(video_frame, 44)
    return Gst.FlowReturn.OK

def cam_45(video_frame: VideoFrame):
    cam(video_frame, 45)
    return Gst.FlowReturn.OK

def cam_46(video_frame: VideoFrame):
    cam(video_frame, 46)
    return Gst.FlowReturn.OK

def cam_47(video_frame: VideoFrame):
    cam(video_frame, 47)
    return Gst.FlowReturn.OK

def cam_48(video_frame: VideoFrame):
    cam(video_frame, 48)
    return Gst.FlowReturn.OK

def cam_49(video_frame: VideoFrame):
    cam(video_frame, 49)
    return Gst.FlowReturn.OK

def cam_50(video_frame: VideoFrame):
    cam(video_frame, 50)
    return Gst.FlowReturn.OK

def cam_51(video_frame: VideoFrame):
    cam(video_frame, 51)
    return Gst.FlowReturn.OK

def cam_52(video_frame: VideoFrame):
    cam(video_frame, 52)
    return Gst.FlowReturn.OK

def cam_53(video_frame: VideoFrame):
    cam(video_frame, 53)
    return Gst.FlowReturn.OK

def cam_54(video_frame: VideoFrame):
    cam(video_frame, 54)
    return Gst.FlowReturn.OK

def cam_55(video_frame: VideoFrame):
    cam(video_frame, 55)
    return Gst.FlowReturn.OK

def cam_56(video_frame: VideoFrame):
    cam(video_frame, 56)
    return Gst.FlowReturn.OK

def cam_57(video_frame: VideoFrame):
    cam(video_frame, 57)
    return Gst.FlowReturn.OK

def cam_58(video_frame: VideoFrame):
    cam(video_frame, 58)
    return Gst.FlowReturn.OK

def cam_59(video_frame: VideoFrame):
    cam(video_frame, 59)
    return Gst.FlowReturn.OK

def cam_60(video_frame: VideoFrame):
    cam(video_frame, 60)
    return Gst.FlowReturn.OK

def cam_61(video_frame: VideoFrame):
    cam(video_frame, 61)
    return Gst.FlowReturn.OK

def cam_62(video_frame: VideoFrame):
    cam(video_frame, 62)
    return Gst.FlowReturn.OK

def cam_63(video_frame: VideoFrame):
    cam(video_frame, 63)
    return Gst.FlowReturn.OK