from flask import Flask, request, render_template, flash
import os
import atexit
import requests
from time import sleep
from pytz import timezone
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from confluent_kafka import Producer
from datetime import datetime, timedelta
from pytz import UTC
import db_handler
import sqlite3
from datetime import datetime
from logger import create_logger
from TAPPAS_WORKSPACE import tappas_workspace
from db_schema_manager import initialise_sqlite_db

# DB Initialisation MUST be above licensing import
DB_NAME = f"{tappas_workspace}/data/config.db"
initialise_sqlite_db(DB_NAME)

import kafka_handler
import config as cfg
import db_config as db_cfg
import NxAPI as nx
from gstream_driver import gStreamHandler
from threading import Thread
import app_cfg

logger, traces = create_logger("Flask_Server")
logger.info("Starting Flask_Server")

app = Flask(__name__)

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'color_detection'
INSTANCE_SEGMENTATION_SCRIPT = os.path.join(tappas_workspace, "apps/h8/gstreamer/general/instance_segmentation/instance_segmentation.sh")
CONFIG_FILE = os.path.abspath('./data/config.yaml')
pipeline_process = None

# add nx token to the database at start of the app
config = cfg.get_config()
nx_user = config.nxUser
nx_pass = config.nxPass
nx_ip = config.nxAddress
nx_port = config.nxPort
token = nx.get_token(nx_user, nx_pass, nx_ip, nx_port)

if token != "" and token != "invalidCred":
    db_handler.add_token(token)

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

if not os.path.exists('./data'):
    os.mkdir('./data')

# NOTE: sheduler may need to be shutdown using the shutdown method
# init scheduler
sched = BackgroundScheduler(timezone=UTC)
sched.start()

# Scheduler to get objects from Nx API
sched_obj = BackgroundScheduler(timezone=UTC)
sched_obj.start()

def server_shutdown():
    """mark as server shutdown
    """
    logger.info('Server shutting down')

# register server_shutdown function at the exit of app
atexit.register(server_shutdown)

class ThreadHandler:
    """Thread handler class to start / stop gstreamer pipeline
    """
    def __init__(self):
        self._running = False

    def terminate(self):
        self._running = False

    def run(self):
        self._running = True
        db_handler.reorder_rowids()
        gstreamer = gStreamHandler()
        
        total_streams = db_handler.get_active_source_count()
        
        logger.debug("Thread Handler starting Gstreamer Pipeline")
        gstreamer.build_pipeline(total_streams)
        gstreamer.start_pipeline()

        while self._running:
            sleep(1)

        logger.debug("Thread Handler shutting down Gstreamer Pipeline")
        gstreamer.stop_pipeline()
        
global handler
handler = ThreadHandler()

def source_factory(analytic_type: str) -> db_handler.SourceInterface:
    """Matches the analytic_type to a SourceInterface implementation, 
    instantiating the concrete class & returning it."""
    
    source: db_handler.SourceInterface = None
        
    # Source factory
    if analytic_type == "detection":
        source = db_handler.DetectSource()
        
    return source

def restart_pipeline(nx_port, nx_user, nx_pass, nx_ip):
    """restart gstreamer pipeline

    Args:
        nx_port (string): nx server port
        nx_user (string): nx server username
        nx_pass (string): nx server password
        nx_ip (_type_):  nx server ip address
    """
    try:
        global handler

        logger.info("Gstreamer Pipeline restart initiated")

        if handler._running:
            handler.terminate()
            atexit.unregister(handler.terminate)

        sleep(1)
        handler = ThreadHandler()

        db_handler.check_sources(nx_port, nx_user, nx_pass, nx_ip)

        t = Thread(target=handler.run)
        atexit.register(handler.terminate)
        t.start()
    except BaseException as b:
        logger.error(f"Failed to restart pipeline: {b}", exc_info=traces)

@app.route("/")
def index():
    """render homepage template on root route

    Returns:
        html template: homepage.html
    """
    logger.debug(f"Route {request.method} \'/\' called")
    return render_template('homepage.html')

@app.route('/start_detection', methods=["GET"])
def start_detection():
    """start analytics

    Returns:
        string: "OK" message
    """
    logger.debug(f"Route {request.method} \'/start_detection\' called")
    config = cfg.get_config()
    nx_user = config.nxUser
    nx_pass = config.nxPass
    nx_ip = config.nxAddress
    nx_port = config.nxPort

    db_handler.check_sources(nx_port, nx_user, nx_pass, nx_ip)

    t = Thread(target=handler.run)
    atexit.register(handler.terminate)
    t.start()
    
    return "OK"

# for testing purposes
@app.route('/stop_detection', methods=["GET"])
def stop_detection():
    """ stop analytics

    Returns:
        string: blank string
        integer: 200 response code
    """
    logger.debug(f"Route {request.method} \'/stop_detection\' called")
    global handler

    handler.terminate()
    atexit.unregister(handler.terminate)
    
    sleep(1)
    handler = ThreadHandler()

    return "", 200

@app.route('/remove_source', methods=["POST"])
def remove_source():
    """remove rtsp sources when associted plugin is turned off

    Returns:
        string: blank string
        integer: 200 response code
    """
    logger.debug(f"Route {request.method} \'/remove_source\' called")
    res = request.get_json()
    camera_id = res["camera_id"]
    analytic_type = res["analytic_type"]
    
    source = source_factory(analytic_type)

    db_handler.remove_source(camera_id, source)

    config = cfg.get_config()
    nx_user = config.nxUser
    nx_pass = config.nxPass
    nx_ip = config.nxAddress
    nx_port = config.nxPort

    logger.info(f"Schdueling pipeline restart at: {datetime.now(tz=UTC) + timedelta(seconds=5)}")
    sched.add_job(
        restart_pipeline,
        'date',
        args = [nx_port, nx_user, nx_pass, nx_ip], 
        run_date = datetime.now(tz=UTC) + timedelta(seconds=5), 
        id = 'restart_pipeline_1', 
        max_instances = 1,
        replace_existing = True # removed manual check
    )

    return "", 200

@app.route('/objects', methods=["POST"])
def get_objects():
    """to capture selected object event from plugin

    Returns:
        string: blank string
        integer: 200 response code
    """
    logger.debug(f"Route {request.method} \'/objects\' called")
    try:
        res = request.get_json()
        db_handler.add_objects(res)

    except BaseException as b:
        logger.error(f"Failed to add new objects: {b}", exc_info=traces)
        pass

    # schedule pipeline to restart
    logger.debug(f"Schdueling add objects at: {datetime.now(tz=UTC) + timedelta(seconds=1)}")
    sched_obj.add_job(
        db_handler.add_objects,
        'date',
        args = [res], 
        run_date = datetime.now(tz=UTC) + timedelta(seconds=1), 
        id = 'add_objects_1', 
        max_instances = 1,
        replace_existing = True # removed manual check
    )
    
    logger.info(f"Schdueling pipeline restart at: {datetime.now(tz=UTC) + timedelta(seconds=5)}")
    sched.add_job(
        restart_pipeline,
        'date',
        args = [nx_port, nx_user, nx_pass, nx_ip], 
        run_date = datetime.now(tz=UTC) + timedelta(seconds=5), 
        id = 'restart_pipeline_1', 
        max_instances = 1,
        replace_existing = True # removed manual check
    )

    return "", 200

@app.route('/add_source', methods=["POST"])
def add_source():
    """Retrieve rtsp source data from detect plugin
        and add them to the database

    Returns:
        string: blank string
        integer: 200 response code
    """
    logger.debug(f"Route {request.method} \'/add_source\' called")

    config = cfg.get_config()
    nx_user = config.nxUser
    nx_pass = config.nxPass
    nx_ip = config.nxAddress
    nx_port = config.nxPort

    # request needs to have 'Content-Type: application/json' in header
    try:
        res = request.get_json()
        kafka_topic = res["kafka_topic"]
        camera_id = res["camera_id"]
        analytic_type = res["analytic_type"]

        kafka_handler.create_topic(kafka_topic)
        
        source = source_factory(analytic_type)

        # get source codec type for both primary and seconday streams 
        prim_codec, sec_codec = nx.get_device_codec(nx_port, nx_user, nx_pass, nx_ip, camera_id)
        source_type = prim_codec # taking primary stream as source

        rtsp_address = f"rtsp://{nx_user}:{nx_pass}@{nx_ip}:{nx_port}/{camera_id}?onvif_replay=true"
    
        db_handler.add_source(source, kafka_topic, rtsp_address, camera_id, source_type)
    
    except BaseException as b:
        logger.error(f"Failed to add new source: {b}", exc_info=traces)

    # schedule pipeline to restart
    logger.debug(f"Schdueling Gstreamer Pipeline restart at: {datetime.now(tz=UTC) + timedelta(seconds=5)}")
    sched.add_job(
        restart_pipeline,
        'date',
        args = [nx_port, nx_user, nx_pass, nx_ip], 
        run_date = datetime.now(tz=UTC) + timedelta(seconds=5), 
        id = 'restart_pipeline_1', 
        max_instances = 1,
        replace_existing = True # removed manual check
    )

    return "", 200
    
@app.route('/heartbeat_camera', methods=["GET", "POST"])
def heartbeat_camera():
    """retrieve rtsp source data from plugin on every hearbeat
        and update the database

    Returns:
        string: blank string
        integer: 200 response code
    """

    res = request.get_json()
    kafka_topic = res["kafka_topic"]
    camera_id = res["camera_id"]
    analytic_type = res["analytic_type"]
    
    source = source_factory(analytic_type)
        
    kafka_handler.create_topic(kafka_topic)
        
    logger.debug(f"Route {request.method} \'/heartbeat_camera\' called by CamID: {camera_id}")

    config = cfg.get_config()
    nx_user = config.nxUser
    nx_pass = config.nxPass
    nx_ip = config.nxAddress
    nx_port = config.nxPort
    rtsp_address = f"rtsp://{nx_user}:{nx_pass}@{nx_ip}:{nx_port}/{camera_id}?onvif_replay=true"
    
    health = db_handler.heartbeat_source(source, kafka_topic, rtsp_address, camera_id)

    global handler
    if not handler._running:
        logger.info(f"Heartbeat detected that Gstreamer Pipeline was not running")

    if not health or not handler._running:
        # schedule pipeline to restart
        logger.debug(f"Schdueling Gstreamer Pipeline restart at: {datetime.now(tz=UTC) + timedelta(seconds=5)}")
        sched.add_job(
            restart_pipeline,
            'date',
            args=[nx_port, nx_user, nx_pass, nx_ip], 
            run_date = datetime.now(tz=UTC) + timedelta(seconds=5), 
            id='restart_pipeline_1', 
            max_instances=1,
            replace_existing=True # removed manual check
        )

    return "", 200

@app.route('/config', methods=["GET", "POST"])
def config():
    """received nx config parameters from user and save in a yaml file

    Returns:
        html template: 'config.html'
    """
    logger.debug(f"Route {request.method} \'/config\' called")
    conf = cfg.get_config()

    if request.method == 'POST':
        if request.form.get('nxuser') != "":
            conf.nxUser = request.form.get('nxuser')
        if request.form.get('nxpass') != "":
            conf.nxPass = request.form.get('nxpass')
        if request.form.get('nxserveraddress') != "":
            conf.nxAddress = request.form.get('nxserveraddress')
        if request.form.get('nxserverport') != "":
            conf.nxPort = request.form.get('nxserverport')
        if request.form.get('confidence') != "0.80":
            conf.confidence = request.form.get('confidence')
       
            
        # validating nx config setting
        error_msg = nx.validateConfig(conf.nxUser, conf.nxPass, conf.nxAddress, conf.nxPort)
        
        # error occurs, pop it on gui
        if error_msg != "":
            logger.error(f"Config error: {error_msg}")
            flash(error_msg, 'error')
        # if no error, save to the database and show successfull message 
        else:        
            cfg.save_config(conf)
            
            logger.info("Config changes saved successfully")
            flash("Config changes saved successfully", 'success')

        config = cfg.get_config()
        nx_user = config.nxUser
        nx_ip = config.nxAddress
        nx_port = config.nxPort
        nx_pass = config.nxPass     

        # update nx session token into database
        token = nx.get_token(nx_user, nx_pass, nx_ip, nx_port)

        if token != "" and token != "invalidCred":
            db_handler.add_token(token)

        # update nx session token into database
        token = nx.get_token(nx_user, nx_pass, nx_ip, nx_port)

        if token != "" and token != "invalidCred":
            db_handler.add_token(token)

        logger.debug(f"Schdueling Gstreamer Pipeline restart at: {datetime.now(tz=UTC) + timedelta(seconds=5)}")
        sched.add_job(
            restart_pipeline,
            'date',
            args = [nx_port, nx_user, nx_pass, nx_ip],
            run_date = datetime.now(tz=UTC) + timedelta(seconds=5),
            id = 'restart_pipeline_1', 
            max_instances = 1,
            replace_existing = True # removed manual check
        )

    return render_template('config.html', config=conf)


@app.route('/db-config', methods=["GET", "POST"])
def db_config():
    """received db config parameters from user and save in a sqlitebase

    Returns:
        html template: 'dbConfig.html'
    """
    logger.debug(f"Route {request.method} \'/db-config\' called")
    
    db_conf = db_cfg.get_config()

    if request.method == 'POST':
        if request.form.get('checkbox') != "":
            db_conf.checkbox = request.form.get('checkbox')          
            if (db_conf.checkbox == '1'):
                db_conf.checkbox = 1
            else:
                db_conf.checkbox = 0

        if request.form.get('hostname') != "":
            db_conf.hostname = request.form.get('hostname')            

        if request.form.get('dbname') != "":
            db_conf.dbname = request.form.get('dbname')

        if request.form.get('tablename') != "":
            db_conf.tablename = request.form.get('tablename')

        db_conf.old_column_name = request.form.getlist('old_column_name[]')
       
        db_conf.old_data_type = request.form.getlist('old_data_type[]')

        db_conf.associated_column_name = request.form.getlist('associated_column_name[]')        

        if request.form.get('username') != "":
            db_conf.username = request.form.get('username')

        if request.form.get('password') != "":
            db_conf.password = request.form.get('password')

        result_db = db_handler.error_custom_mysql_cursor(db_conf.hostname, db_conf.dbname, db_conf.username, db_conf.password)
        result_table = db_handler.error_custom_mysql_table(db_conf.hostname, db_conf.dbname, db_conf.username, db_conf.password, db_conf.tablename)
        
        if db_conf.checkbox == 1 and (result_db == 500 or result_table == 500):
            db_conf.checkbox = 0
            flash("Invalid database connection. We assume your database was already created with provided table name and credential.", 'error')
            
        else:
            logger.info("DB Config changes saved successfully")
            flash("DB Config settings have been saved successfully", 'success')

            if db_conf.checkbox == 0:
                db_conf.hostname = ""
                db_conf.dbname = ""
                db_conf.tablename = ""
                db_conf.old_column_name = ""
                db_conf.old_data_type = ""
                db_conf.associated_column_name = ""
                db_conf.username = ""
                db_conf.password = ""

            db_cfg.save_config(db_conf)
            

        config = cfg.get_config()
        nx_user = config.nxUser
        nx_ip = config.nxAddress
        nx_port = config.nxPort
        nx_pass = config.nxPass

        # update nx session token into database
        token = nx.get_token(nx_user, nx_pass, nx_ip, nx_port)

        if token != "" and token != "invalidCred":
            db_handler.add_token(token)

        logger.debug(f"Schdueling Gstreamer Pipeline restart at: {datetime.now(tz=UTC) + timedelta(seconds=5)}")
        sched.add_job(
            restart_pipeline,
            'date',
            args = [nx_port, nx_user, nx_pass, nx_ip],
            run_date = datetime.now(tz=UTC) + timedelta(seconds=5),
            id = 'restart_pipeline_1', 
            max_instances = 1,
            replace_existing = True # removed manual check
        )

    return render_template('dbConfig.html', config=db_conf)


@app.route('/restart_detection', methods=["GET"])
def restart_detection():
    """restart gstreamer pipeline

    Returns:
       string: blank string
       integer: 200 response code
    """
    logger.debug(f"Route {request.method} \'/restart_detection\' called")
    global handler

    config = cfg.get_config()
    nx_user = config.nxUser
    nx_pass = config.nxPass
    nx_ip = config.nxAddress
    nx_port = config.nxPort

    logger.debug(f"Schdueling Gstreamer Pipeline restart at: {datetime.now(tz=UTC) + timedelta(seconds=5)}")
    sched.add_job(
        restart_pipeline,
        'date',
        args = [nx_port, nx_user, nx_pass, nx_ip],
        run_date = datetime.now(tz=UTC) + timedelta(seconds=5),
        id = 'restart_pipeline_1', 
        max_instances = 1,
        replace_existing = True # removed manual check
    )

    return "", 200

 # restart the pipeline when it halted
def gst_health_check():
    try:
        logger.info(f"Schdueling gst health check restart at: {datetime.now(tz=UTC)}")

        if db_handler.table_exists(DB_NAME, 'Gst_Health'):
            connection = sqlite3.connect(DB_NAME)
            cursor = connection.cursor()
            cursor.execute("SELECT * from Gst_Health")        
            data_row = cursor.fetchone()
            cursor.close()
            connection.close()

            if data_row:
                current_time = datetime.now(tz=UTC)
                last_execution_time_str = data_row[0]
                last_execution_time = datetime.strptime(last_execution_time_str, '%Y-%m-%d %H:%M:%S.%f%z')
                delay = current_time - last_execution_time                

                if delay > timedelta(minutes=6): # wait time before restarting the pipeline
                    logger.info(f"The pipeline has been frozen for the last {delay} minutes.")
                    url = "http://localhost:47760/restart_detection"
                    response = requests.get(url, verify=False)
            else:
                logger.debug(f"No entry found in gst health table.")
        
    except BaseException as b:
        logger.error(f"Failed to restart pipeline on crashing: {b}", exc_info=traces)

# gst health check scheduler
sched_gst_health = BackgroundScheduler(timezone=UTC)
freq_gen_event = 5 # minutes
sched_gst_health.add_job(
    gst_health_check,
    'interval',
    minutes=freq_gen_event,
    max_instances=1
    )
sched_gst_health.start()


# Create a scheduler with a background thread
scheduler = BackgroundScheduler()

# Schedule the function to run with the defined trigger
db_conf = db_cfg.get_config()
logger.info(f"type of db_conf.checkbox = {db_conf.checkbox}")
if db_conf.checkbox == 1:
    scheduler.add_job(db_handler.push_to_custom_sql, 'cron', minute='*/15')
    logger.info("custom db color initiated ...")
else:
    scheduler.add_job(db_handler.push_to_sql, 'cron', minute='*/15')
    logger.info("default db color initiated ...")
scheduler.start()


# driver function
if __name__ == "__main__":
    app_config = app_cfg.Settings()
    app_config.read_config()
    # port should default to 47760
    port = app_config.server_port
    logger.info(f"Flask Server is running on port: {port}")
    app.run(debug=True, use_reloader=False, host="0.0.0.0", port=port)
