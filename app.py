from flask import Flask, request, jsonify
import subprocess
import os
import atexit
from apscheduler.schedulers.background import BackgroundScheduler
from confluent_kafka import Producer
import yaml
import json
from datetime import datetime

app = Flask(__name__)

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'color_detection'
# Path to the TAPPAS workspace
TAPPAS_WORKSPACE = "/local/workspace/tappas"
 
# Path to the instance segmentation script
INSTANCE_SEGMENTATION_SCRIPT = os.path.join(TAPPAS_WORKSPACE, "apps/h8/gstreamer/general/instance_segmentation/instance_segmentation.sh")
CONFIG_FILE = os.path.abspath('./data/config.yaml')
pipeline_process = None

# Kafka producer setup
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

# Ensure data directory exists
if not os.path.exists('./data'):
    os.mkdir('./data')

class Settings:
    def __init__(self):
        self.username = ""
        self.password = ""
        self.nxip = '127.0.0.1'
        self.nxport = '7001'
        self.nxuser = ""
        self.nxpassword = ""
        self.useKafka = True
        self.kafkaIP = "127.0.0.1"
        self.kafkaPort = 9092

    def toDict(self):
        return {
            "username": self.username,
            "password": self.password,
            "nxip": self.nxip,
            "nxport": self.nxport,
            "nxuser": self.nxuser,
            "nxpassword": self.nxpassword,
            "useKafka": self.useKafka,
            "kafkaIP": self.kafkaIP,
            "kafkaPort": self.kafkaPort
        }

    def tryParse(self, settings):
        try:
            self.username = settings["username"]
            self.password = settings["password"]
            self.nxip = settings["nxip"]
            self.nxport = settings["nxport"]
            self.nxuser = settings["nxuser"]
            self.nxpassword = settings["nxpassword"]
            self.useKafka = settings["useKafka"]
            self.kafkaIP = settings["kafkaIP"]
            self.kafkaPort = settings["kafkaPort"]
        except BaseException as e:
            print("Parse settings error: " + str(e))
            return False
        return True

    def readConfig(self):
        try:
            if not os.path.exists(CONFIG_FILE):
                self.saveSettings()
            with open(CONFIG_FILE) as f:
                data = yaml.load(f, Loader=yaml.FullLoader)
                return self.tryParse(data)
        except BaseException as ex:
            print(f"Load settings error: {ex}")
            return False
        return True

    def saveSettings(self):
        try:
            with open(CONFIG_FILE, 'w+') as f:
                yaml.dump(data=self.toDict(), stream=f)
        except BaseException as e:
            print("Save settings error: " + str(e))
            return False
        return True

def getNxRtspUrl(nxCamId):
    rtspUrl = ""
    try:
        nxCamId = str(nxCamId).replace('{', '').replace('}', '')
        appSettings = Settings()
        ok = appSettings.readConfig()
        if not ok:
            raise BaseException("Read settings failed")

        ip = appSettings.nxip
        port = appSettings.nxport

        rtspUrl = f"rtsp://{ip}:{port}/{nxCamId}?onvif_replay=1"

    except BaseException as e:
        print(f"Request error: {e}")

    return rtspUrl

@app.route("/")
def index():
    return "Instance Segmentation API"

@app.route('/start_detection', methods=["POST"])
def start_detection():
    global pipeline_process

    nxCamId = request.json.get('nx_cam_id', None)
    if nxCamId:
        video_source = getNxRtspUrl(nxCamId)
    else:
        video_source = request.json.get('video_source', '/path/to/default/video.mp4')

    show_fps = request.json.get('show_fps', False)
    args = [INSTANCE_SEGMENTATION_SCRIPT, '-i', video_source]

    if show_fps:
        args.append('--show-fps')

    try:
        pipeline_process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return jsonify({"message": "Pipeline started", "pid": pipeline_process.pid})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/stop_detection', methods=["POST"])
def stop_detection():
    global pipeline_process

    if pipeline_process:
        pipeline_process.terminate()
        pipeline_process.wait()
        pipeline_process = None
        return jsonify({"message": "Pipeline stopped"})
    else:
        return jsonify({"error": "No pipeline running"}), 400

@app.route('/heartbeat', methods=['GET', 'POST'])
def heartbeat():
    reply = {"result": ""}
    current_time = datetime.now()
    try:
        print(f"heartbeat called at: {current_time}")

        if request.method == "POST":
            nxCamId = request.args.get("nxcam_id")
            heartbeatLog = f'/tmp/heartbeat{nxCamId}.log'
            if os.name == 'nt':
                heartbeatLog = f"C:\\temp\\heartbeat{nxCamId}.log"

            now = datetime.now()
            with open(heartbeatLog, 'w') as f:
                f.write((str(now.timestamp())))

    except BaseException as e:
        reply["result"] = f"Request error: {e}"
        print(f"Request error: {e}")
    timenow = datetime.now()
    print(f"heartbeat completed at: {timenow}")
    return jsonify(reply)

@app.route('/nxrtspurl', methods=['GET', 'POST'])
def nxrtspurl_route():
    reply = {"result": ""}
    try:
        if request.method in ["POST", "GET"]:
            nxCamId = request.args.get("nxcam_id")
            nxCamId = str(nxCamId).replace('{', '').replace('}', '')
            reply["result"] = getNxRtspUrl(nxCamId=nxCamId)

    except BaseException as e:
        reply["result"] = f"Request error: {e}"
        print(f"Request error: {e}")

    return jsonify(reply)

def shutdown_server():
    if pipeline_process:
        pipeline_process.terminate()
        pipeline_process.wait()

atexit.register(shutdown_server)

# Scheduler for periodic tasks
scheduler = BackgroundScheduler()
scheduler.start()

if __name__ == "__main__":
    app.run(debug=True, use_reloader=False, host="127.0.0.1", port=5000)
