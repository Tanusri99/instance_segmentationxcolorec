import requests
import urllib3
import sqlite3
import datetime

from TAPPAS_WORKSPACE import tappas_workspace
from logger import create_logger
import config as cfg
import db_handler as db

global session_count
session_count = 0

urllib3.disable_warnings()

logger, traces = create_logger("NxAPI_Handler")

def get_token(server_user: str, server_password: str, server_ip: str, port: str) -> str:
    """get Nx session token to authorize further API calls

    Args:
        server_user (str): nx server username
        server_password (str): nx server password
        server_ip (str): nx server ip addresss
        port (str): nx server port

    Returns:
        str: token
    """
    bearerToken =""
    try:
        if server_ip:
            data = {
                "username": server_user,
                "password": server_password
            }
            url = f"https://{server_ip}:{port}/rest/v1/login/sessions"
            response = requests.post(url, json=data, headers={"Content-Type": "application/json"}, verify=False)
            
            # ok response
            if response.status_code == 200:
                jsondata = response.json()
                bearerToken = jsondata["token"]
                # add token session to the database
                logger.debug("Successfully received Nx Witness API session token")
                return bearerToken
            else:
                bearerToken = "invalidCred"
                logger.warning(f"Failed to receive Nx Witness API session token with status code: {response.status_code}")
                return bearerToken

    except BaseException as b:
        logger.error(f"Error fetching Nx Witness API session token: {b}", exc_info=traces) 
        pass

    return bearerToken

def validateConfig(server_user:str, server_password:str, server_ip:str, port:str) -> str:
    """checks for any error on nx config

    Args:
        server_user (str): nx server username
        server_password (str): nx server password
        server_ip (str): nx server ip addresss
        port (str): nx server port

    Returns:
        str: error message
    """
    errorMsg = ""
    db.fetch_updated_token(server_user, server_password, server_ip, port)
    bearerToken = get_token(server_user, server_password, server_ip, port)

    if bearerToken == "":
        errorMsg = "Invalid Nx IP address or port!"
    
    if bearerToken == "invalidCred":
        errorMsg = "Invalid Nx username or password!"

    return errorMsg

def get_device_status(port: str, server_user: str, server_password: str, server_ip: str, cam_id: str):
    """Sends a http request to receive a camera's status.
    
    Args:
        port (str): port
        server_user (str): user of nx server
        server_password (str): password of nx server
        server_ip (str): ip address of nx server
        cam_id (str): NX camera id
        
    Returns:
        str: status of camera (possible options include, 'Online', 'Recording', 'Offline')
    """
    #token = get_token(server_user, server_password, server_ip, port)
    status = None

    if server_ip:    
        token = db.fetch_updated_token(server_user, server_password, server_ip, port)
        
        url = f"https://{server_ip}:{port}/rest/v1/devices/{cam_id}/status"
        
        headers = {
            "accept" : "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }

        try:
            response = requests.get(url, verify=False, headers=headers)
            # update token if not valid
            if response.status_code != 200:
                response = get_updated_response(url, token, server_user, server_password, server_ip, port)
                
        except BaseException as b:
            logger.error(f"Failed to query status for device with CamID-{cam_id} : {b}", exc_info=traces)
        
        try:
            if "status" in response.json():
                status = response.json()["status"]
        except BaseException as b:
            status = None
            logger.warning(f"Device with CamID-{cam_id} failed to report a valid status: {b}", exc_info=traces)
    
    return status

def get_device_codec(port: str, server_user: str, server_password: str, server_ip: str, cam_id: str):
    """get streams video codec type (H264 / H265) from both primary

    Args:
        port (str): _description_
        server_user (str): _description_
        server_password (str): _description_
        server_ip (str): _description_
        cam_id (str): _description_

    Returns:
        tuple: codec type (primary stream), codec type (secondary stream)
    """
    primary_stream_codec = ''
    secondary_stream_codec = ''
    primary_stream = {}
    secondary_stream = {}

    if server_ip:
        #token = get_token(server_user, server_password, server_ip, port)
        token = db.fetch_updated_token(server_user, server_password, server_ip, port)
        
        url = f"https://{server_ip}:{port}/rest/v1/devices/{cam_id}"
        
        headers = {
            "accept" : "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }
        
        try:
            response = requests.get(url, verify=False, headers=headers)
            # update token if not valid
            if response.status_code != 200:
                response = get_updated_response(url, token, server_user, server_password, server_ip, port)
            try:
                streams = response.json()['parameters']['mediaStreams']['streams']
                    
                if len(streams) > 2:
                    for stream in streams:
                        if 'encoderIndex' in stream:
                            if stream['encoderIndex'] == 1:
                                secondary_stream = stream
                        else:
                            primary_stream = stream
                    logger.debug(f"Total of {len(streams)} Nx streams found for device with CamID: {cam_id}")
                    codec_primary_stream = primary_stream['codec']
                    codec_secondary_stream = secondary_stream['codec']

                elif len(streams) <= 2:
                    for stream in streams:
                        if 'encoderIndex' not in stream:
                            primary_stream = stream
                    logger.warning(f"Less than 3 Nx streams for device found, assuming only primary stream. CamID: {cam_id}")
                    codec_primary_stream = primary_stream['codec']
                    codec_secondary_stream = ""

                if int(codec_primary_stream) == 27:
                    primary_stream_codec = 'H264'  
                if int(codec_primary_stream) == 173:
                    primary_stream_codec = 'H265'
                if int(codec_primary_stream) ==  7:
                    primary_stream_codec = "MJPEG"

                if int(codec_secondary_stream) == 27:
                    secondary_stream_codec = 'H264'
                if int(codec_secondary_stream) == 173:
                    secondary_stream_codec = 'H265'   
                if int(codec_secondary_stream) == 4:
                    secondary_stream_codec = 'H263'
        
            except BaseException as b:
                logger.error(f"Failed to fetch stream info from Nx API device record: {b}", exc_info=traces)
                logger.warning("Please check your Nx settings in the Server config page")

        except BaseException as b:
            logger.error(f"Failed to fetch device record from Nx API: {b}", exc_info=traces)
            logger.warning("Please check your Nx settings in the Server config page")
    
    return primary_stream_codec, secondary_stream_codec

def get_camera_name(cam_id:str) -> str:
    """get camera name from camera id

    Args:
        cam_id (str): camera id

    Returns:
        str: camera name
    """
    camera_name = ""
    
    config = cfg.get_config()
    nx_user = config.nxUser
    nx_pass = config.nxPass
    nx_ip = config.nxAddress
    nx_port = config.nxPort

    if nx_ip:  
        #token = get_token(nx_user, nx_pass, nx_ip, nx_port)
        token = db.fetch_updated_token(nx_user, nx_pass, nx_ip, nx_port)
        
        if token == "":
            logger.error(f"Wrong Nx auth token, please check Nx cedentials.")
        
        url = f"https://{nx_ip}:{nx_port}/rest/v1/devices/{cam_id}"

        headers = {
            "accept" : "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }
        try:
            response = requests.get(url, verify=False, headers=headers)
            # update token if not valid
            if response.status_code != 200:
                response = get_updated_response(url, token, nx_user, nx_pass, nx_ip, nx_port)
            camera_name = response.json()["name"]
            
        except BaseException as b:
            logger.error(f"Failed to fetch camera name from nx api: {b}", exc_info=traces)
        
    return camera_name

def get_confidence_threshold(cam_id:str):
    """fetch confidence threshold from plugin setting via nx api

    Args:
        cam_id (str): camera is

    Returns:
        tuple: detect threshold
    """
    detect_con_threshold = 0.4
    device_agent_settings_values = []

    config = cfg.get_config()
    server_user = config.nxUser
    server_password = config.nxPass
    server_ip = config.nxAddress
    server_port = config.nxPort

    if server_ip:
        #token = get_token(server_user, server_password, server_ip, server_port)
        token = db.fetch_updated_token(server_user, server_password, server_ip, server_port)

        if token == "":
            logger.error(f"Wrong Nx auth token, please check Nx cedentials.")
        
        url = f"https://{server_ip}:{server_port}/rest/v1/devices/{cam_id}?_with=parameters.deviceAgentsSettingsValuesProperty.value"

        headers = {
            "accept" : "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }
        try:
            response = requests.get(url, verify=False, headers=headers)

            if response.status_code != 200:
                response = get_updated_response(url, token, server_user, server_password, server_ip, server_port)

            if "parameters" in response.json():
                device_agent_settings_values = response.json()["parameters"]["deviceAgentsSettingsValuesProperty"]

            for val in device_agent_settings_values:
                # get detect confidence threshold
                if 'value' in val:
                    if "nx.aol_color_detection_plugin.confidenceThreshold" in val["value"]:
                        detect_con_threshold_str = val["value"]["nx.aol_color_detection_plugin.confidenceThreshold"]
                        detect_con_threshold = float(detect_con_threshold_str)

        except BaseException as b:
            logger.error(f"Failed to fetch confidence threshold from plugin using Nx API: {b}", exc_info=traces)
    
    return detect_con_threshold
    ######## GET CONF END ########
   
def get_selected_objects(cam_id:str):
    """fetch list of selected objects from nx plugin

    Args:
        cam_id (str): camera is

    Returns:
        tuple: detect_objects, count_objects
    """
    try:
        detect_objects = []
        count_objects = []
        device_agent_settings_values = []
        
        config = cfg.get_config()
        server_user = config.nxUser
        server_password = config.nxPass
        server_ip = config.nxAddress
        server_port = config.nxPort

        if server_ip:
            #token = get_token(server_user, server_password, server_ip, server_port)
            token = db.fetch_updated_token(server_user, server_password, server_ip, server_port)

            if token == "":
                logger.error("Wrong Nx auth token, please check Nx cedentials.")

            url = f"https://{server_ip}:{server_port}/rest/v1/devices/{cam_id}"

            headers = {
                "accept" : "application/json",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}"
            }
            
            response = requests.get(url, verify=False, headers=headers)

            if response.status_code != 200:
                response = get_updated_response(url, token, server_user, server_password, server_ip, server_port)

            if "parameters" in response.json():
                device_agent_settings_values = response.json()["parameters"]["deviceAgentsSettingsValuesProperty"]
        
            # detection plugin
            for val in device_agent_settings_values:
                if 'value' in val:
                    if "nx.aol_color_detection_plugin.confidenceThreshold" in val["value"]:
                        for key in val["value"]:
                            if key.startswith('enable'):
                                # strip enable off the key to get object names
                                detect_objects.append(key.replace('enable', ''))
            
    except BaseException as b:
        logger.error(f"Failed to fetch selected object list from NX API for camera {cam_id}: {b}", exc_info=traces) 

    return detect_objects, count_objects


"""
update generic API response  with updated token
"""
def get_updated_response(url, expired_token, nx_user, nx_pass, nx_ip, nx_port):
    db.update_token(expired_token)
    token = db.fetch_updated_token(nx_user, nx_pass, nx_ip, nx_port)
    headers = {
        "accept" : "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    response = requests.get(url, verify=False, headers=headers)
    return response