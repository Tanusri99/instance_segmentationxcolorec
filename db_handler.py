import sqlite3
import json
import mysql.connector
import uuid
import datetime
from abc import ABC, abstractmethod

import config as cfg
import db_config as db_cfg
import NxAPI as nx
from app_cfg import Settings
from logger import create_logger
from TAPPAS_WORKSPACE import tappas_workspace

logger, traces = create_logger("DB_Handler")
config = Settings()

DB_NAME = f"{tappas_workspace}/data/config.db"

def get_sqlite_cursor():
    """sqlite database connection set up

    Returns:
        sqlite object: mysql cursor
        sqlite object: mysql connection
    """
    connection = sqlite3.connect(DB_NAME) 
    cursor = connection.cursor()
    return cursor, connection
    

def get_mysql_cursor():
    """MySQL cloud database for COLOR_DETECTION connection set up

    Returns:
        mysql object: mysql cloud cursor
        mysql object: mysql cloud connection
    """
    config = Settings()
    config.read_config()
    
    # SQL set up sql cloud connection
    connection = mysql.connector.connect(
        host = config.mysql_hostname,
        user = config.mysql_user,
        password = config.mysql_pass,
        database = config.mysql_dbname
    )
    cursor = connection.cursor()   
    return cursor, connection

# custom database
def get_custom_mysql_cursor():
    """MySQL cloud database for COLOR_DETECTION connection set up

    Returns:
        mysql object: mysql cloud cursor
        mysql object: mysql cloud connection
    """
    try:
        db_conf = db_cfg.get_config()

        hostname = db_conf.hostname
        dbname = db_conf.dbname    
        username = db_conf.username
        password = db_conf.password
        
        # SQL set up sql cloud connection
        connection = mysql.connector.connect(
            host = hostname,
            user = username,
            password = password,
            database = dbname
        )
        cursor = connection.cursor()   
        return cursor, connection
    except mysql.connector.Error as err:
        logger.error(f"Failed to establish custom sql db connection: {err}", exc_info=traces)


# error handling at ui for custom database
def error_custom_mysql_cursor(hostname, dbname, username, password):
    """MySQL cloud database for COLOR_DETECTION connection set up

    Returns:
        mysql object: mysql cloud cursor
        mysql object: mysql cloud connection
    """
    try:
        hostname = hostname
        dbname = dbname
        username = username
        password = password
        
        # SQL set up sql cloud connection
        connection = mysql.connector.connect(
            host = hostname,
            user = username,
            password = password,
            database = dbname
        )
        cursor = connection.cursor()   
        return cursor, connection
    except mysql.connector.Error as err:
        logger.error(f"Failed to establish custom sql db connection: {err}", exc_info=traces)
        return 500
 
def error_custom_mysql_table(hostname, dbname, username, password,tablename):
   
    # Establish a connection to the MySQL database
    try:
        connection = mysql.connector.connect( 
            host = hostname,
            user = username,
            password = password,
            database = dbname)

        # Create a cursor to execute SQL queries
        cursor = connection.cursor()

        # Execute the query to check if the table exists
        table_name_to_check = tablename
        query = f"SHOW TABLES LIKE '{table_name_to_check}'"
        cursor.execute(query)

        # Fetch the result
        result = cursor.fetchone()

        # Check if the result exists (table exists)
        if result:
            return 200
        else:
            return 500

        # Close the cursor and connection
        cursor.close()
        connection.close()

    except mysql.connector.Error as error:
        print(f"Error: {error}")


def get_mysql_checkin_cursor():
    """MySQL cloud database for ADMIN connection set up

    Returns:
        mysql object: mysql cloud cursor
        mysql object: mysql cloud connection
    """
    config = Settings()
    config.read_config()
    
    # SQL set up sql cloud connection
    connection = mysql.connector.connect(
        host = config.mysql_hostname,
        user = config.mysql_user,
        password = config.mysql_pass,
        database = "ADMIN"
    )
    cursor = connection.cursor()   
    return cursor, connection

def get_object_name(object_type: str) -> str:
    """get object name (person) from object type (nx.aol_color_detection.person)

    Args:
        object_type (str): object type as received from nx API

    Returns:
        str: object name
    """
    object_name = ""
    if object_type != "":
        object_name = object_type.split('.')[2]

    return object_name

def add_event(event_data: dict):
    """Adds a color event in the sql cloud database.

    Args:
        event_data (dict): color metadata
    """  
    try:
        camera_id = event_data["camera_id"]
        camera_name = event_data["camera_name"]
        object_type = event_data["object_type"]
        event_type = event_data["event_type"]
        event_label = event_data["event_label"]
        event_time = event_data["timestamp"] 
               
        
        cursor, connection =  get_sqlite_cursor()
        query =  "INSERT INTO Color \
                        (event_id, hw_id, camera_id, camera_name, object_type, \
                        event_type, event_label, event_time ) \
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)" 
                        
        param = (camera_id, camera_name, object_type, event_type, event_label, event_time)  
        cursor.execute(query, param)
        connection.commit()
        cursor.close()
        connection.close()

    except BaseException as b:
        logger.error(f"Failed to store event data into SQLite 'Color' table: {b}", exc_info=traces)

def get_active_source_count() -> int:
    """Returns the number of sources with active analytics."""
    cursor, connection = get_sqlite_cursor()
    
    cursor.execute(f"SELECT COUNT(*) FROM Sources WHERE detect_flag=1;")
    active_source_count = cursor.fetchone()[0]
    
    cursor.close()
    connection.close()
    
    return active_source_count   

def remove_conflicting_kafka_topics(kafka_topic_sql_column: str, kafka_topic: str, camera_id: str) -> list:
    """Remove entries that have the specified kafka topic aside from the entry with the specified camera_id. 

    Args:
        kafka_topic_sql_column (str): the sql column of the desired kafka topic
        kafka_topic (str): the kafka topic
        camera_id (str): the camera id of the entry to be kept

    Returns:
        list: camera ids of entries that were removed
    """
    cursor, connection =  get_sqlite_cursor()
    
    cursor.execute(f"SELECT camera_id FROM Sources WHERE {kafka_topic_sql_column}='{kafka_topic}' AND camera_id!='{camera_id}' ")
    removed_cameras = cursor.fetchall()
    
    cursor.execute(f"DELETE FROM Sources WHERE {kafka_topic_sql_column}='{kafka_topic}' AND camera_id!='{camera_id}' ")
    
    logger.debug(f"remove_conflicting_kafka_topics: kafka_topic conflict, deleted database entries for '{removed_cameras}' ")
    
    connection.commit()
    cursor.close()
    connection.close()
    
    return removed_cameras

def remove_conflicting_rtsp(rtsp_address: str, camera_id: str) -> list:
    """Remove entries that have the specified rtsp address aside from the entry with the specified camera_id.

    Args:
        rtsp_address (str): rtsp address
        camera_id (str): camera id of entry to be kept

    Returns:
        list: camera ids of entries that were removed
    """
    cursor, connection =  get_sqlite_cursor()
    
    # check if rtsp address already exist in database
    cursor.execute(f"SELECT camera_id FROM Sources WHERE rtsp_address='{rtsp_address}' AND camera_id!='{camera_id}' ")
    removed_cameras = cursor.fetchall()
    
    cursor.execute(f"DELETE FROM Sources WHERE rtsp_address='{rtsp_address}' AND camera_id!='{camera_id}' ")
    
    logger.debug(f"remove_conflicting_rtsp: rtsp_address conflict, deleting database entries for '{removed_cameras}' ")
        
    connection.commit()
    cursor.close()
    connection.close()
    
    return removed_cameras

def update_source_type(camera_id: str) -> tuple:
    """Updates the source type in the database to match the source type from NX Witness.

    Args:
        camera_id (str): the camera id in NX

    Returns:
        tuple: (str, str)
            str: previous source type
            str: new source type 
    """
    nx_config = cfg.get_config()
        
    primary_source_type, secondary_source_type = nx.get_device_codec(
        nx_config.nxPort, nx_config.nxUser, nx_config.nxPass, nx_config.nxAddress, camera_id)
        
    cursor, connection = get_sqlite_cursor()
    
    cursor.execute(f"SELECT source_type FROM Sources WHERE camera_id='{camera_id}'")
    previous_source_type = cursor.fetchone()[0]
    
    if primary_source_type != previous_source_type:
        cursor.execute(f"UPDATE Sources SET source_type='{primary_source_type}' WHERE camera_id='{camera_id}'")
    
    connection.commit()
    cursor.close()
    connection.close()
    
    return (previous_source_type, primary_source_type)
        
class SourceInterface(ABC):
    
    @abstractmethod
    def get_kafka_topic_sql_column(self) -> str:
        """Returns the name of the sql column that contains the desired kafka topic of the analytic type."""
        raise NotImplementedError()
    
    @abstractmethod
    def get_flag_sql_column(self) -> str:
        """Returns the name of the sql column that contains the flag of the analytic type."""
        raise NotImplementedError()
    
    @abstractmethod
    def get_analytic_type(self) -> str:
        raise NotImplementedError()
    
    @abstractmethod
    def insert_source(self, kafka_topic: str, rtsp_address: str, camera_id: str, source_type: str, nx_server: dict) -> None:
        raise NotImplementedError()
    
    @abstractmethod
    def update_source(self, kafka_topic: str, rtsp_address: str, camera_id: str, nx_server: dict) -> None:
        raise NotImplementedError()
    
    @abstractmethod
    def deactivate_source(self, camera_id: str) -> None:
        raise NotImplementedError()
    

        
class DetectSource(SourceInterface):
    
    def __init__(self):
        self.__kafka_topic_sql_column = "detect_kafka_topic"
        self.__flag_sql_column = "detect_flag"
        self.__analytic_type = "detect"
        
    def get_kafka_topic_sql_column(self) -> str:
        return self.__kafka_topic_sql_column
    
    def get_flag_sql_column(self) -> str:
        return self.__flag_sql_column
    
    def get_analytic_type(self) -> str:
        return self.__analytic_type
    
    def __get_detect_details(self, camera_id: str) -> tuple:
        
        # TODO refactor detect confidence thresholds into  getters
        detect_con_threshold= nx.get_confidence_threshold(camera_id)
        
        # TODO refactor detect selected objects into  getters
        detect_objects= nx.get_selected_objects(camera_id)
        
        detect_objects_str = ""
        
        if len(detect_objects) != 0: detect_objects_str = json.dumps(detect_objects)
        
        return (detect_con_threshold, detect_objects_str)
    
    def insert_source(self, kafka_topic: str, rtsp_address: str, camera_id: str, source_type: str, nx_server: dict) -> None:
        cursor, connection = get_sqlite_cursor()
        
        detect_con_threshold, detect_objects_str = self.__get_detect_details(camera_id)
        
        source_detect = (kafka_topic, '', '', 1, 0, 0, rtsp_address, \
                        camera_id, nx.get_camera_name(camera_id), detect_con_threshold, 0, source_type, \
                        '', '', '', '', '', detect_objects_str, '')
        
        cursor.execute("INSERT INTO Sources \
                        ( \
                            detect_kafka_topic, detect_flag, rtsp_address, \
                            camera_id, camera_name \
                        ) \
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", source_detect)
        
        connection.commit()
        cursor.close()
        connection.close()
        
    def update_source(self, kafka_topic: str, rtsp_address: str, camera_id: str, nx_server: dict) -> None:
        cursor, connection = get_sqlite_cursor()
        
        detect_con_threshold, detect_objects_str = self.__get_detect_details(camera_id)
        
        source_detect_update = (kafka_topic, 1, detect_con_threshold, detect_objects_str, rtsp_address, camera_id)
        
        update_detect_query = "UPDATE Sources SET detect_kafka_topic=?, detect_flag=?, \
                        detect_con_threshold=?, detect_objects=?, rtsp_address=? WHERE camera_id=?"
                        
        cursor.execute(update_detect_query, source_detect_update)
        
        connection.commit()
        cursor.close()
        connection.close()
        
    def deactivate_source(self, camera_id: str) -> None:
        cursor, connection = get_sqlite_cursor()
        
        update_query = f"UPDATE Sources SET detect_kafka_topic=?, detect_objects=?, detect_con_threshold=?, \
                        camera_name=?, detect_flag=? WHERE camera_id=?"
        
        update_params = ('', '', 0.0, \
                        nx.get_camera_name(camera_id), 0, camera_id)
        
        cursor.execute(update_query, update_params)
        
        connection.commit()
        cursor.close()
        connection.close()

def heartbeat_source(source: SourceInterface, kafka_topic: str, rtsp_address: str, camera_id: str) -> bool:
    
    health = True
    
    KAFKA_TOPIC_SQL_COLUMN = source.get_kafka_topic_sql_column()
    
    previous_source_type, new_source_type = update_source_type(camera_id)
    if previous_source_type != new_source_type: 
        logger.info(f"Source.heartbeat(...): Updated source_type, '{previous_source_type}' to '{new_source_type}', in the database for '{camera_id}'")
        health = False
    
    conflicting_cams = remove_conflicting_kafka_topics(KAFKA_TOPIC_SQL_COLUMN, kafka_topic, camera_id)
    if conflicting_cams: 
        logger.debug(f"Source.heartbeat(...): Conflicting kafka_topic, '{kafka_topic}', found in database; entry belonging to '{conflicting_cams}' has been removed.")
        health = False
    
    conflicting_cams = remove_conflicting_rtsp(rtsp_address, camera_id)
    if conflicting_cams: 
        logger.debug(f"Source.heartbeat(...): Conflicting rtsp_address, '{rtsp_address}', found in database; entry belonging to '{conflicting_cams}' has been removed.")
        health = False
    
    nx_config = cfg.get_config()
    nx_server = {
        "server_user": nx_config.nxUser,
        "server_password": nx_config.nxPass,
        "server_address": nx_config.nxAddress,
        "server_port": nx_config.nxPort
    }
        
    cursor, connection = get_sqlite_cursor()
    
    cursor.execute(f"SELECT camera_id FROM Sources WHERE camera_id='{camera_id}' ")
    existing_entry = cursor.fetchall()
    
    if existing_entry:
        source.update_source(kafka_topic, rtsp_address, camera_id, new_source_type, nx_server)
    else:
        source.insert_source(kafka_topic, rtsp_address, camera_id, new_source_type, nx_server)
        health = False

    connection.commit()
    cursor.close()
    connection.close()
    
    return health

def add_source(source: SourceInterface, kafka_topic: str, rtsp_address: str, camera_id: str, source_type: str, nx_server: dict) -> None:
    # TODO ^^encapsulate parameters into source obj for ease of passing values around, caller can instantiate the necessary obj
    
    KAFKA_TOPIC_SQL_COLUMN = source.get_kafka_topic_sql_column()
    
    cursor, connection = get_sqlite_cursor()
    
    # remove conflicting entries
    conflicting_cams = remove_conflicting_kafka_topics(KAFKA_TOPIC_SQL_COLUMN, kafka_topic, camera_id)
    if conflicting_cams: logger.debug(f"Add_source: Conflicting kafka_topic found in database; entry belonging to '{conflicting_cams}' has been removed.")
    
    conflicting_cams = remove_conflicting_rtsp(rtsp_address, camera_id)
    if conflicting_cams: logger.debug(f"Add_source: Conflicting rtsp_address found in database; entry belonging to '{conflicting_cams}' has been removed.")
    
    # insert/update source
    cursor.execute(f"SELECT camera_id FROM Sources WHERE camera_id='{camera_id}' ")
    existing_entry = cursor.fetchall()
    
    if existing_entry:
        source.update_source(kafka_topic, rtsp_address, camera_id, source_type, nx_server)
    else:
        source.insert_source(kafka_topic, rtsp_address, camera_id, source_type, nx_server)

    connection.commit()
    cursor.close()
    connection.close()

def reorder_rowids():
    """Uses Sqlite VACUUM to reorder rowids on entry deletion
    """
    cursor, connection =  get_sqlite_cursor()
    connection.execute("VACUUM")
    cursor.close()
    connection.close()
    
def remove_source(camera_id: str, source: SourceInterface):
    
    source.deactivate_source(camera_id)
    
    cursor, connection = get_sqlite_cursor()
 
    cursor.execute(f"SELECT detect_flag from Sources WHERE camera_id='{camera_id}'")
    flag_data = cursor.fetchone()
    
    detect_flag = 0
    
    if flag_data:
        detect_flag = flag_data[0]
        
    if detect_flag == 0:
        cursor.execute(f"DELETE FROM Sources WHERE camera_id='{camera_id}'")
    
    connection.commit()
    cursor.close()
    connection.close()
    
    reorder_rowids()
    logger.info(f"remove_source(...): removed database entry for source '{camera_id}'")

def check_sources(nx_port: str, nx_user: str, nx_password: str, nx_ip: str) -> None:
    """Removes any cameras in the database that are not either online or recording.

    Args:
        port (str): port
        server_user (str): user of nx server
        server_password (str): password of nx server
        server_ip (str): ip address of nx server
    """
    import NxAPI
    
    cursor, connection =  get_sqlite_cursor()
    
    # grab all camera_id's from database
    cursor.execute(f"SELECT camera_id FROM Sources")
    rows = cursor.fetchall()
    
    if rows:
        # iterate over all camera ids
        for camID in [row[0] for row in rows]:

            # get device status
            status = NxAPI.get_device_status(nx_port, nx_user, nx_password, nx_ip, camID)
            
            logger.info(f"Source_check: '{camID}' has Nx Witness status of '{status}'")
            
            # if device is not recording
            if status != "Recording":
                
                # remove from database
                cursor.execute(f"DELETE FROM Sources WHERE camera_id='{camID}'")
                connection.commit()
                
                logger.warning(f"Source_check: '{camID}' does not have a valid Nx Witness status, deleting from the database")
   
    cursor.close()
    connection.close()



def push_to_custom_sql():
    """Push agrregated data into custom sql cloud ...
    """
    
    rows = []
    try:
        db_conf = db_cfg.get_config()
        dbname = db_conf.dbname
        tablename = db_conf.tablename
        old_column_name = json.loads(db_conf.old_column_name)
        associated_column_name = json.loads(db_conf.associated_column_name)
        placeholder = ', '.join(['%s'] * len(associated_column_name))
        current_time = datetime.datetime.now()

        logger.debug(f"push data to custom cloud scheduler at time: {current_time}")        
        
        # default schedule (15 minutes)
        previous_time = current_time - datetime.timedelta(minutes=15)
        
        # Format the timestamps for the SQL query
        current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
        previous_time_str = previous_time.strftime('%Y-%m-%d %H:%M:%S')
        
        sqlite_table = 'Detection'
        if check_table_existence(sqlite_table):
            # fetch aggregated data from sqlite
            query_sqlite = f"SELECT '{current_time_str}' as check_time, hw_id, camera_id, camera_name, \
                event_type, event_label, object_type FROM '{sqlite_table}' \
                    WHERE  event_time >= '{previous_time_str}' AND event_time < '{current_time_str}' GROUP BY camera_id, event_type, object_type"

            cursor_sqlite, connection_sqlite =  get_sqlite_cursor()
            cursor_sqlite.execute(query_sqlite)
            rows = cursor_sqlite.fetchall()
            cursor_sqlite.close()
            connection_sqlite.close()
            
            # push the fetched data into sql cloud
            cloud_table = tablename
            check_time = None

            found_events = []  # to store events found in table
            if len(rows):
                
                for row in rows:
                    check_time, hw_id, camera_id, camera_name, event_type, event_label, object_type = row
                    
                    found_events.append({"camera_id": camera_id, "event_type": event_type})

                    logger.info(f"camera {camera_name} and event = {event_type}")
                    
                    param_sql_cloud = (check_time, hw_id, camera_id, camera_name, event_type)
                    # push merged events to custom database
                    cursor_sql_cloud, connection_sql_cloud = get_custom_mysql_cursor()

                for each in param_sql_cloud:
                    custom_param_cloud = get_custom_param_cloud(each, old_column_name) # get matching column values
                    logger.info(f"custom_param_cloud = {custom_param_cloud}")
                    query_sql_cloud = f"INSERT INTO  {cloud_table} VALUES ({placeholder})"
                    cursor_sql_cloud.execute(query_sql_cloud, custom_param_cloud)
                    connection_sql_cloud.commit()
                    cursor_sql_cloud.close()
                    connection_sql_cloud.close()

                # push merged events to custom database
                cursor_sql_cloud, connection_sql_cloud = get_custom_mysql_cursor()




    except BaseException as b:
        logger.error(f"Failed to push data into custom sql cloud: {b}", exc_info=traces)

def push_to_sql():
    """Push agrregated data into default sql cloud
    """
    db_name = DB_NAME
    rows = []
    try:
        current_time = datetime.datetime.now()

        logger.debug(f"push count data to cloud scheduler at time: {current_time}")
        
        
        # default schedule (15 minutes)
        previous_time = current_time - datetime.timedelta(minutes=15)
        
        # Format the timestamps for the SQL query
        current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
        previous_time_str = previous_time.strftime('%Y-%m-%d %H:%M:%S')
        
        sqlite_table = 'Count'
        if check_table_existence(sqlite_table):
            # fetch aggregated data from sqlite
            query_sqlite = f"SELECT '{current_time_str}' as check_time, hw_id, camera_id, camera_name, \
                event_type, event_label, object_type FROM '{sqlite_table}' \
                    WHERE  event_time >= '{previous_time_str}' AND event_time < '{current_time_str}' GROUP BY camera_id, event_type, object_type"

            cursor_sqlite, connection_sqlite =  get_sqlite_cursor()
            cursor_sqlite.execute(query_sqlite)
            rows = cursor_sqlite.fetchall()
            cursor_sqlite.close()
            connection_sqlite.close()
            
            # push the fetched data into sql cloud
            cloud_table = 'people_count'
            check_time = None

            found_events = []  # to store events found in Count table
            if len(rows):
                for row in rows:
                    check_time, hw_id, camera_id, camera_name, event_type, event_label, object_type = row
                    

                    found_events.append({"camera_id": camera_id, "event_type": event_type, "object_type": object_type})

                    logger.info(f"camera {camera_name} and event = {event_type}")

                    query_sql_cloud = f"INSERT INTO  {cloud_table} \
                        (check_time, hw_id, camera_id, camera_name, event_type, event_label, object_type, count) \
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                    
                    param_sql_cloud = (check_time, hw_id, camera_id, camera_name, event_type, event_label, object_type)
                    cursor_sql_cloud, connection_sql_cloud = get_mysql_cursor()
                    cursor_sql_cloud.execute(query_sql_cloud, param_sql_cloud)
                    connection_sql_cloud.commit()
                    cursor_sql_cloud.close()
                    connection_sql_cloud.close()

                logger.info(f"{len(rows)} new rows added to the cloud for camera name = {camera_name}")


    except BaseException as b:
        logger.error(f"Failed to push count data into sql cloud: {b}", exc_info=traces)

def get_custom_param_cloud(each, old_column_name):
    filtered_params = []
    for item in old_column_name:
        if item in each:
            filtered_params.append(each[item])
    return tuple(filtered_params)

def get_custom_params(param_sql_cloud):
     #param_sql_cloud = [check_time, hw_id, camera_id, camera_name, event_type] 
    new_params_dict = {'check_time': param_sql_cloud[0],
                       'hw_id': param_sql_cloud[1],
                       'camera_id': param_sql_cloud[2],
                       'camera_name': param_sql_cloud[3]
                       }
        
    return new_params_dict

def get_unique_cameras(new_param_cloud_dict_list):
    camera_ids = []
    unique_cameras = []
    for each_dict in new_param_cloud_dict_list:
        camera_ids.append(each_dict['camera_id'])
    if len(camera_ids):
        unique_cameras = list(set(camera_ids))
    return unique_cameras                    
        

def is_value_present_in_list_of_dicts(list_of_dicts, target_value):
    if not list_of_dicts: # handle empty list error
        return False
    
    for d in list_of_dicts:
        if target_value in d.values():
            return True
    return False

# check if a camera_id is in the event_list and if yes, 
# check if the given event_type is in the corresponding dictionary object
def check_event(camera_id, event_type, event_list):
    if not event_list:
        return False

    found = False
    for event in event_list:
        if event["camera_id"] == camera_id and event["event_type"] == event_type:
            found = True
            break
    return found

# check if a camera_id is in the event_list and if yes, 
# check if the given event_type is in the corresponding dictionary object,
# if yes, check if a particular object_type is in the corresponding dictionary object.
def check_object(camera_id, event_type, object_type, event_list):
    if not event_list:
        return False

    found = False
    for event in event_list:
        if (event["camera_id"] == camera_id and event["event_type"] == event_type and event["object_type"].lower() == object_type.lower()):
            found = True
            break
    return found


def check_table_existence(table_name: str) -> bool:
    """check if a table exist in the databse

    Args:
        table_name (str): table name 

    Returns:
        bool: True if table found, otherwise False
    """
    try:        
        cursor, conn =  get_sqlite_cursor()
        # Query to fetch table names
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        cursor.execute(query, (table_name,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        # Check if the table exists
        if result is not None:
            return True
        else:
            return False
    except BaseException as b:
        logger.error(f"Failed to check table existance: {b}", exc_info=traces)

def add_objects(res: dict):
    """add selected objects and confidence threshold to the databse

    Args:
        res (dict):response from plugin
    """
    try:
        cam_id = res["device_id"]
        object_type = res["object_type"]
        
        detect_objects, count_objects = nx.get_selected_objects(cam_id)
        detect_con_threshold, count_con_threshold = nx.get_confidence_threshold(cam_id)
        
        cursor, connection =  get_sqlite_cursor()
        # when routed from detect plugin
        if object_type == "detection":
            update_query = f"UPDATE Sources SET detect_objects=?, detect_con_threshold=? WHERE camera_id=? "
            update_param = (json.dumps(detect_objects), detect_con_threshold, cam_id)
            cursor.execute(update_query, update_param)
        
        # when routed from count plugin
        if object_type == "count":
            update_query = f"UPDATE Sources SET count_objects=?, count_con_threshold=? WHERE camera_id=?"
            update_param = (json.dumps(count_objects), count_con_threshold, cam_id)
            cursor.execute(update_query, update_param)

        connection.commit()
        cursor.close()
        connection.close()

    except BaseException as b:
        logger.error(f"Failed to add objects to the database on camera {cam_id}: {b}", exc_info=traces)


def add_token(token: str):
    """ First time adding nx token into the database
    
    Args:
        token (str): nx session token
    """
    try:
        if table_exists(DB_NAME, 'Sessions'):
            cursor, connection =  get_sqlite_cursor()

            # clear the old entries
            cursor.execute("DELETE from Sessions")
            connection.commit()
           
            query = "INSERT INTO Sessions (token) VALUES (?)"
            param = (token,)
            cursor.execute(query, param)
            connection.commit()
            
            cursor.close()
            connection.close()

    except BaseException as b:
        logger.error(f"Failed to add token session into database: {b}", exc_info=traces)

def update_token(expired_token):
    """update nx token into database on expiry
    
    Args:
        expired_token (str): expired nx token
    """
    try:
        conf = cfg.get_config()
        server_user = conf.nxUser
        server_password = conf.nxPass
        server_ip = conf.nxAddress
        port = conf.nxPort

        new_token = nx.get_token(server_user, server_password, server_ip, port)

        cursor, connection =  get_sqlite_cursor()
        query = "UPDATE Sessions set token = ? WHERE token = ?"
        param = (new_token, expired_token)
        cursor.execute(query, param)
        connection.commit()
        cursor.close()
        connection.close()

    except BaseException as b:
        logger.error(f"Failed to update token session into database: {b}", exc_info=traces)

def fetch_updated_token(nx_user, nx_pass, nx_ip, nx_port) -> str:
    """fetch the updated nx token from database

    Returns:
        str: updated nx token
    """
    fetch_token = ''
    try:
        cursor, connection =  get_sqlite_cursor()
        cursor.execute("SELECT token from Sessions")
        fetch_row = cursor.fetchone()

        if fetch_row is not None:
            fetch_token = fetch_row[0]

            # update database with fresh session if it is blank
            if fetch_token == '':
                token = nx.get_token(nx_user, nx_pass, nx_ip, nx_port)
                if token != "" and token != "invalidCred":
                    add_token(token)

                cursor.execute("SELECT token from Sessions")
                fetch_row = cursor.fetchone()
                fetch_token = fetch_row[0]
                logger.debug("Blank session token found! Session updated with the fresh one")
        
        # update database with fresh session if table entries are deleted
        else:
            token = nx.get_token(nx_user, nx_pass, nx_ip, nx_port)
            if token != "" and token != "invalidCred":
                add_token(token)

            cursor.execute("SELECT token from Sessions")
            fetch_row = cursor.fetchone()
            if fetch_row:
                fetch_token = fetch_row[0]
                logger.debug("None session token found! Session updated with the fresh one")

        cursor.close()
        connection.close()
        
    except BaseException as b:
        logger.error(f"Failed to fetch nx token from database: {b}", exc_info=traces)

    return fetch_token

def initialise_scheduler():
    """initialise Scheduler table with camera_id and camera_name from Sources table
    """
    try:
        cursor, connection =  get_sqlite_cursor()
        query = "SELECT camera_id, camera_name from Sources"
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            cam_id = row[0]
            cam_name = row[1]
            
            # if blank table, or camera not in the table -> insert data from Sources table into Scheduler table
            table_name = 'Scheduler'
            if not is_cam_exist(cam_id, table_name):
                insert_query = f"INSERT INTO Scheduler (\
                                camera_id, camera_name, detect_start_flag,\
                                count_start_flag, detect_last_time, count_last_time)\
                                VALUES (?, ?, ?, ?, ?, ?)"
                current_time = datetime.datetime.now()
                insert_param = (cam_id, cam_name, 0, \
                                0, current_time, current_time)
                cursor.execute(insert_query, insert_param)

        connection.commit()
        cursor.close()
        connection.close()
    except BaseException as b:
        logger.error(f"Failed to initialise Scheduler table: {b}", exc_info=traces)

def is_empty(table_name) -> bool:
    """Check if a table is empty
    
    Returns:
        bool: True if table is empty, False otherwise

    """
    cursor, conn =  get_sqlite_cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    if len(rows):
        return False
    else:
        return True

def is_cam_exist(camera_id: str, table_name: str) -> bool:
    """check if a camera is present in the database table

    Args:
        camera_id (str): camera id 
        table_name (str): table name in the database

    Returns:
        bool: True if the camera id present in the table, False otherwise
    """
    cursor, connection =  get_sqlite_cursor()
    cursor.execute(f"SELECT * from '{table_name}' WHERE camera_id = '{camera_id}'")
    rows = cursor.fetchall()
    cursor.close()
    connection.close()
    if len(rows):
        return True
    else:
        return False

def is_val_present(list_of_dict, val):
    """check if a value is present in a list of dictionary

    Args:
        list_of_dict (list): list of dectionary 
        val (any): a given value parameter

    Returns:
        bool: True if the value found in the list, False otherwise
    """
    for i in range(len(list_of_dict)):
        if val in list_of_dict[i].values():
            return i
    return -1


def model_selection():
    """fetch object list from all the devices in Source table and 
        check if only person object is selected in all.
        If so, set the flag model_type as person-face, otherwise, coco.
    """
    try:
        model_type = "coco"
        cursor, connection = get_sqlite_cursor()
        query = "SELECT detect_objects, count_objects from Sources"
        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            for each in row:
                if each != "" and each != None:
                    each = json.loads(each)
                    if len(each) == 1:
                        if "Person" in each:
                            model_type = "person-face"
                    else:
                        model_type = "coco"
                        logger.info(f"Model type = {model_type} selected.")
                        return model_type

    except BaseException as b:
        logger.error(f"Failed to select a model type. {b}", exc_info=traces)
    
    cursor.close()
    connection.close()

    logger.info(f"Model type = {model_type} selected.")

    return model_type


# check if a table exists in a sqlite database
def table_exists(database_name, table_name):
    try:
        # Establish a connection to the SQLite database
        connection = sqlite3.connect(database_name)
        cursor = connection.cursor()

        # Execute a query to check if the table exists in the database
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")

        # Fetch the result
        result = cursor.fetchone()

        # Close the cursor and the connection
        cursor.close()
        connection.close()

        # Return True if the table exists, otherwise return False
        return result is not None

    except sqlite3.Error as e:
        print(f"Error: {e}")
        return False
