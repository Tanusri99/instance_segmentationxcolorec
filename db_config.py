import sqlite3
from logger import create_logger
from TAPPAS_WORKSPACE import tappas_workspace
import json

logger, traces = create_logger("Config")
DB_NAME = f"{tappas_workspace}/data/config.db"

class config:
    checkbox  = 0
    hostname  = ""
    dbname  = ""
    tablename  = ""
    old_column_name = ""
    old_data_type = ""
    associated_column_name = ""
    username = ""
    password = ""

# get Nx config table data
def get_config():
    cfg = config()
    connection = sqlite3.connect(DB_NAME)
    table = """CREATE TABLE IF NOT EXISTS DB_Config (
                check_custom_db INTEGER,
                hostname TEXT NOT NULL,
                dbname TEXT NOT NULL,
                tablename TEXT NOT NULL UNIQUE,
                old_column_name TEXT NOT NULL,
                old_data_type TEXT NOT NULL,
                associated_column_name TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL);"""
    
    cursor = connection.cursor()                
    cursor.execute(table)
    connection.commit()
    select_statement = """SELECT * FROM DB_Config;"""
    res = cursor.execute(select_statement)
    res= res.fetchone()

    if res == None:
        cfg.checkbox  = ""
        cfg.hostname  = ""
        cfg.dbname  = ""
        cfg.tablename  = ""
        cfg.old_column_name = ""
        cfg.old_data_type = ""
        cfg.associated_column_name = ""
        cfg.username = ""
        cfg.password = ""
       
    else:
        cfg.checkbox = res[0]
        cfg.hostname = res[1]
        cfg.dbname = res[2]
        cfg.tablename = res[3]
        cfg.old_column_name = res[4]
        cfg.old_data_type = res[5]
        cfg.associated_column_name = res[6]
        cfg.username = res[7]
        cfg.password = res[8]
        
   
    cursor.close()
    connection.close()

    logger.debug("Successfully fetched config from database")

    return cfg

def save_config(config):
    try:
        checkbox = config.checkbox
        hostname = config.hostname
        dbname = config.dbname
        tablename = config.tablename
        old_column_name = config.old_column_name
        old_data_type = config.old_data_type
        associated_column_name = config.associated_column_name
        username = config.username
        password = config.password

        logger.info(f"checkbox = {checkbox}")
        logger.info(f"hostname = {hostname}")
        logger.info(f"dbname = {dbname}")
        logger.info(f"tablename = {tablename}")
        logger.info(f"old_column_name = {old_column_name}")
        logger.info(f"old_data_type = {old_data_type}")
        logger.info(f"associated_column_name = {associated_column_name}")
        logger.info(f"username = {username}")
        logger.info(f"password = {password}")
        
        connection = sqlite3.connect(DB_NAME)
        cursor = connection.cursor()
        drop = """DROP TABLE IF EXISTS DB_Config;"""
        cursor.execute(drop)
        connection.commit()
        table = """CREATE TABLE IF NOT EXISTS DB_Config (
                check_custom_db INTEGER,
                hostname TEXT NOT NULL,
                dbname TEXT NOT NULL,
                tablename TEXT NOT NULL UNIQUE,
                old_column_name TEXT NOT NULL,
                old_data_type TEXT NOT NULL,
                associated_column_name TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL);"""
        cursor.execute(table)
        connection.commit()
        insert = """INSERT INTO DB_Config (check_custom_db, hostname, dbname, tablename,
                old_column_name, old_data_type, associated_column_name, username, password)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        params = (checkbox, hostname, dbname, tablename, json.dumps(old_column_name), json.dumps(old_data_type), json.dumps(associated_column_name), username, password)
        cursor.execute(insert, params)
        connection.commit()
        cursor.close()
        connection.close()
        logger.info("Successfully saved db config into database")
    except BaseException as b:
        logger.error(f"Error saving db config into database: {b}", exc_info=traces)
    return "Success"


