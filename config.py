import sqlite3
from logger import create_logger
from TAPPAS_WORKSPACE import tappas_workspace

logger, traces = create_logger("Config")
DB_NAME = f"{tappas_workspace}/data/config.db"

class config:
    nxUser = ""
    nxPass = ""
    nxAddress = ""
    nxPort = ""
    confidence = ""   

# get Nx config table data
def get_config():
    cfg = config()
    connection = sqlite3.connect(DB_NAME)
    table = """CREATE TABLE IF NOT EXISTS Config (
                nx_user TEXT NOT NULL,
                nx_pass TEXT NOT NULL,
                nx_address TEXT NOT NULL UNIQUE,
                nx_port TEXT NOT NULL,
                confidence REAL NOT NULL);"""
    
    cursor = connection.cursor()                
    cursor.execute(table)
    connection.commit()
    select_statement = """SELECT * FROM Config;"""
    res = cursor.execute(select_statement)
    res= res.fetchone()

    if res == None:
        cfg.nxUser = ""
        cfg.nxPass = ""
        cfg.nxAddress = ""
        cfg.nxPort = ""
        cfg.confidence = 0.80
       
    else:
        cfg.nxUser = res[0]
        cfg.nxPass = res[1]
        cfg.nxAddress = res[2]
        cfg.nxPort = res[3]
        cfg.confidence = res[4]
        
   
    cursor.close()
    connection.close()

    logger.debug("Successfully fetched config from database")

    return cfg

def save_config(config):
    try:
        nxuser = config.nxUser
        nxpass = config.nxPass
        nxaddress = config.nxAddress
        nxport = config.nxPort
        confidence = config.confidence
        
        connection = sqlite3.connect(DB_NAME)
        cursor = connection.cursor()
        drop = """DROP TABLE IF EXISTS Config;"""
        cursor.execute(drop)
        connection.commit()
        table = """CREATE TABLE IF NOT EXISTS Config (
                    nx_user TEXT NOT NULL,
                    nx_pass TEXT NOT NULL,
                    nx_address TEXT NOT NULL UNIQUE,
                    nx_port TEXT NOT NULL,
                    confidence REAL NOT NULL);"""
        cursor.execute(table)
        connection.commit()
        insert = """INSERT INTO Config (nx_user, nx_pass, nx_address, nx_port, confidence) VALUES (?, ?, ?, ?, ?)"""
        params = (nxuser, nxpass, nxaddress, nxport, confidence)
        cursor.execute(insert, params)
        connection.commit()
        cursor.close()
        connection.close()
        logger.info("Successfully saved config into database")
    except BaseException as b:
        logger.error(f"Error saving config into database: {b}", exc_info=traces)
    return "Success"
