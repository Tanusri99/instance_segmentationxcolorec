from TAPPAS_WORKSPACE import tappas_logfile
import logging 
import os

def create_logger(logger_name):
    log_env = os.getenv("LOG_LEVEL")
    if log_env == "INFO":
        log_level = logging.INFO
        traces = True
    elif log_env == "WARNING":
        log_level = logging.WARNING
        traces = False
    elif log_env == "ERROR":
        log_level = logging.ERROR
        traces = False
    elif log_env == "CRITICAL":
        log_level = logging.CRITICAL
        traces = False
    else:
        log_level = logging.DEBUG
        traces = False

    logger = logging.getLogger(logger_name)
    handler = logging.FileHandler(tappas_logfile)
    formater = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setLevel(log_level)
    handler.setFormatter(formater)
    logger.addHandler(handler)
    logger.setLevel(log_level)
    
    return logger, traces