import yaml
from TAPPAS_WORKSPACE import tappas_workspace
from logger import create_logger

logger, traces = create_logger("App_cfg")
CONFIG_FILE = f"{tappas_workspace}/data/app_cfg.yaml"

class Settings:
    def __init__(self):
        # Server port
        self.server_port = "47760"
        self.mysql_hostname = '192.168.1.70'
        self.mysql_user = 'aol_color'
        self.mysql_pass = 'Aol2021!'
        self.mysql_dbname = 'DETECT_COLOR'
        
        
    def to_dict(self):
        cfg = {            
            "server_port":self.server_port ,
            "mysql_hostname":self.mysql_hostname,
            "mysql_user":self.mysql_user,
            "mysql_pass":self.mysql_pass,
            "mysql_dbname":self.mysql_dbname
           
        }
        return cfg
        
    def try_parse(self, settings):
        try:
            self.server_port = settings["server_port"]
            self.mysql_hostname = settings["mysql_hostname"]
            self.mysql_user = settings["mysql_user"]
            self.mysql_pass = settings["mysql_pass"]
            self.mysql_dbname = settings["mysql_dbname"]
        except BaseException as b:
            logger.error(f"Parse settings error: {b}", exc_info=traces)
            return False
        return True
    
    def read_config(self):
        global CONFIG_FILE
        config_file = CONFIG_FILE
        ok = True
        try:
            with open(config_file) as f:
                data = yaml.load(f, Loader=yaml.FullLoader)
                ok = self.try_parse(data)
        except BaseException as b:
            logger.error(f"Load settings error: {b}", exc_info=traces)
            return False
        return ok
    
    def save_settings(self):
        global CONFIG_FILE
        config_file = CONFIG_FILE
        try:
            cfg = self.to_dict()
            with open(config_file, 'w+') as f:
                yaml.dump(data=cfg, stream=f)
            logger.info(f"Settings successfully saved to {config_file}")  
        except BaseException as b:
            logger.error(f"Save settings error: {b}", exc_info=traces)
            return False
        return True