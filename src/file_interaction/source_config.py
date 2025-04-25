import configparser
from pathlib import Path


def get_project_root() -> Path:
    return Path(__file__).parent.parent.parent


config = configparser.ConfigParser()
config.read(f"{get_project_root()}/config.ini")


# If you want to add more data set, add another line here
SFDE_USERNAME = config["DATA"].get("sfde_username", "")
SFDE_PASSWORD = config["DATA"].get("sfde_password", "")
SFDE_DATABASE = config["DATA"].get("sfde_database", "")
SFDE_HOST = config["DATA"].get("sfde_host", "")
SFDE_PORT = config["DATA"].get("sfde_port", "")
