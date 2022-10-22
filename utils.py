from os.path import expanduser, isfile
from main.default_config import CONFIG
import json
from main import db

def update_dict(target, updater):
    """ Updates a dictionary, keeping the same structure

    Args:
        target (:obj:`dict`): dictionary to update
        updater (:obj:`dict`): dictionary with the new information
    """
    target_keys = list(target.keys())
    for key in list(updater.keys()):
        if key in target_keys:
            if isinstance(target[key], dict):
                update_dict(target[key], updater[key])
            else:
                target[key] = updater[key]

def merge_bounding_boxes(bb1, bb2):
    """ Combines two bounding boxes

    Args:
        bb1 (:obj:`list` of :obj:`dict`)
        bb2 (:obj:`list` of :obj:`dict`)
    Returns:
        :obj:`list` of :obj:`dict`
    """
    min_lat = min(min(bb1[0]["lat"], bb1[1]["lat"]), min(bb2[0]["lat"], bb2[1]["lat"]))
    min_lon = min(min(bb1[0]["lon"], bb1[1]["lon"]), min(bb2[0]["lon"], bb2[1]["lon"]))
    max_lat = max(max(bb1[0]["lat"], bb1[1]["lat"]), max(bb2[0]["lat"], bb2[1]["lat"]))
    max_lon = max(max(bb1[0]["lon"], bb1[1]["lon"]), max(bb2[0]["lon"], bb2[1]["lon"]))

    return [{"lat": min_lat, "lon": min_lon}, {"lat": max_lat, "lon": max_lon}]


class Manager(object):
    '''
    Manager Template 
    '''
    def __init__(self, config_file, debug):
        self.config = dict(CONFIG) # default configuration
        self.debug = debug

        if config_file and isfile(expanduser(config_file)):
            with open(expanduser(config_file), 'r') as config_file:
                config = json.loads(config_file.read())
                update_dict(self.config, config)
        
    def update_config(self, new_config):
        update_dict(self.config, new_config)
    
    def db_connect(self):
        """ Creates a connection with the database

        Use `db.dispose` to commit and close cursor and connection

        Returns:
            (psycopg2.connection, psycopg2.cursor): Both are None if the connection is invalid
        """
        dbc = self.config['db']
        conn = db.connect_db(dbc['host'], dbc['name'], dbc['user'], dbc['port'], dbc['pass'])
        if conn:
            return conn, conn.cursor()
        else:
            return None, None

    