# -*- coding: utf-8 -*-
"""
Contains class that orchestrates general features
"""
import json
from os.path import expanduser, isfile
from main import db
from utils import update_dict

from main.default_config import CONFIG


class MainManager(object):
    """ 
        
    """

    def __init__(self, config_file):
        self.config = dict(CONFIG)

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

    def get_all_trips(self):
        conn, cur = self.db_connect()
        result = []
        if conn and cur:
            result = db.get_all_trips(cur)
        for val in result:
            val['points'] = val['points'].to_json()
            val['points']['id'] = val['id']
        db.dispose(conn, cur)
        return [r['points'] for r in result]

