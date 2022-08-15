# -*- coding: utf-8 -*-
"""
Contains class that orchestrates general features
"""
import json
from os.path import join, expanduser, isfile
from life.life import Life
from main import db
from utils import merge_bounding_boxes, update_dict

from main.default_config import CONFIG


class MainManager(object):
    """ 
        
    """

    def __init__(self, config_file, debug):
        self.config = dict(CONFIG)
        self.debug = debug
        self.loadedBoundingBox = [{"lat": 0, "lon": 0}, {"lat": 0, "lon": 0}]

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

    def get_trips_and_locations(self):
        conn, cur = self.db_connect()
        result = []
        if conn and cur:
            trips = db.get_canonical_trips(cur, self.debug)
            locations = db.get_canonical_locations(cur, self.debug)
        for val in trips:
            val['points'] = val['points'].to_json()
            val['points']['id'] = val['id']
        for val in locations:
            val['point'] = val['point'].to_json()
            val['point']['label'] = val['label']
            val['point']['id'] = val['id']

        db.dispose(conn, cur)
        return {"trips": [r['points'] for r in trips], "locations": [r['point'] for r in locations]}

    def get_trips(self, latMin, lonMin, latMax, lonMax, canonical):
        conn, cur = self.db_connect()
        self.loadedBoundingBox = [{"lat": latMin, "lon": lonMin}, {"lat": latMax, "lon": lonMax}]

        if conn and cur:
            trips = db.get_trips(cur, self.loadedBoundingBox, canonical, self.debug)

        for val in trips:
            val['points'] = val['points'].to_json()
            val['points']['id'] = val['id']

        db.dispose(conn, cur)
        return {"trips": [r['points'] for r in trips]}

    def get_more_trips(self, latMin, lonMin, latMax, lonMax, canonical):
        conn, cur = self.db_connect()

        if conn and cur:
            trips = db.get_more_trips(cur, [{"lat": latMin, "lon": lonMin}, {"lat": latMax, "lon": lonMax}], self.loadedBoundingBox, canonical, self.debug)

        for val in trips:
            val['points'] = val['points'].to_json()
            val['points']['id'] = val['id']

        self.loadedBoundingBox = merge_bounding_boxes([{"lat": latMin, "lon": lonMin}, {"lat": latMax, "lon": lonMax}], self.loadedBoundingBox) 

        db.dispose(conn, cur)
        return {"trips": [r['points'] for r in trips]}

    def get_all_trips(self):
        conn, cur = self.db_connect()
        if conn and cur:
            trips = db.get_all_trips(cur, self.debug)

        for val in trips:
            val['points'] = val['points'].to_json()
            val['points']['id'] = val['id']

        db.dispose(conn, cur)
        return {"trips": [r['points'] for r in trips]}

    def get_life_from_day(self, data):
        f = open(join(expanduser(self.config['life_all'])), "r")
        raw_life = f.read()

        life = Life()
        life.from_string(raw_life.split('\n'))

        return repr(life.day_at_date(data["date"]))

    def upload_file(self, file):
        f = open(join(expanduser(self.config['input_path']), file['name']), "w")
        f.write(file["data"])
        f.close()