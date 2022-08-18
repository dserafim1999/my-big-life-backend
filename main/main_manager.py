# -*- coding: utf-8 -*-
"""
Contains class that orchestrates general features
"""
import json
from os import remove
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
        """
        TODO docs
        """

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
        """
        TODO docs
        """

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
        """
        TODO docs
        """

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
        """
        TODO docs
        """

        conn, cur = self.db_connect()
        if conn and cur:
            trips = db.get_all_trips(cur, self.debug)

        for val in trips:
            val['points'] = val['points'].to_json()
            val['points']['id'] = val['id']

        db.dispose(conn, cur)
        return {"trips": [r['points'] for r in trips]}

    def get_life_from_day(self, date):
        """
        TODO docs
        """

        f = open(join(expanduser(self.config['life_all'])), "r")
        raw_life = f.read()

        life = Life()
        life.from_string(raw_life.split('\n'))

        return repr(life.day_at_date(date))

    def delete_day(self, date):
        """
        TODO docs
        """

        conn, cur = self.db_connect()
        if conn and cur:
            #if canonical trips exist that are only tied to deleted trips, delete
            db.remove_canonical_trips_from_day(cur, date, self.debug)
            #delete trips/stays
            db.remove_trips_from_day(cur, date, self.debug)

            db.dispose(conn, cur)

        #delete output file (if exists)
        #TODO take output name format into consideration (can be changed in config)
        output_path = join(expanduser(self.config['output_path']), date + '.gpx')
        if isfile(output_path):
            remove(output_path)

        #delete life file(if exists)
        #TODO take life name format into consideration (can be changed in config)
        life_path = join(expanduser(self.config['life_path']), date + '.life')
        if isfile(life_path):
            remove(life_path)
            
        #remove day from all.life
        if self.config['life_path']:
            if self.config['life_all']:
                life_all_file = expanduser(self.config['life_all'])
            else:
                life_all_file = join(expanduser(self.config['life_path']), 'all.life')

            all_lifes = open(life_all_file, 'r').read()
            lifes = Life()
            lifes.from_string(all_lifes)
            life_date = date.replace('-','_')

            lifes.remove_day(life_date)
            with open(life_all_file, 'w') as dest_file:
                dest_file.write(repr(lifes))

    def upload_file(self, file):
        """
        TODO docs
        """
        
        f = open(join(expanduser(self.config['input_path']), file['name']), "w")
        f.write(file["data"])
        f.close()