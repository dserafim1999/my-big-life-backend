# -*- coding: utf-8 -*-
"""
Contains class that orchestrates general features
"""
from datetime import datetime
import json

from os import remove
from os.path import join, expanduser, isfile
from life.life import Life
from main import db
from utils import Manager, merge_bounding_boxes

class MainManager(Manager):
    """ Manager that contains general features

    Arguments:
        configFile: configuration file directory 
    """
    def __init__(self, config_file, debug):
        super().__init__(config_file, debug)
        self.configFile = config_file
        self.loadedBoundingBox = [{"lat": 0, "lon": 0}, {"lat": 0, "lon": 0}]

    def update_config(self, new_config):
        """ Updates the config object by overlapping with the new config object

        Args:
            new_config (obj): JSON object that contains configuration changes 
        """
        super().update_config(new_config)
        with open(expanduser(self.configFile), 'w') as config_file:
            json.dump(self.config, config_file, indent=4)

    def get_trips_and_locations(self):
        """ Fetches canonical trips and locations from the database

        See `db.get_canonical_trips` and `db.get_canonical_locations`

        Returns:
            :obj:`dict`
        """

        conn, cur = self.db_connect()
        if conn and cur:
            trips = db.get_canonical_trips(cur, self.debug)
            locations = db.get_canonical_locations(cur, self.debug)

        db.dispose(conn, cur)
        return {"trips": trips, "locations": locations}

    def get_locations(self):
        """ Fetches canonical locations from the database

        See `db.get_canonical_locations`

        Returns:
            :obj:`dict`
        """

        conn, cur = self.db_connect()
        if conn and cur:
            locations = db.get_canonical_locations(cur, self.debug)

        db.dispose(conn, cur)
        return {"locations": locations}

    def get_canonical_trips(self):
        """ Fetches canonical trips from the database

        See `db.get_canonical_trips`

        Returns:
            :obj:`dict`
        """

        conn, cur = self.db_connect()
        if conn and cur:
            trips = db.get_canonical_trips(cur, self.debug)

        db.dispose(conn, cur)
        return {"trips": trips}

    def get_trips(self, latMin, lonMin, latMax, lonMax, canonical):
        """ Fetches trips from the database in bounds

        See `db.get_trips`

        Args:
            latMin (float): minimum latitude of bounds
            lonMin (float): minimum longitude of bounds
            latMax (float): maximum latitude of bounds
            lonMax (float): minimum longitude of bounds
            canonical (bool): determines whether the trips are canonical or not
        Returns:
            :obj:`dict`
        """

        conn, cur = self.db_connect()
        self.loadedBoundingBox = [{"lat": latMin, "lon": lonMin}, {"lat": latMax, "lon": lonMax}]

        if conn and cur:
            trips = db.get_trips(cur, self.loadedBoundingBox, canonical, self.debug)

        db.dispose(conn, cur)
        return {"trips": trips}

    def can_get_more_trips(self, latMin, lonMin, latMax, lonMax, canonical):
        """ Checks whether there are trips in db that haven't been fetched yet in a certain bounding box

        See `db.get_more_trips`

        Args:
            latMin (float): minimum latitude of bounds
            lonMin (float): minimum longitude of bounds
            latMax (float): maximum latitude of bounds
            lonMax (float): minimum longitude of bounds
            canonical (bool): determines whether the trips are canonical or not
        Returns:
            :obj:`dict`
        """

        conn, cur = self.db_connect()

        if conn and cur:
            can_load = db.can_get_more_trips(cur, [{"lat": latMin, "lon": lonMin}, {"lat": latMax, "lon": lonMax}], self.loadedBoundingBox, canonical, self.debug)

        db.dispose(conn, cur)
        return can_load

    def get_more_trips(self, latMin, lonMin, latMax, lonMax, canonical):
        """ Fetches trips from the database in bounds that have not yet been loaded

        See `db.get_more_trips`

        Args:
            latMin (float): minimum latitude of bounds
            lonMin (float): minimum longitude of bounds
            latMax (float): maximum latitude of bounds
            lonMax (float): minimum longitude of bounds
            canonical (bool): determines whether the trips are canonical or not
        Returns:
            :obj:`dict`
        """

        conn, cur = self.db_connect()

        if conn and cur:
            trips = db.get_more_trips(cur, [{"lat": latMin, "lon": lonMin}, {"lat": latMax, "lon": lonMax}], self.loadedBoundingBox, canonical, self.debug)

        self.loadedBoundingBox = merge_bounding_boxes([{"lat": latMin, "lon": lonMin}, {"lat": latMax, "lon": lonMax}], self.loadedBoundingBox) 

        db.dispose(conn, cur)
        return {"trips": trips}

    def get_all_trips(self):
        """ Fetches all trips from the database

        See `db.get_all_trips`

        Returns:
            :obj:`dict`
        """

        conn, cur = self.db_connect()
        if conn and cur:
            trips = db.get_all_trips(cur, self.debug)

        db.dispose(conn, cur)
        return {"trips": trips}

    def get_life_from_day(self, date):
        """ Returns the LIFE representation of a day in the database

        Args:
            date (str)
        Returns:
            str
        """
        f = open(join(expanduser(self.config['life_all'])), "r")
        raw_life = f.read()
        f.close()

        life = Life()
        life.from_string(raw_life.split('\n'))

        date = date.replace("-", "_")
        return repr(life.day_at_date(date))

    def get_global_life_string(self):
        """ Returns the global LIFE file string

        Returns:
            str
        """

        f = open(join(expanduser(self.config['life_all'])), "r")
        life_str = f.read()
        f.close()

        return life_str

    def get_global_life_json(self):
        """ Returns the global LIFE file in JSON

        Returns:
            obj
        """
        life_str = self.get_global_life_string()
        life = Life()
        life.from_string(life_str)

        return life.to_json()
        

    def delete_day(self, date):
        """ Deletes all data saved for a specific day, including track and LIFE files, as well as DB entries
        Args:
            date (str)
        """

        conn, cur = self.db_connect()
        if conn and cur:
            #if canonical trips exist that are only tied to deleted trips, delete
            db.remove_canonical_trips_from_day(cur, date, self.debug)
            #delete trips/stays
            db.remove_trips_from_day(cur, date, self.debug)

            db.dispose(conn, cur)

        day_datetime = datetime.strptime(date, "%Y-%m-%d")
        
        # Take output name format into consideration (format can be changed in config)
        day = day_datetime.strftime(self.config['trip_name_format']) 

        # Delete output file (if exists)
        output_path = join(expanduser(self.config['output_path']), day + '.gpx')
        if isfile(output_path):
            remove(output_path)

        # Delete life file(if exists)
        life_path = join(expanduser(self.config['life_path']), day + '.life')
        if isfile(life_path):
            remove(life_path)
            
        # Remove day from all.life
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
        """ Creates a file using a JSON object

        Args: 
            file (:obj:`dict`): contains file name and data
        """
        
        f = open(join(expanduser(self.config['input_path']), file['name']), "w")
        f.write(file["data"])
        f.close()