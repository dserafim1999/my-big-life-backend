# -*- coding: utf-8 -*-
"""
Contains class that orchestrates queries
"""

import json
import datetime
import itertools
import math

from os.path import expanduser, isfile
from operator import itemgetter


import psycopg2
from scipy import rand
from main import db
from utils import update_dict

import queries.utils as utils 

from main.default_config import CONFIG

class QueryManager(object):
    """ 
    """
    def __init__(self, config_file, debug):
        self.config = dict(CONFIG)
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
    
    def execute_query(self, payload):
        conn, cur = self.db_connect()

        items = parse_items(payload["data"])
        generate_queries(items)
        self.moreResults, results = fetch_from_db(cur, items, self.debug)
        return results

class Range:
    start = ""
    end = ""
    temporalStartRange = 0 #minutes
    temporalEndRange = 0
    duration = "" #6h15m 6h 5m
    location = ""
    spatialRange = 0 #meters
    durationSign = ""
    startSign = ""
    endSign = ""
    spatialSign = ""
    fullDate = ""
    castTime = ""
    query = ""
    locationCoords = ""

    def get_query(self):
        return self.query

    def has_value(self, value):
        return value.strip() != ""

    def __init__(self, start, end, temporalStartRange, temporalEndRange, duration, location, spatialRange):
        global date
        global previousEndDate

        self.fullDate, self.castTime = utils.is_full_date(date)

        if self.has_value(start):
            self.start = utils.join_date_time(date, start)
            self.startSign = utils.get_sign(start)
        else:
            self.start = None
            self.startSign = None

        if self.has_value(end):
            self.end = utils.join_date_time(date, end)
            self.endSign = utils.get_sign(end)
        else:
            self.end = None
            self.endSign = None

        if(self.start is not None and utils.get_all_but_sign(start) < previousEndDate and date != "--/--/----"):
            self.start += datetime.timedelta(days=1)
        if(self.end is not None and utils.get_all_but_sign(end) < previousEndDate and date != "--/--/----"):
            self.end += datetime.timedelta(days=1)

        if self.has_value(temporalStartRange): #stored as half
            self.temporalStartRange = utils.fuzzy_to_sql(temporalStartRange)
        else:
            self.temporalStartRange = None

        if self.has_value(temporalEndRange): #stored as half
            self.temporalEndRange = utils.fuzzy_to_sql(temporalEndRange)
        else:
            self.temporalEndRange = None

        if self.has_value(duration):
            self.duration = utils.duration_to_sql(duration)
            self.durationSign = utils.get_sign(duration)
        else:
            self.duration = None
            self.durationSign = None

        if self.has_value(spatialRange):
            self.spatialRange = utils.spatial_range_to_meters(spatialRange)
            self.spatialSign = utils.get_sign(spatialRange)
        else:
            self.spatialRange = None
            self.spatialSign = ""

        if self.has_value(location):
            if utils.is_coordinates(location):
                self.locationCoords = utils.switch_coordinates(location)
                self.location = None
                if self.spatialRange is None:
                    self.spatialRange = 0
                    self.spatialSign = "="
            else:
                self.locationCoords = None
                self.location = location
                if self.spatialRange is None:
                    self.spatialRange = 0
                    self.spatialSign = "="
        else:
            self.location = None
            self.locationCoords = None

    def query_chunk_date(self, type, cast, sign, date):
        return f" {type}{cast} {sign} '{date}' "

    def query_chunk_start_date(self):
        return self.query_chunk_date('start_date', self.castTime, self.startSign, self.start)

    def query_chunk_end_date(self):
        return self.query_chunk_date('end_date', self.castTime, self.endSign, self.end)
    
    def query_chunk_interval(self, type, cast, dateType, temporalRange, date):
        return f" {type}{cast} BETWEEN CAST('{date}' AS {dateType}) - CAST('{temporalRange}' AS INTERVAL) AND CAST('{date}' AS {dateType}) + CAST('{temporalRange}' AS INTERVAL) "

    def query_chunk_start_interval(self):
        return self.query_chunk_interval('start_date', self.castTime, self.fullDate, self.temporalStartRange, self.start)

    def query_chunk_end_interval(self):
        return self.query_chunk_interval('end_date', self.castTime, self.fullDate, self.temporalEndRange, self.end)

    def query_chunk_duration(self, table_name):
        with_chunk = f" {table_name} AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM stays) "
        where_chunk = f" {table_name}.duration {self.durationSign} '{self.duration}' "

        return (table_name, with_chunk, where_chunk)

    def query_chunk_location(self, table_name):
        with_chunk = f" {table_name} AS (SELECT centroid FROM locations WHERE label = '{self.location}') "
        where_chunk = f" ST_DISTANCE({table_name}.centroid, locations.centroid) {self.spatialSign} '{self.spatialRange}' "

        return (table_name, with_chunk, where_chunk)

    def query_chunk_coords(self, table_name):
        with_chunk = f" {table_name} AS (SELECT label FROM locations WHERE ST_Distance(centroid, ST_SetSRID(ST_MakePoint({self.locationCoords}),4326)) {self.spatialSign} '{self.spatialRange}' ) "
        where_chunk = f" {table_name}.label = stays.location_label "

        return (table_name, with_chunk, where_chunk)

    def query_chunks(self):
        with_chunks = []
        where_chunks = [" locations.label = stays.location_label "]
        tables = ["stays", "locations"]

        if self.start is not None:
            if self.temporalStartRange is not None:
                where_chunks.append(self.query_chunk_start_interval())
                if self.startSign != '=':
                    where_chunks.append(self.query_chunk_start_date())
            else:
                where_chunks.append(self.query_chunk_start_date())

        if self.end is not None:
            if self.temporalEndRange is not None:
                where_chunks.append(self.query_chunk_end_interval())
                if self.endSign != '=':
                    where_chunks.append(self.query_chunk_end_date())
            else:
                where_chunks.append(self.query_chunk_end_date())

        if self.duration is not None:
            duration_chunks = self.query_chunk_duration("durations")
            
            tables.append(duration_chunks[0])
            with_chunks.append(duration_chunks[1])
            where_chunks.append(duration_chunks[2])

        if self.location is not None:
            location_chunks = self.query_chunk_location("locs")

            tables.append(location_chunks[0])
            with_chunks.append(location_chunks[1])
            where_chunks.append(location_chunks[2])
            
        if self.locationCoords is not None:
            coords_chunks = self.query_chunk_coords("coords")

            tables.append(coords_chunks[0])
            with_chunks.append(coords_chunks[1])
            where_chunks.append(coords_chunks[2])

        global date
        if date != "--/--/----":
            where_chunks.append(f" start_date::date = '{date}' ")

        return tables, with_chunks, where_chunks        


    def generate_query(self):
        #base_query = " SELECT DISTINCT stay_id, start_date, end_date, locations.centroid FROM "
        base_query = " SELECT DISTINCT location_label, start_date, end_date, locations.centroid FROM "

        tables, with_chunks, where_chunks = self.query_chunks()

        query = ""

        # WITH ...
        if len(with_chunks) > 0:
            query += " WITH "
            for i in range(len(with_chunks)):
                query += with_chunks[i]
                if i < len(with_chunks) - 1:
                    query += " , "

        # SELECT ...
        query += base_query

        # FROM ...
        if len(tables) > 0:
            for i in range(len(tables)):
                query += tables[i]
                if i < len(tables) - 1:
                    query += " , "

        # WHERE ...
        if len(where_chunks) > 0:
            query += " WHERE "
            for i in range(len(where_chunks)):
                query += where_chunks[i]
                if i < len(where_chunks) - 1:
                    query += " AND "

        #print(query)
        self.query =  query

class Interval:
    start = ""
    end = ""
    temporalStartRange = 0 #minutes
    temporalEndRange = 0
    duration = ""
    route = ""
    durationSign = ""
    startSign = ""
    endSign = ""
    fullDate = ""
    castTime = ""
    query = ""

    def get_query(self):
        return self.query

    def has_value(self, value):
        return value.strip() != ""

    def __init__(self, start, end, temporalStartRange, temporalEndRange, duration, route):
        global date
        global previousEndDate

        self.fullDate, self.castTime = utils.is_full_date(date)

        if self.has_value(start):
            self.start = utils.join_date_time(date, start)
            self.startSign = utils.get_sign(start)
        else:
            self.start = None
            self.startSign = None

        if self.has_value(end):
            self.end = utils.join_date_time(date, end)
            self.endSign = utils.get_sign(end)
        else:
            self.end = None
            self.endSign = None

        if(self.start is not None and utils.get_all_but_sign(start) < previousEndDate):
            self.start += datetime.timedelta(days=1)
        if(self.end is not None and utils.get_all_but_sign(end) < previousEndDate):
            self.end += datetime.timedelta(days=1)

        if self.has_value(temporalStartRange): #stored as half
            self.temporalStartRange = utils.fuzzy_to_sql(temporalStartRange)
        else:
            self.temporalStartRange = None

        if self.has_value(temporalEndRange): #stored as half
            self.temporalEndRange = utils.fuzzy_to_sql(temporalEndRange)
        else:
            self.temporalEndRange = None

        if self.has_value(duration):
            self.duration = utils.duration_to_sql(duration)
            self.durationSign = utils.get_sign(duration)
        else:
            self.duration = None
            self.durationSign = None

        if self.has_value(route) and utils.is_coordinates(route):
            self.route = utils.switch_coordinates(route)
        else:
            self.route = None


    def query_chunk_date(self, type, cast, sign, date):
        return f" {type}{cast} {sign} '{date}' "

    def query_chunk_start_date(self):
        return self.query_chunk_date('start_date', self.castTime, self.startSign, self.start)

    def query_chunk_end_date(self):
        return self.query_chunk_date('end_date', self.castTime, self.endSign, self.end)
    
    def query_chunk_interval(self, type, cast, dateType, temporalRange, date):
        return f" {type}{cast} BETWEEN CAST('{date}' AS {dateType}) - CAST('{temporalRange}' AS INTERVAL) AND CAST('{date}' AS {dateType}) + CAST('{temporalRange}' AS INTERVAL) "

    def query_chunk_start_interval(self):
        return self.query_chunk_interval('start_date', self.castTime, self.fullDate, self.temporalStartRange, self.start)

    def query_chunk_end_interval(self):
        return self.query_chunk_interval('end_date', self.castTime, self.fullDate, self.temporalEndRange, self.end)

    def query_chunk_duration(self, table_name):
        with_chunk = f" {table_name} AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration, trip_id FROM trips) "
        where_chunk = f" {table_name}.duration {self.durationSign} '{self.duration}' "

        return (table_name, with_chunk, where_chunk)

    def query_chunk_route(self):
        return f" ST_DWithin(points, ST_SetSRID(ST_MakePoint({self.route}),4326)::geography, 250) "

    def query_chunks(self):
        with_chunks = []
        where_chunks = []
        tables = ["trips"]

        if self.start is not None:
            if self.temporalStartRange is not None:
                where_chunks.append(self.query_chunk_start_interval())
                if self.startSign != '=':
                    where_chunks.append(self.query_chunk_start_date())
            else:
                where_chunks.append(self.query_chunk_start_date())

        if self.end is not None:
            if self.temporalEndRange is not None:
                where_chunks.append(self.query_chunk_end_interval())
                if self.endSign != '=':
                    where_chunks.append(self.query_chunk_end_date())
            else:
                where_chunks.append(self.query_chunk_end_date())

        if self.duration is not None:
            duration_chunks = self.query_chunk_duration("durations")
            
            tables.append(duration_chunks[0])
            with_chunks.append(duration_chunks[1])
            where_chunks.append(duration_chunks[2])
            where_chunks.append(f" {duration_chunks[0]}.trip_id = trips.trip_id ")

        if self.route is not None:
            where_chunks.append(self.query_chunk_route())

        global date
        if date != "--/--/----":
            where_chunks.append(f" start_date::date = '{date}' ")

        return tables, with_chunks, where_chunks        


    def generate_query(self):
        base_query = " SELECT DISTINCT trips.trip_id, start_date, end_date, points FROM "
        tables, with_chunks, where_chunks = self.query_chunks()

        query = ""

        # WITH ...
        if len(with_chunks) > 0:
            query += " WITH "
            for i in range(len(with_chunks)):
                query += with_chunks[i]
                if i < len(with_chunks) - 1:
                    query += " , "

        # SELECT ...
        query += base_query

        # FROM ...
        if len(tables) > 0:
            for i in range(len(tables)):
                query += tables[i]
                if i < len(tables) - 1:
                    query += " , "

        # WHERE ...
        if len(where_chunks) > 0:
            query += " WHERE "
            for i in range(len(where_chunks)):
                query += where_chunks[i]
                if i < len(where_chunks) - 1:
                    query += " AND "

        #print(query)
        self.query =  query



def fetch_from_db(cur, items, debug = False):
    results = []
    moreResults = []
    all = []
    segments = []

    size = len(items)
    if size > 1:
        template = "SELECT %s " \
                   " FROM (%s) q1 INNER JOIN (%s) q2 ON q1.end_date = q2.start_date " \
                   " INNER JOIN (%s) q3 ON q2.end_date = q3.start_date "

        add_template = " INNER JOIN (%s) q%s ON q%s.end_date = q%s.start_date " \
                       " INNER JOIN (%s) q%s ON q%s.end_date = q%s.start_date "

        select = ""
        for i in range(1, size+1):
            if i % 2 == 0:
                select += "q"+str(i)+".trip_id,q"+str(i)+".start_date,q"+str(i)+".end_date,q"+str(i)+".points, "
            else:
                select += "q"+str(i)+".location_label,q"+str(i)+".start_date,q"+str(i)+".end_date,q"+str(i)+".centroid, "
        select = select.rstrip(', ')

        template = template%(select, items[0].get_query(),items[1].get_query(), items[2].get_query())

        if size > 3:
            id = 4
            how_many = math.floor((size-3)/2)
            for j in range(1, how_many+1):
                if id <= size:
                    template += add_template%(items[id-1].get_query(), id, id-1, id, items[id].get_query(), id+1, id, id+1)
                    id += 2
    elif size == 1:
        template = items[0].get_query()
    else:
        if debug:
            print("Empty query")
        return {"results": [], "segments": []}


    try:
        if debug:
            print("-------query------- ")
            print(template)
            print("--------------")
        
        cur.execute(template)
        temp = cur.fetchall()

        for result in temp:
            for i in range(0, size*4, 4):
                id = result[i]
                start_date = result[i+1]
                end_date = result[i+2]
                points = db.to_segment(result[i+3], debug=debug).to_json()
                points['id'] = id

                if size == 1: # only stay's location
                    segments.append(points)
                elif (i/4) % 2 != 0: # route points
                    segments.append(points)
                
                try:
                    int(id)
                    results.append(ResultInterval(id, start_date, end_date, None))
                except ValueError:
                    results.append(ResultRange(id, start_date, end_date, None))
                    
            all.append(results)
            results = []
    except psycopg2.ProgrammingError as e:
        print(("error ", e))

    size2 = len(all)

    to_show = all

    to_show = utils.refine_with_group_by(to_show)
    to_show = utils.refine_with_group_by_date(to_show)

    for key, value in list(to_show.items()):
        temp = value
        moreResults.append(temp)


    id = 0
    for key, value in list(to_show.items()):
        to_show[id] = to_show.pop(key)
        id += 1


    summary = utils.quartiles(to_show, size2)

    i = 0
    end = []
    for key, value in list(summary.items()):
        stays = []
        routes = []
        if value != []:
            for item in value:
                if utils.represent_int(item[2]):
                    temp = ResultInterval(item[2], item[0], item[1], item[3], i)
                    routes.append({"start": True, "time":item[0]})
                    routes.append({"start": False, "time": item[1]})
                else:
                    temp = ResultRange(item[2], item[0], item[1], item[3], i)
                    stays.append({"start": True, "time":item[0]})
                    stays.append({"start": False, "time": item[1]})
                
            end.append(sort_data(stays, routes, i))
        
        i += 1

    print(end)
        
    return moreResults, {"results": end, "segments": segments}

def sort_data(stays, routes, moreResultsId):
    """TODO comments"""
    num = 0
    stays = sorted(stays, key=lambda d: d["time"].time()) 
    stays_freq = []
    for i in range(1, len(stays)):
        num = num + 1 if stays[i - 1]["start"] else num - 1
        stays_freq.append({"start": stays[i - 1]["time"], "end": stays[i]["time"], "freq": num})

    num = 0
    routes = sorted(routes, key=lambda d: d["time"].time()) 
    routes_freq = []
    for i in range(1, len(routes)):
        num = num + 1 if routes[i - 1]["start"] else num - 1
        routes_freq.append({"start": routes[i - 1]["time"], "end": routes[i]["time"], "freq": num})

    return {"stays": stays_freq, "routes": routes_freq, "moreResultsId": moreResultsId}


def generate_queries(items):
    for item in items:
        item.generate_query()


def parse_items(obj):
    items = []
    global date
    date = obj[0]["date"]

    iterobj = iter(obj) #skip the first, that is the date
    next(iterobj)

    for item in iterobj:
        if item.get("spatialRange") != None: #its a range
            global previousEndDate
            if len(items) == 0:
                previousEndDate = utils.get_all_but_sign(item["start"])
            else:
                previousEndDate = utils.get_all_but_sign(item["end"])
            items.append(Range(item["start"], item["end"], item["temporalStartRange"], item["temporalEndRange"], item["duration"], item["location"], item["spatialRange"]))
        else: #its an interval
            items.append(Interval(item["start"], item["end"], item["temporalStartRange"], item["temporalEndRange"], item["duration"], item["route"]))

    return items


class ResultRange:
    id = ""
    start_date = None
    end_date = None
    type = "range"
    date = None
    moreResultsId = None

    def __init__(self, id, start_date, end_date, date, moreResultsId = None):
        now = datetime.datetime.now()
        if date:
            self.date = date
        else:
            self.date = start_date.date()
        self.id = id
        self.start_date = start_date
        self.end_date = end_date
        self.moreResultsId = moreResultsId
        self.type = "range"

    def __repr__(self):
        return str(self.id) + " " + str(self.start_date) + " " + str(self.end_date) + " "

    def __hash__(self):
        return hash((self.start_date, self.end_date, self.id, self.type))

    def __eq__(self, other):
        return self.start_date == other.start_date and self.end_date == other.end_date and self.id == other.id and self.type == other.type

    def to_json(self):
        return {
            'id': self.id, 
            'moreResultsId': self.moreResultsId,
            'date': self.date, 
            'start_date': self.start_date, 
            'end_date': self.end_date, 
            'type': self.type 
        }

class ResultInterval:
    id = ""
    start_date = None
    end_date = None
    type = "interval"
    date = None
    moreResultsId = None

    def __init__(self, id, start_date, end_date, date, moreResultsId = None):
        now = datetime.datetime.now()
        if date:
            self.date = date
        else:
            self.date = start_date.date()
        self.id = id
        self.start_date = start_date
        self.end_date = end_date
        self.moreResultsId = moreResultsId
        self.type = "interval"

    def __repr__(self):
        return str(self.id) + " " + str(self.start_date) + " " + str(self.end_date) + " "

    def __hash__(self):
        return hash((self.start_date, self.end_date, self.id, self.type))

    def __eq__(self, other):
        return self.start_date == other.start_date and self.end_date == other.end_date and self.id == other.id and self.type == other.type

    def to_json(self):
        return {
            'id': self.id, 
            'moreResultsId': self.moreResultsId,
            'date': self.date, 
            'start_date': self.start_date, 
            'end_date': self.end_date, 
            'type': self.type 
        }
   

