# -*- coding: utf-8 -*-
"""
Contains class that orchestrates queries
"""

import json
import datetime
import math
import psycopg2
import queries.utils as utils 
from utils import Manager

class QueryManager(Manager):
    """ Manages queries

    Arguments:
        currentQuerySize: Number of JSON objects, either Stays or Routes, that compose the query
        loadMoreId: latest ID for a chunk of results (based on number of results loaded at a time) 
    """
    def __init__(self, config_file, debug):
        super().__init__(config_file, debug)
        self.currentQuerySize = 0
        self.loadMoreId = 0

    def execute_query(self, payload):
        """ Receives a query JSON object and executes the query in the database

        Args:
            payload (:obj:`dict`): contains the query (data) and if all results should be loaded at once (loadAll)

        Returns:
            :obj:`dict`: JSON object containing loaded results, the total size of the results and the query size
        """
        conn, cur = self.db_connect()

        items = self.parse_items(payload["data"])
        self.generate_queries(items)
        return self.fetch_from_db(cur, items, payload["loadAll"], self.debug)

    def fetch_from_db(self, cur, items, loadAll, debug = False):
        """ Composes the SQL query based on the number of items on the query object and formats the results

        Args:
            cur (psycopg2.cursor)
            items (:obj:`list` of :obj:`Range` and/or :obj:`Interval`):
            loadAll (bool): defines if results should be loaded in segments or simultaneously
            debug (bool, optional): activates debug mode. 
                Defaults to False
        Returns:
            :obj:`dict`: JSON object containing loaded results, the total size of the results and the query size
        """
        results = []
        segments = []
        to_show= []

        self.loadMoreId = 0
        self.currentQuerySize = len(items)

        if self.currentQuerySize > 1:
            template = "SELECT %s " \
                    " FROM (%s) q1 INNER JOIN (%s) q2 ON q1.end_date = q2.start_date " \
                    " INNER JOIN (%s) q3 ON q2.end_date = q3.start_date "

            add_template = " INNER JOIN (%s) q%s ON q%s.end_date = q%s.start_date " \
                        " INNER JOIN (%s) q%s ON q%s.end_date = q%s.start_date "

            select = ""
            for i in range(1, self.currentQuerySize+1):
                if i % 2 == 0:
                    select += "q"+str(i)+".trip_id,q"+str(i)+".start_date,q"+str(i)+".end_date, q"+str(i)+".points,q"+str(i)+".timestamps, "
                else:
                    select += "q"+str(i)+".stay_id,q"+str(i)+".start_date,q"+str(i)+".end_date, q"+str(i)+".centroid,q"+str(i)+".label, "
            select = select.rstrip(', ')

            template = template%(select, items[0].get_query(),items[1].get_query(), items[2].get_query())

            if self.currentQuerySize > 3:
                id = 4
                how_many = math.floor((self.currentQuerySize-3)/2)
                for j in range(1, how_many+1):
                    if id <= self.currentQuerySize:
                        template += add_template%(items[id-1].get_query(), id, id-1, id, items[id].get_query(), id+1, id, id+1)
                        id += 2
        elif self.currentQuerySize == 1:
            template = items[0].get_query()
        else:
            if debug:
                print("Empty query")
            return {"results": [], "segments": []}


        try:
            if debug:
                print("-------query-------")
                print(template)
                print("-------------------")
            
            cur.execute(template)
            temp = cur.fetchall()

            for result in temp:
                for i in range(0, self.currentQuerySize*5, 5):
                    id = result[i]
                    start_date = result[i+1]
                    end_date = result[i+2]
                    points = {"id": start_date, "geoJSON": json.loads(result[i+3])}

                    if (i/5) % 2 != 0: # route points
                        results.append(ResultInterval(id, start_date, end_date, None, points))
                    else: # stay
                        points["label"] = result[i+4]
                        results.append(ResultRange(result[i+4], start_date, end_date, None, points))
                    
                to_show.append(results)
                results = []
        except psycopg2.ProgrammingError as e:
            print(("error ", e))

        size = len(to_show)

        to_show = utils.refine_with_group_by(to_show)
        to_show = utils.refine_with_group_by_date(to_show)

        id = 0
        for key, value in list(to_show.items()):
            to_show[id] = to_show.pop(key)
            id += 1

        self.allResults = utils.quartiles(to_show, size)
        self.allSegments = segments

        return self.load_results(loadAll)

    def load_results(self, loadAll):
        """ Handles how results are loaded, whether they are loaded all at once or in chunks

        Args:
            loadAll (bool): defines if results should be loaded in segments or simultaneously
        Returns:
            :obj:`dict`: JSON object containing loaded results, the total size of the results and the query size
        """
        self.numResults = int(self.config['load_more_amount'])

        i = 0
        results = []
        
        if (loadAll):
            loadedResults = list(self.allResults.items())
        else:
            start_index  = self.loadMoreId * self.numResults
            end_index  = self.loadMoreId * self.numResults + self.numResults
            end_index = len(self.allResults) if end_index >= len(self.allResults) else end_index

            loadedResults = list(self.allResults.items())[start_index : end_index] if self.loadMoreId * self.numResults < len(self.allResults) else []

        for key, value in loadedResults:
            stays, routes, result = [], [], []
            if value != []:
                for item in value:
                    if utils.represent_int(item[2]):
                        id = item[2]
                        result.append(ResultInterval(id, item[0], item[1], item[3], item[4]).to_json())
                        routes.append({"start": True, "time":item[0], "location": item[2]})
                        routes.append({"start": False, "time": item[1], "location": item[2]})
                    else:
                        id = item[2] + str(start_index + i if not loadAll else i)
                        result.append(ResultRange(item[2], item[0], item[1], item[3], item[4]).to_json())
                        stays.append({"start": True, "time":item[0], "location": item[2]})
                        stays.append({"start": False, "time": item[1], "location": item[2]})
                    
                results.append({"id": id, "result": result, "render": self.sort_render_data(stays, routes), "multiple": len(result) > self.currentQuerySize, "querySize": self.currentQuerySize})
            i += 1

        self.loadMoreId += 1

        return {"results": results, "total": len(self.allResults), "querySize": self.currentQuerySize}

    def sort_render_data(self, stays, routes):
        """ Creates an object to visually represent stays and routes based on how the overlap

        Args:
            stays (:obj:`list`: of :obj:`dict`)
            routes (:obj:`list`: of :obj:`dict`)
        Returns:
            :obj:`list`: of :obj:`dict`
        """
        num = 0
        stays = sorted(stays, key=lambda d: d["time"].time()) 
        stays_freq = []
        for i in range(1, len(stays)):
            num = num + 1 if stays[i - 1]["start"] else num - 1
            stays_freq.append({"start": stays[i - 1]["time"], "end": stays[i]["time"], "freq": num, "location": stays[i - 1]["location"], "id": i})

        num = 0
        routes = sorted(routes, key=lambda d: d["time"].time()) 
        routes_freq = []
        for i in range(1, len(routes)):
            num = num + 1 if routes[i - 1]["start"] else num - 1
            routes_freq.append({"start": routes[i - 1]["time"], "end": routes[i]["time"], "freq": num, "location": routes[i - 1]["location"], "id": i})

        return {"stays": stays_freq, "routes": routes_freq}


    def generate_queries(self, items):
        """ Iterates through list of Range and Interval objects and generates their query
        
        Args:
            items(:obj:`list` of :obj:`Range` and/or :obj:`Interval`)
        """

        for item in items:
            item.generate_query()


    def parse_items(self, obj):
        """ Parses the JSON object that corresponds to a query in order to format or remove incomplete data

        Args:
            obj (:obj:`list` of :obj:`dict`)
        Returns:
            :obj:`list` of :obj:`Range` and/or :obj:`Interval`
        """
        items = []
        global date
        date = obj[0]["date"]

        iterobj = iter(obj) #skip the first, that is the date
        next(iterobj)

        for item in iterobj:
            if item.get("spatialRange") != None: #its a range
                global previousEndDate
                if len(items) == 0:
                    previousEndDate = utils.get_all_but_symbol(item["start"])
                else:
                    previousEndDate = utils.get_all_but_symbol(item["end"])
                items.append(Range(item["start"], item["end"], item["temporalStartRange"], item["temporalEndRange"], item["duration"], item["location"], item["spatialRange"]))
            else: #its an interval
                items.append(Interval(item["start"], item["end"], item["temporalStartRange"], item["temporalEndRange"], item["duration"], item["route"]))

        return items

class Range:
    """ Defines a period of time that was spent at a certain location"""
    start = ""
    end = ""
    temporalStartRange = 0 #In minutes
    temporalEndRange = 0 #In minutes
    duration = "" #Example: 6h15m or 6h or 5m
    location = "" #Location name
    locationCoords = "" #Format: lat,lon
    spatialRange = 0 #In meters
    # Symbol can be =, <, ≤, > or ≥
    durationSymbol = "" 
    startSymbol = ""
    endSymbol = ""
    spatialSymbol = ""
    fullDate = ""
    castTime = ""
    query = ""

    def get_query(self):
        """Returns generated query
        
        Returns:
            str
        """
        return self.query

    def has_value(self, value):
        """ Checks if value string is not empty

        Args:
            value (str)
        Returns:
            bool
        """
        return value.strip() != ""

    def __init__(self, start, end, temporalStartRange, temporalEndRange, duration, location, spatialRange):
        global date
        global previousEndDate

        self.fullDate, self.castTime = utils.is_full_date(date)

        if self.has_value(start):
            self.start = utils.join_date_time(date, start)
            self.startSymbol = utils.get_symbol(start)
        else:
            self.start = None
            self.startSymbol = None

        if self.has_value(end):
            self.end = utils.join_date_time(date, end)
            self.endSymbol = utils.get_symbol(end)
        else:
            self.end = None
            self.endSymbol = None

        if(self.start is not None and utils.get_all_but_symbol(start) < previousEndDate and date != "--/--/----"):
            self.start += datetime.timedelta(days=1)
        if(self.end is not None and utils.get_all_but_symbol(end) < previousEndDate and date != "--/--/----"):
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
            self.durationSymbol = utils.get_symbol(duration)
        else:
            self.duration = None
            self.durationSymbol = None

        if self.has_value(spatialRange):
            self.spatialRange = utils.spatial_range_to_meters(spatialRange)
            self.spatialSymbol = utils.get_symbol(spatialRange)
        else:
            self.spatialRange = None
            self.spatialSymbol = ""

        if self.has_value(location):
            if utils.is_coordinates(location):
                self.locationCoords = utils.switch_coordinates(location)
                self.location = None
                if self.spatialRange is None:
                    self.spatialRange = 0
                    self.spatialSymbol = "="
            else:
                self.locationCoords = None
                self.location = location
                if self.spatialRange is None:
                    self.spatialRange = 0
                    self.spatialSymbol = "="
        else:
            self.location = None
            self.locationCoords = None

    def query_chunk_date(self, type, cast, symbol, date):
        """ Creates a query chunk related to dates

        Args:
            type (str): defines the parameter
            cast (str): defines the part of the date to use
            symbol (str): either =, <, ≤, > or ≥
            date (str)
        Returns:
            str: formatted string that represents part of a query
        """
        
        return f" {type}{cast} {symbol} '{date}' "

    def query_chunk_start_date(self):
        """ Creates a query chunk that compares the start date """

        return self.query_chunk_date('start_date', self.castTime, self.startSymbol, self.start)

    def query_chunk_end_date(self):
        """ Creates a query chunk that compares the end date """

        return self.query_chunk_date('end_date', self.castTime, self.endSymbol, self.end)
    
    def query_chunk_interval(self, type, cast, dateType, temporalRange, date):
        """ Creates a query chunk related to an interval of time

        Args:
            type (str): defines the parameter
            cast (str): defines the part of the date to use
            dateType (str): defines the type of date to use
            temporalRange (str): defines the number of minutes that will be used to create the start/end range of values that are also considered
            date (str)
        Returns:
            str: formatted string that represents part of a query
        """
        return f" {type}{cast} BETWEEN CAST('{date}' AS {dateType}) - CAST('{temporalRange}' AS INTERVAL) AND CAST('{date}' AS {dateType}) + CAST('{temporalRange}' AS INTERVAL) "

    def query_chunk_start_interval(self):
        """ Creates a query chunk that compares the start date using an interval of time"""

        return self.query_chunk_interval('start_date', self.castTime, self.fullDate, self.temporalStartRange, self.start)

    def query_chunk_end_interval(self):
        """ Creates a query chunk that compares the end date using an interval of time"""

        return self.query_chunk_interval('end_date', self.castTime, self.fullDate, self.temporalEndRange, self.end)

    def query_chunk_duration(self, table_name):
        """ Creates a query chunk related to a duration of time

        Args:
            table_name (str): defines the name of the table that will be created to calculate the durations
        Returns:
            str: formatted string that represents part of a query
        """
        with_chunk = f" {table_name} AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration, stay_id FROM stays) "
        where_chunk = f" {table_name}.duration {self.durationSymbol} '{self.duration}' "

        return (table_name, with_chunk, where_chunk)

    def query_chunk_location(self, table_name):
        """ Creates a query chunk related to named locations

        Args:
            table_name (str): defines the name of the table that will be created to calculate the location distances
        Returns:
            str: formatted string that represents part of a query
        """
        with_chunk = f" {table_name} AS (SELECT centroid FROM locations WHERE label = '{self.location}') "
        where_chunk = f" ST_DISTANCE({table_name}.centroid, locations.centroid) {self.spatialSymbol} '{self.spatialRange}' "

        return (table_name, with_chunk, where_chunk)

    def query_chunk_coords(self, table_name):
        """ Creates a query chunk related to location coordinates

        Args:
            table_name (str): defines the name of the table that will be created to calculate the location distances
        Returns:
            str: formatted string that represents part of a query
        """

        with_chunk = f" {table_name} AS (SELECT label FROM locations WHERE ST_Distance(centroid, ST_SetSRID(ST_MakePoint({self.locationCoords}),4326)) {self.spatialSymbol} '{self.spatialRange}' ) "
        where_chunk = f" {table_name}.label = stays.location_label "

        return (table_name, with_chunk, where_chunk)

    def query_chunks(self):
        """ Composes a tuple with query chunks that are derived from the query objects' parameters

        Returns: 
            (:obj:`list` of str, :obj:`list` of str, :obj:`list` of str): contains table names and chunks for the with and where sections of a SQL query
        """

        with_chunks = []
        where_chunks = [" locations.label = stays.location_label "]
        tables = ["stays", "locations"]

        if self.start is not None:
            if self.temporalStartRange is not None:
                where_chunks.append(self.query_chunk_start_interval())
                if self.startSymbol != '=':
                    where_chunks.append(self.query_chunk_start_date())
            else:
                where_chunks.append(self.query_chunk_start_date())

        if self.end is not None:
            if self.temporalEndRange is not None:
                where_chunks.append(self.query_chunk_end_interval())
                if self.endSymbol != '=':
                    where_chunks.append(self.query_chunk_end_date())
            else:
                where_chunks.append(self.query_chunk_end_date())

        if self.duration is not None:
            duration_chunks = self.query_chunk_duration("durations")
            
            tables.append(duration_chunks[0])
            with_chunks.append(duration_chunks[1])
            where_chunks.append(duration_chunks[2])
            where_chunks.append(f" {duration_chunks[0]}.stay_id = stays.stay_id ")

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
            date = datetime.datetime.strptime(date,  "%d/%m/%Y")
            date = datetime.datetime.strftime(date, "%Y-%m-%d")
            where_chunks.append(f" start_date::date = '{date}' ")

        return tables, with_chunks, where_chunks        


    def generate_query(self):
        """ Composes a SQL query string using the query chunks that have been derived from the JSON query object """

        base_query = " SELECT DISTINCT stays.stay_id, start_date, end_date, ST_AsGEOJson(locations.centroid) as centroid, locations.label "

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
            query += " FROM "
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

        self.query =  query

class Interval:
    """ Defines a period of time that was spent between locations """
    start = ""
    end = ""
    temporalStartRange = 0 #In minutes
    temporalEndRange = 0 #In minutes
    duration = ""  #Example: 6h15m or 6h or 5m
    route = ""
    # Symbol can be =, <, ≤, > or ≥
    durationSymbol = ""
    startSymbol = ""
    endSymbol = ""
    fullDate = ""
    castTime = ""
    query = ""

    def get_query(self):
        """Returns generated query
        
        Returns:
            str
        """
        return self.query

    def has_value(self, value):
        """ Checks if value string is not empty

        Args:
            value (str)
        Returns:
            bool
        """
        return value.strip() != ""

    def __init__(self, start, end, temporalStartRange, temporalEndRange, duration, route):
        global date
        global previousEndDate

        self.fullDate, self.castTime = utils.is_full_date(date)

        if self.has_value(start):
            self.start = utils.join_date_time(date, start)
            self.startSymbol = utils.get_symbol(start)
        else:
            self.start = None
            self.startSymbol = None

        if self.has_value(end):
            self.end = utils.join_date_time(date, end)
            self.endSymbol = utils.get_symbol(end)
        else:
            self.end = None
            self.endSymbol = None

        if(self.start is not None and utils.get_all_but_symbol(start) < previousEndDate):
            self.start += datetime.timedelta(days=1)
        if(self.end is not None and utils.get_all_but_symbol(end) < previousEndDate):
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
            self.durationSymbol = utils.get_symbol(duration)
        else:
            self.duration = None
            self.durationSymbol = None

        if self.has_value(route) and utils.is_coordinates(route):
            self.route = utils.switch_coordinates(route)
        else:
            self.route = None


    def query_chunk_date(self, type, cast, symbol, date):
        """ Creates a query chunk related to dates

        Args:
            type (str): defines the parameter
            cast (str): defines the part of the date to use
            symbol (str): either =, <, ≤, > or ≥
            date (str)
        Returns:
            str: formatted string that represents part of a query
        """
        return f" {type}{cast} {symbol} '{date}' "

    def query_chunk_start_date(self):
        """ Creates a query chunk that compares the start date """

        return self.query_chunk_date('start_date', self.castTime, self.startSymbol, self.start)

    def query_chunk_end_date(self):
        """ Creates a query chunk that compares the end date """

        return self.query_chunk_date('end_date', self.castTime, self.endSymbol, self.end)
    
    def query_chunk_interval(self, type, cast, dateType, temporalRange, date):
        """ Creates a query chunk related to an interval of time

        Args:
            type (str): defines the parameter
            cast (str): defines the part of the date to use
            dateType (str): defines the type of date to use
            temporalRange (str): defines the number of minutes that will be used to create the start/end range of values that are also considered
            date (str)
        Returns:
            str: formatted string that represents part of a query
        """
        return f" {type}{cast} BETWEEN CAST('{date}' AS {dateType}) - CAST('{temporalRange}' AS INTERVAL) AND CAST('{date}' AS {dateType}) + CAST('{temporalRange}' AS INTERVAL) "

    def query_chunk_start_interval(self):
        """ Creates a query chunk that compares the start date using an interval of time"""

        return self.query_chunk_interval('start_date', self.castTime, self.fullDate, self.temporalStartRange, self.start)

    def query_chunk_end_interval(self):
        """ Creates a query chunk that compares the end date using an interval of time"""

        return self.query_chunk_interval('end_date', self.castTime, self.fullDate, self.temporalEndRange, self.end)

    def query_chunk_duration(self, table_name):
        """ Creates a query chunk related to a duration of time

        Args:
            table_name (str): defines the name of the table that will be created to calculate the durations
        Returns:
            str: formatted string that represents part of a query
        """
        with_chunk = f" {table_name} AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration, trip_id FROM trips) "
        where_chunk = f" {table_name}.duration {self.durationSymbol} '{self.duration}' "

        return (table_name, with_chunk, where_chunk)

    def query_chunk_route(self):
        """ Creates a query chunk related to the route

        Returns:
            str: formatted string that represents part of a query
        """
        return f" ST_DWithin(points, ST_SetSRID(ST_MakePoint({self.route}),4326)::geography, 250) "

    def query_chunks(self):
        """ Composes a tuple with query chunks that are derived from the query objects' parameters

        Returns: 
            (:obj:`list` of str, :obj:`list` of str, :obj:`list` of str): contains table names and chunks for the with and where sections of a SQL query
        """

        with_chunks = []
        where_chunks = []
        tables = ["trips"]

        if self.start is not None:
            if self.temporalStartRange is not None:
                where_chunks.append(self.query_chunk_start_interval())
                if self.startSymbol != '=':
                    where_chunks.append(self.query_chunk_start_date())
            else:
                where_chunks.append(self.query_chunk_start_date())

        if self.end is not None:
            if self.temporalEndRange is not None:
                where_chunks.append(self.query_chunk_end_interval())
                if self.endSymbol != '=':
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
            date = datetime.datetime.strptime(date,  "%d/%m/%Y")
            date = datetime.datetime.strftime(date, "%Y-%m-%d")
            where_chunks.append(f" start_date::date = '{date}' ")

        return tables, with_chunks, where_chunks        


    def generate_query(self):
        """ Composes a SQL query string using the query chunks that have been derived from the JSON query object """

        base_query = " SELECT DISTINCT trips.trip_id, start_date, end_date, ST_AsGEOJson(points) as points, timestamps "
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
            query += " FROM "
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
class ResultRange:
    """ Represents a set of results that correpond to a period of time spent in a location"""
    
    id = ""
    start_date = None
    end_date = None
    type = "range"
    date = None
    points = []
    render = {}

    def __init__(self, id, start_date, end_date, date, points):
        now = datetime.datetime.now()
        if date:
            self.date = date
        else:
            self.date = start_date.date()
        self.id = id
        self.start_date = start_date
        self.end_date = end_date
        self.render = {"start": start_date, "end": end_date, "freq": 1, "location": id}
        self.points = points
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
            'date': self.date, 
            'start_date': self.start_date, 
            'end_date': self.end_date, 
            'type': self.type,
            'points': self.points,
            'render': self.render
        }

class ResultInterval:
    """ Represents a set of results that correpond to a period of time spent between locations"""

    id = ""
    start_date = None
    end_date = None
    type = "interval"
    date = None
    points=[]
    render = {}

    def __init__(self, id, start_date, end_date, date, points):
        now = datetime.datetime.now()
        if date:
            self.date = date
        else:
            self.date = start_date.date()
        self.id = id
        self.start_date = start_date
        self.end_date = end_date
        self.render = {"start": start_date, "end": end_date, "freq": 1, "location": id}
        self.points = points
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
            'date': self.date, 
            'start_date': self.start_date, 
            'end_date': self.end_date, 
            'type': self.type,
            'points': self.points,
            'render': self.render
        }
   

