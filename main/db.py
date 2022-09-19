"""
Database related functions
"""
import datetime
import json
import ppygis3
import psycopg2

from psycopg2.extensions import AsIs, register_adapter
from tracktotrip3 import Segment, Point
from tracktotrip3.location import update_location_centroid
from life.life import Life

def adapt_point(point):
    """ Adapts a `tracktotrip3.Point` to use with `psycopg` methods

    Params:
        points (:obj:`tracktotrip3.Point`)
    """
    return AsIs("'SRID=%s;POINT(%s %s 0)'" % (4326, point.lon, point.lat))

def to_point(gis_point, time=None, debug = False):
    """ Creates from raw ppygis representation

    Args:
        gis_point
        timestamp (:obj:`datatime.datetime`, optional, debug = False): timestamp to use
            Defaults to none (point will have empty timestamp)
            debug (bool, optional): activates debug mode. 
                Defaults to False
    Returns:
        :obj:`tracktotrip3.Point`
    """
    gis_point = ppygis3.Geometry.read_ewkb(gis_point)
    return Point(gis_point.y, gis_point.x, time, debug)

def to_segment(gis_points, timestamps=None, debug = False):
    """ Creates from raw ppygis representation

    Args:
        gis_points
        timestamps (:obj:`list` of :obj:`datatime.datetime`, optional): timestamps to use
            Defaults to none (all points will have empty timestamps)
        debug (bool, optional): activates debug mode. 
            Defaults to False
    Returns:
        :obj:`tracktotrip3.Segment`
    """
    gis_geometry = ppygis3.Geometry.read_ewkb(gis_points)
    result = []
    
    if type(gis_geometry) is ppygis3.Point:
        result.append(Point(gis_geometry.y, gis_geometry.x, timestamps, debug))
    else:
        gis_points = gis_geometry.points

        for i, point in enumerate(gis_points):
            tmstmp = timestamps[i] if timestamps is not None else None
            result.append(Point(point.y, point.x, tmstmp, debug))

    return Segment(result, debug)

def adapt_segment(segment, debug = False):
    """ Adapts a `tracktotrip3.Segment` to use with `psycopg` methods

    Args:
        segment (:obj:`tracktotrip3.Segment`)
        debug (bool, optional): activates debug mode. 
            Defaults to False
    """
    points = ""

    for p in segment.points:
        points += ("%s %s 0, " % (p.lon, p.lat))

    points = points.removesuffix(", ")

    return  AsIs("'LINESTRING(%s)'" % (points))


register_adapter(Point, adapt_point)
register_adapter(Segment, adapt_segment)

def span_date_to_datetime(date, minutes, debug = False):
    """ Converts date string and minutes to datetime

    Args:
        date (str): Date in the `%Y_%m_%d` format
        minutes (int): Minutes since midnight
        debug (bool, optional): activates debug mode. 
            Defaults to False
    Returns:
        :obj:`datetime.datetime`
    """
    date_format = "%Y_%m_%d %H%M"
    str_date = "%s %02d%02d" % (date, minutes/60, minutes%60)
    return datetime.datetime.strptime(str_date, date_format)

def get_day_from_life(life, track, debug = False):
    """ Extracts the day from a `life.Life` object

    Args:
        life (:obj:`life.Life`)
        track (:obj:`tracktotrip3.Segment`)
        debug (bool, optional): activates debug mode. 
            Defaults to False
    Returns:
        str or None
    """
    track_time = track.segments[0].points[0].time
    track_day = "%d_%d_%d" % (track_time.year, track_time.month, track_time.day)
    for day in life.days:
        if day == track_day:
            return day
    return None

def life_date(point, debug = False):
    """ Convert point date into LIFE date format

    Args:
        point (:obj:`tracktotrip3.Point`) 
        debug (bool, optional): activates debug mode. 
            Defaults to False
    """
    date = point.time.date()
    return "%d_%02d_%02d" % (date.year, date.month, date.day)

def life_time(point, debug = False):
    """ Convert point date into LIFE time format

    Args:
        point (:obj:`tracktotrip3.Point`) 
        debug (bool, optional): activates debug mode. 
            Defaults to False
    """
    time = point.time.time()
    return "%02d%02d" % (time.hour, time.minute)

def load_from_segments_annotated(cur, track, life_content, max_distance, min_samples, insert_locs = True, debug = False):
    """ Uses a LIFE formated string to populate the database

    Args:
        cur (:obj:`psycopg2.cursor`)
        track (:obj:`tracktotrip3.Track`)
        life_content (str): LIFE formatted string
        max_distance (float): Max location distance. See
            `tracktotrip3.location.update_location_centroid`
        min_samples (float): Minimum samples requires for location.  See
            `tracktotrip3.location.update_location_centroid`
        insert_locs (bool): Determines whether locations are inserted into database
            Defaults to True
        debug (bool, optional): activates debug mode. 
            Defaults to False
    """
    
    life = Life()
    life.from_string(life_content.split('\n'))

    def in_loc(points, start, end):
        """ Inserts locations into database
        
        See `insert_location`

        Args:
            points (:obj:`dict`)
            start (int)
            end (int)
        """
        startPoint = points[start]
        endPoint = points[end]
        startLocation = life.where_when(life_date(startPoint, debug), life_time(startPoint, debug))
        endLocation = life.where_when(life_date(endPoint, debug), life_time(endPoint, debug))

        if (debug):
            print(('in_loc', start, startLocation, startPoint.lat, startPoint.lon))
            print(('in_loc', end, endLocation, endPoint.lat, endPoint.lon))
        
        if startLocation is not None:
            if isinstance(startLocation, str): 
                if startLocation != '#?':
                    insert_location(cur, startLocation, startPoint, max_distance, min_samples, debug)
            else: # is a tuple with (start, end) -> multiplace
                if startLocation[0] != '#?':
                    insert_location(cur, startLocation[0], startPoint, max_distance, min_samples, debug)
                if endLocation is not None and startLocation[1] != '#?':
                    insert_location(cur, startLocation[1], endPoint, max_distance, min_samples, debug)

        if endLocation is not None:
            if isinstance(endLocation, str):
                if endLocation != '#?':
                    insert_location(cur, endLocation, endPoint, max_distance, min_samples, debug)
            else: # is a tuple with (start, end) -> multiplace
                if startLocation is not None and endLocation[0] != '#?':
                    insert_location(cur, endLocation[0], startPoint, max_distance, min_samples, debug)
                if endLocation[1] != '#?':
                    insert_location(cur, endLocation[1], endPoint, max_distance, min_samples, debug)

    if insert_locs:
        for segment in track.segments:
            in_loc(segment.points, 0, -1)

    # Insert stays
    for day in life.days:
        date = day.date
        for span in day.spans:
            start = span_date_to_datetime(date, span.start, debug)
            end = span_date_to_datetime(date, span.end, debug)

            if isinstance(span.place, str):
                insert_stay(cur, span.place, start, end, debug)

    # Insert canonical places
    for place, (lat, lon) in list(life.coordinates.items()):
        if isinstance(place, str) and place != '#?':
            insert_location(cur, place, Point(lat, lon, None, debug), max_distance, min_samples, debug)


def load_from_life(cur, content, max_distance, min_samples, debug = False):
    """ Uses a LIFE formated string to populate the database

    Args:
        cur (:obj:`psycopg2.cursor`)
        content (str): LIFE formatted string
        max_distance (float): Max location distance. See
            `tracktotrip3.location.update_location_centroid`
        min_samples (float): Minimum samples requires for location.  See
            `tracktotrip3.location.update_location_centroid`
        debug (bool, optional): activates debug mode. 
            Defaults to False    
    """
    life = Life()
    life.from_string(content.split('\n'))

    # Insert canonical places
    for place, (lat, lon) in list(life.coordinates.items()):
        if isinstance(place, str) and place != '#?':
            insert_location(cur, place, Point(lat, lon, None, debug), max_distance, min_samples, debug)

    # Insert stays
    for day in life.days:
        date = day.date
        for span in day.spans:
            start = span_date_to_datetime(date, span.start, debug)
            end = span_date_to_datetime(date, span.end, debug)

            if isinstance(span.place, str):
                insert_stay(cur, span.place, start, end, debug)


def connect_db(host, name, user, port, password):
    """ Connects to database

    Args:
        host (str)
        name (str)
        user (str)
        port (str)
        password (str)
    Returns:
        :obj:`psycopg2.connection` or None
    """
    try:
        if host != None and name != None and user != None and password != None:
            return psycopg2.connect(
                host=host,
                database=name,
                user=user,
                password=password,
                port=port
            )
    except psycopg2.Error:
        pass
    return None

def dispose(conn, cur):
    """ Disposes a connection

    Args:
        conn (:obj:`psycopg2.connection`): Connection
        cur (:obj:`psycopg2.cursor`): Cursor
    """
    if conn:
        conn.commit()
        if cur:
            cur.close()
        conn.close()
    elif cur:
        cur.close()


def gis_bounds(bounds, debug = False):
    """ Converts bounds to its representation

    Args:
        bounds ((float, float, float, float))
        debug (bool, optional): activates debug mode. 
            Defaults to False
    """

    points = ''
    points += ("%s %s 0, " % (bounds[0], bounds[1]))
    points += ("%s %s 0, " % (bounds[0], bounds[3]))
    points += ("%s %s 0, " % (bounds[2], bounds[1]))
    points += ("%s %s 0, " % (bounds[2], bounds[3]))
    points += ("%s %s 0"   % (bounds[0], bounds[1]))

    return AsIs("'POLYGON((%s))'" % (points))

def insert_location(cur, label, point, max_distance, min_samples, debug = False):
    """ Inserts a location into the database

    Args:
        cur (:obj:`psycopg2.cursor`)
        label (str): Location's name
        point (:obj:`Point`): Position marked with current label
        max_distance (float): Max location distance. See
            `tracktotrip3.location.update_location_centroid`
        min_samples (float): Minimum samples requires for location.  See
            `tracktotrip3.location.update_location_centroid`
        debug (bool, optional): activates debug mode. 
            Defaults to False
    """

    if (debug):
        print('Inserting location %s, %f, %f' % (label, point.lat, point.lon))

    cur.execute("""
            SELECT location_id, label, centroid, point_cluster
            FROM locations
            WHERE label=%s
            ORDER BY ST_Distance(centroid, %s)
            """, (label, point))
    if cur.rowcount > 0:
        # Updates current location set of points and centroid
        location_id, _, centroid, point_cluster = cur.fetchone()
        centroid = to_point(centroid, debug=debug)
        point_cluster = to_segment(point_cluster, debug=debug).points

        centroid, point_cluster = update_location_centroid(
            point,
            point_cluster,
            max_distance,
            min_samples,
            debug
        )

        cur.execute("""
                UPDATE locations
                SET centroid=%s, point_cluster=%s
                WHERE location_id=%s
                """, (centroid, Segment(point_cluster, debug), location_id))
    else:
        # Creates new location
        cur.execute("""
                INSERT INTO locations (label, centroid, point_cluster)
                VALUES (%s, %s, %s)
                """, (label, point, Segment([point, point], debug)))

def insert_stay(cur, label, start_date, end_date, debug = False):
    """ Inserts stay in the database

    Args:
        cur (:obj:`psycopg2.cursor`)
        label (str): Location
        start_date (:obj:`datetime.datetime`)
        end_date (:obj:`datetime.datetime`)
        debug (bool, optional): activates debug mode. 
            Defaults to False
    """

    cur.execute("""
        INSERT INTO stays(location_label, start_date, end_date)
        VALUES (%s, %s, %s)
        """, (label, start_date, end_date))

def insert_segment(cur, segment, max_distance, min_samples, debug = False):
    """ Inserts segment in the database

    Args:
        cur (:obj:`psycopg2.cursor`)
        segment (:obj:`tracktotrip3.Segment`): Segment to insert
        max_distance (float): Max location distance. See
            `tracktotrip3.location.update_location_centroid`
        min_samples (float): Minimum samples requires for location.  See
            `tracktotrip3.location.update_location_centroid`
        debug (bool, optional): activates debug mode. 
            Defaults to False
    Returns:
        int: Segment id
    """

    tstamps = [p.time.replace(second=0, microsecond=0, tzinfo=None) for p in segment.points]

    cur.execute("""
            INSERT INTO trips (start_date, end_date, bounds, points, timestamps)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING trip_id
            """, (
                segment.points[0].time.replace(second=0, microsecond=0, tzinfo=None),
                segment.points[-1].time.replace(second=0, microsecond=0, tzinfo=None),
                gis_bounds(segment.bounds(), debug),
                segment,
                tstamps
            ))
    trip_id = cur.fetchone()
    trip_id = trip_id[0]

    return trip_id

def match_canonical_trip(cur, trip, distance, debug = False):
    """ Queries database for canonical trips with bounding boxes that intersect the bounding
        box of the given trip

    Args:
        cur (:obj:`psycopg2.cursor`)
        trip (:obj:`tracktotrip3.Segment`): Trip to match
        debug (bool, optional): activates debug mode. 
            Defaults to False
    """
    cur.execute("""
        SELECT canonical_id, points FROM canonical_trips WHERE bounds && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
        """ % trip.bounds(thr=distance))
    results = cur.fetchall()

    can_trips = []
    for (canonical_id, points) in results:
        segment = to_segment(points, debug=debug)
        can_trips.append((canonical_id, segment))

    return can_trips

def match_canonical_trip_bounds(cur, bounds, debug = False):
    """ Queries database for canonical trips with bounding boxes that intersect the bounding
        box of the given trip

    Args:
        cur (:obj:`psycopg2.cursor`)
        trip (:obj:`tracktotrip3.Segment`): Trip to match
        debug (bool, optional): activates debug mode. 
            Defaults to False
    Returns:
        :obj:`list` of (int, :obj:`tracktotrip3.Segment`, int): List of tuples with the id of
            the canonical trip, the segment representation and the number of times it appears
    """
    cur.execute("""
        SELECT can.canonical_id, can.points, COUNT(rels.trip)
        FROM canonical_trips AS can
            INNER JOIN canonical_trips_relations AS rels
                ON can.canonical_id = rels.canonical_trip
        WHERE bounds && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
        GROUP BY can.canonical_id
        """ % bounds)
    results = cur.fetchall()

    can_trips = []
    for (canonical_id, points, count) in results:
        segment = to_segment(points, debug=debug)
        can_trips.append((canonical_id, segment, count))

    return can_trips

def insert_canonical_trip(cur, can_trip, mother_trip_id, debug = False):
    """ Inserts a new canonical trip into the database

    It also creates a relation between the trip that originated
    the canonical representation and the representation

    Args:
        cur (:obj:`psycopg2.cursor`)
        can_trip (:obj:`tracktotrip3.Segment`): Canonical trip
        mother_trip_id (int): Id of the trip that originated the canonical representation
        debug (bool, optional): activates debug mode. 
            Defaults to False
    Returns:
        int: Canonical trip id
    """

    cur.execute("""
        INSERT INTO canonical_trips (bounds, points)
        VALUES (%s, %s)
        RETURNING canonical_id
        """, (
            gis_bounds(can_trip.bounds(), debug),
            Segment(can_trip.points, debug)
        ))
    result = cur.fetchone()
    c_trip_id = result[0]

    cur.execute("""
        INSERT INTO canonical_trips_relations (canonical_trip, trip)
        VALUES (%s, %s)
        """, (c_trip_id, mother_trip_id))

    return c_trip_id

def update_canonical_trip(cur, can_id, trip, mother_trip_id, debug = False):
    """ Updates a canonical trip

    Args:
        cur (:obj:`psycopg2.cursor`)
        can_id (int): canonical trip id to update
        trip (:obj:`tracktotrip3.Segment): canonical trip
        mother_trip_id (int): Id of trip that caused the update
        debug (bool, optional): activates debug mode. 
            Defaults to False
    """

    cur.execute("""
        UPDATE canonical_trips
        SET bounds=%s, points=%s
        WHERE canonical_id=%s
        """, (gis_bounds(trip.bounds(), debug), trip, can_id))

    cur.execute("""
        INSERT INTO canonical_trips_relations (canonical_trip, trip)
        VALUES (%s, %s)
        """, (can_id, mother_trip_id))

def query_locations(cur, lat, lon, radius, debug = False):
    """ Queries the database for location around a point location

    Args:
        cur (:obj:`psycopg2.cursor`)
        lat (float): Latitude
        lon (float): Longitude
        radius (float): Radius from the given point, in meters
        debug (bool, optional): activates debug mode. 
            Defaults to False
    Returns:
        :obj:`list` of (str, ?, ?): List of tuples with the label, the centroid, and the point
            cluster of the location. Centroid and point cluster need to be converted
    """
    cur.execute("""
        SELECT label, centroid, point_cluster
        FROM locations
        WHERE ST_DWithin(centroid, %s, %s)
        ORDER BY ST_Distance(centroid, %s)
        """, (Point(lat, lon, None, debug), radius * 4, Point(lat, lon, None, debug)))
        
    results = cur.fetchall()
    a = [
        (label, to_point(centroid, debug=debug), to_segment(cluster, debug=debug)) for (label, centroid, cluster) in results
    ]

    return a

def get_canonical_trips(cur, debug = False):
    """ Gets canonical trips

    Args:
        cur (:obj:`psycopg2.cursor`)
        debug (bool, optional): activates debug mode. 
            Defaults to False
    Returns:
        :obj:`list` of :obj:`dict`:
    """
    cur.execute("SELECT canonical_id, ST_AsGEOJson(points) FROM canonical_trips")
    trips = cur.fetchall()
    return [{'id': t[0], 'geoJSON': json.loads(t[1])} for t in trips]

def get_canonical_locations(cur, debug = False):
    """ Gets canonical trips

    Args:
        cur (:obj:`psycopg2.cursor`)
        debug (bool, optional): activates debug mode. 
            Defaults to False
    Returns:
        :obj:`list` of :obj:`dict`:
    """
    cur.execute("SELECT location_id, label, ST_AsGEOJson(centroid) FROM locations")
    locations = cur.fetchall()
    return [{'id': t[0], 'label': t[1], 'geoJSON': json.loads(t[2])} for t in locations]

def get_trips(cur, bounding_box, canonical=False, debug = False):
    """ Gets trips in db

    Args:
        cur (:obj:`psycopg2.cursor`)
        debug (bool, optional): activates debug mode. 
            Defaults to False
    Returns:
        :obj:`list` of :obj:`dict`
    """

    if canonical:
        cur.execute("""
            SELECT canonical_id, ST_AsGEOJson(points) FROM canonical_trips WHERE bounds && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
            """, (bounding_box[0]["lat"], bounding_box[0]["lon"], bounding_box[1]["lat"], bounding_box[1]["lon"]))
    else:
        cur.execute("""
            SELECT start_date::date, ST_AsGEOJson(points) FROM trips WHERE bounds && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
            """, (bounding_box[0]["lat"], bounding_box[0]["lon"], bounding_box[1]["lat"], bounding_box[1]["lon"]))
    
    trips = cur.fetchall()
    return [{'id': t[0], 'geoJSON': json.loads(t[1])} for t in trips]

def can_get_more_trips(cur, bounding_box, loaded_bb, canonical=False, debug = False):
    """ Checks whether there are trips in db that haven't been fetched yet in a certain bounding box

    Args:
        cur (:obj:`psycopg2.cursor`)
        debug (bool, optional): activates debug mode. 
            Defaults to False
    Returns:
        bool
    """

    if canonical:
        cur.execute("""
            WITH results as (
                (SELECT canonical_id, points FROM canonical_trips WHERE bounds && ST_MakeEnvelope(%s, %s, %s, %s, 4326)) 
                EXCEPT (
                    (SELECT canonical_id, points FROM canonical_trips WHERE bounds && ST_MakeEnvelope(%s, %s, %s, %s, 4326))
                        INTERSECT 
                    (SELECT canonical_id, points FROM canonical_trips WHERE bounds && ST_MakeEnvelope(%s, %s, %s, %s, 4326))
                )
            ) SELECT count(1) FROM results
        """, (  bounding_box[0]["lat"], bounding_box[0]["lon"], bounding_box[1]["lat"], bounding_box[1]["lon"],\
                loaded_bb[0]["lat"], loaded_bb[0]["lon"], loaded_bb[1]["lat"], loaded_bb[1]["lon"],\
                bounding_box[0]["lat"], bounding_box[0]["lon"], bounding_box[1]["lat"], bounding_box[1]["lon"]\
            ))
    else:
        cur.execute("""
            WITH results as (
                (SELECT trip_id, points FROM trips WHERE bounds && ST_MakeEnvelope(%s, %s, %s, %s, 4326)) 
                EXCEPT (
                    (SELECT trip_id, points FROM trips WHERE bounds && ST_MakeEnvelope(%s, %s, %s, %s, 4326))
                        INTERSECT 
                    (SELECT trip_id, points FROM trips WHERE bounds && ST_MakeEnvelope(%s, %s, %s, %s, 4326))
                )
            ) SELECT count(1) FROM results
        """, (  bounding_box[0]["lat"], bounding_box[0]["lon"], bounding_box[1]["lat"], bounding_box[1]["lon"],\
                loaded_bb[0]["lat"], loaded_bb[0]["lon"], loaded_bb[1]["lat"], loaded_bb[1]["lon"],\
                bounding_box[0]["lat"], bounding_box[0]["lon"], bounding_box[1]["lat"], bounding_box[1]["lon"]\
            ))

    results = cur.fetchone()
    return results[0] > 0

def get_more_trips(cur, bounding_box, loaded_bb, canonical=False, debug = False):
    """ Gets trips in db that haven't been fetched yet

    Args:
        cur (:obj:`psycopg2.cursor`)
        debug (bool, optional): activates debug mode. 
            Defaults to False
    Returns:
        :obj:`list` of :obj:`dict`
    """

    if canonical:
        cur.execute("""
            (SELECT canonical_id, ST_AsGEOJson(points) FROM canonical_trips WHERE bounds && ST_MakeEnvelope(%s, %s, %s, %s, 4326)) 
            EXCEPT (
                (SELECT canonical_id, ST_AsGEOJson(points) FROM canonical_trips WHERE bounds && ST_MakeEnvelope(%s, %s, %s, %s, 4326))
                    INTERSECT 
                (SELECT canonical_id, ST_AsGEOJson(points) FROM canonical_trips WHERE bounds && ST_MakeEnvelope(%s, %s, %s, %s, 4326))
            )
        """, (  bounding_box[0]["lat"], bounding_box[0]["lon"], bounding_box[1]["lat"], bounding_box[1]["lon"],\
                loaded_bb[0]["lat"], loaded_bb[0]["lon"], loaded_bb[1]["lat"], loaded_bb[1]["lon"],\
                bounding_box[0]["lat"], bounding_box[0]["lon"], bounding_box[1]["lat"], bounding_box[1]["lon"]\
            ))
    else:
        cur.execute("""
            (SELECT start_date::date, ST_AsGEOJson(points) FROM trips WHERE bounds && ST_MakeEnvelope(%s, %s, %s, %s, 4326)) 
            EXCEPT (
                (SELECT start_date::date, ST_AsGEOJson(points) FROM trips WHERE bounds && ST_MakeEnvelope(%s, %s, %s, %s, 4326))
                    INTERSECT 
                (SELECT start_date::date, ST_AsGEOJson(points) FROM trips WHERE bounds && ST_MakeEnvelope(%s, %s, %s, %s, 4326))
            )
        """, (  bounding_box[0]["lat"], bounding_box[0]["lon"], bounding_box[1]["lat"], bounding_box[1]["lon"],\
                loaded_bb[0]["lat"], loaded_bb[0]["lon"], loaded_bb[1]["lat"], loaded_bb[1]["lon"],\
                bounding_box[0]["lat"], bounding_box[0]["lon"], bounding_box[1]["lat"], bounding_box[1]["lon"]\
            ))

    trips = cur.fetchall()
    return [{'id': t[0], 'geoJSON': json.loads(t[1])} for t in trips]


def get_all_trips(cur, debug = False):
    """ Gets trips in db

    Args:
        cur (:obj:`psycopg2.cursor`)
        debug (bool, optional): activates debug mode. 
            Defaults to False
    Returns:
        :obj:`list` of :obj:`dict`
    """
    cur.execute("SELECT trip_id, ST_AsGEOJson(points) FROM trips")
    trips = cur.fetchall()
    return [{'id': t[0], 'points': json.loads(t[1])} for t in trips]

def remove_trips_from_day(cur, date, debug= False):
    ''' Removes trips and stays associated to a certain day

    Args:
        cur (:obj:`psycopg2.cursor`)
        date (str)
        debug (bool, optional): activates debug mode. 
            Defaults to False
    '''
    cur.execute("""
        DELETE FROM trips WHERE start_date::date = %s;
        DELETE FROM stays WHERE start_date::date = %s;
    """, (date, date)) 

def remove_canonical_trips_from_day(cur, date, debug=False):
    ''' Removes canonical trips that are only associated to one track of a certain day
    
    Args:
        cur (:obj:`psycopg2.cursor`)
        date (str)
        debug (bool, optional): activates debug mode. 
            Defaults to False
    '''
    
    cur.execute(f"""
        DELETE FROM canonical_trips c WHERE c.canonical_id IN (
            SELECT canonical_id FROM (
                SELECT canonical_trip as canonical_id, trip as trip_id FROM
                    canonical_trips_relations AS a 
                INNER JOIN
                    (
                        SELECT canonical_trip FROM canonical_trips_relations
                        GROUP BY canonical_trip HAVING COUNT(canonical_trip) = 1
                    ) AS b
                USING (canonical_trip)
            ) relations INNER JOIN (
                SELECT trip_id FROM trips WHERE start_date::date = '{date}'
            ) trips USING (trip_id)
        )
    """)


def execute_query(cur, query, debug = False):
    ''' Executes query in database

    Args:
        cur (:obj:`psycopg2.cursor`)
        query (str)
        debug (bool, optional): activates debug mode. 
            Defaults to False
    '''
    cur.execute(query)