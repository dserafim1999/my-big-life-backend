from urllib.parse import urlencode
from xmlrpc.client import Boolean
import requests
import random
import polyline
import argparse
from datetime import datetime, timedelta

from math import radians, cos, sin, asin, sqrt

import json
from os.path import expanduser, isfile
from utils import update_dict

from life.life import Life

from main.default_config import CONFIG

#TODO finish documentation
#TODO explain api setup

parser = argparse.ArgumentParser(description='')
parser.add_argument('--config', '-c', dest='config', metavar='c', type=str,
        help='configuration file')
parser.add_argument('--life', '-l', dest='life', metavar='l', type=str,
        help='life file', required=True)
parser.add_argument('--google', '-g', dest='use_google_maps_api', metavar='g', type=Boolean,
        help='use google maps api (default False)')
args = parser.parse_args()

def indentation(n):
        return ''.join('\t' for i in range(n))

FAIL_COLOR = '\033[91m'
END_COLOR = '\033[0m'

class LIFEToTrackConverter(object):
    """ 

        
    """

    def __init__(self, life_file, config_file, use_google_maps_api=False):
        self.config = dict(CONFIG)

        if config_file and isfile(expanduser(config_file)):
            with open(expanduser(config_file), 'r') as config_file:
                config = json.loads(config_file.read())
                update_dict(self.config, config)

        self.life = Life()
        self.life.from_file(life_file)
        self.days = self.life.days
        self.routes = {}

        self.google_maps_api = use_google_maps_api
        self.set_api()

        self.get_bounds()
        self.get_places_in_LIFE()

        self.LIFE_to_gpx()
    
    def set_api(self):
        """ Selects what API to use based on if explicitly set and/or based on what keys were set in the configuration file
        """

        # if no API key is set, quit program
        if len(self.config['life_converter']['google_maps_api_key']) == 0 and len(self.config['life_converter']['tom_tom_api_key']) == 0:
            print(f"{FAIL_COLOR}No API set to generate routes.\nPlease set a Google Maps or TomTom API key in your configuration JSON file.{END_COLOR}")
            quit()

        # checks if Google API key is set, uses Tom Tom API key if not
        if self.google_maps_api:
            if len(self.config['life_converter']['google_maps_api_key']) > 0:
                self.api_key = self.config['life_converter']['google_maps_api_key']
                print("Using Google Maps Directions API to generate routes...")
            elif len(self.config['life_converter']['tom_tom_api_key']) > 0:
                self.api_key = self.config['life_converter']['tom_tom_api_key']
                self.google_maps_api = False
                print("Using Tom Tom Routing API to generate routes...")

        # checks if Tom Tom API key is set, uses Google API key if not
        else:
            if len(self.config['life_converter']['tom_tom_api_key']) > 0:
                self.api_key = self.config['life_converter']['tom_tom_api_key']
                self.google_maps_api = False
                print("Using Tom Tom Routing API to generate routes...")
            elif len(self.config['life_converter']['google_maps_api_key']) > 0:
                self.api_key = self.config['life_converter']['google_maps_api_key']
                print("Using Google Maps Directions API to generate routes...")

    def get_bounds(self):
        """ Stores bounds for random coordinates generation defined in the config file

        Returns:
            :obj:`list` of :obj:`dict` : Contains the two points that define an upper and lower corner of the bounds 
        """
        
        point1 = {'lat': self.config['life_converter']['bounds']['point1']['lat'], 'lng': self.config['life_converter']['bounds']['point1']['lng']}
        point2 = {'lat': self.config['life_converter']['bounds']['point2']['lat'], 'lng': self.config['life_converter']['bounds']['point2']['lng']}

        self.bounds = [point1, point2]

    def get_places_in_LIFE(self):
        """ Get all places in LIFE file and associates random coordinates in the designated bounds
        """ 

        places = {}
        place_names = self.life.all_places()

        for place in place_names:
            if (place in places): #if place was already added for being a subplace/name swap or location swap
                continue
            
            if (place in self.life.coordinates):
                coords = self.life.coordinates[place]
                places[place] = (coords[0], coords[1]) # if coordinates are explicitly defined in LIFE file, set them
            else:
                places[place] =  self.random_point_in_bounds() # assign random coordinates in predefined bounds

            if (place in self.life.superplaces.keys()):
                places[place] = places[self.life.superplaces[place]] # if subplace, set the coordinates of its superplace
            
            if (place in self.life.nameswaps):
                places[self.life.nameswaps[place][0]] = places[place] # if place changed name, copies coords from original to new
            
            if (place in self.life.locationswaps):
                places[self.life.locationswaps[place][0]] = places[place] # if something new in same location, copies coords from original to new

        self.places = places

    def distance(self, coords1, coords2): 
        """ Calculates distance, in km, of two coordinates defined by latitude and longitude
        taken from https://www.geeksforgeeks.org/program-distance-two-points-earth/#:~:text=For%20this%20divide%20the%20values,is%20the%20radius%20of%20Earth.

        Args:
            coords1 (:obj:`dict`): Coordinates of the first point
            coords2 (:obj:`dict`): Coordinates of the second point
        Returns:
            float: distance between both points
        """
        lon1 = radians(coords1['lng'])
        lon2 = radians(coords2['lng'])
        lat1 = radians(coords1['lat'])
        lat2 = radians(coords2['lat'])
        
        # Haversine formula
        dlon = lon2 - lon1 
        dlat = lat2 - lat1
        a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    
        c = 2 * asin(sqrt(a))
        
        r = 6371 # Radius of earth in kilometers
        
        return(c * r)

    def random_point_in_bounds(self):
        """ Generates a random latitude/longitude pair inside predefined bounds
        Returns:
            :obj:`tuple`: Coordinates
        """

        lat = random.uniform(min(self.bounds[0]['lat'], self.bounds[1]['lat']), max(self.bounds[0]['lat'], self.bounds[1]['lat']))
        lng = random.uniform(min(self.bounds[0]['lng'], self.bounds[1]['lng']), max(self.bounds[0]['lng'], self.bounds[1]['lng']))

        return (lat, lng)

    def calculate_speed(self, distance, time):
        """ Calculates the speed (in metres per second)
        Args: 
            distance (float): Distance (in metres)
            time (int): Duration of route (in seconds)
        Returns:
            float: speed (in m/s)
        """
        if time == 0: 
            return 0 
        else: 
            return distance / time

    def parse_duration_in_seconds(self, response):
        """ Parses duration from http request response into seconds
        Args:
            response (:obj:`dict`:): http response for the routing request
        Returns:
            int: duration in seconds
        """
        result = 3600 * hours + 60 * mins

        if self.google_maps_api:
            string = response['routes'][0]['legs'][0]['distance']['text']
            words = string.split(" ")
            hours = mins = 0

            if "hour" in words[1]:
                hours = int(words[0])
                if len(words) == 4:
                    mins = int(words[2])
            elif "min" in words[1]:
                mins = int(words[0])
        else:
            result = response['routes'][0]['summary']['travelTimeInSeconds']
        
        return result

    def parse_distance_in_metres(self, response):
        """ Parses distance from http request response into metres
        Args:
            response (:obj:`dict`:): http response for the routing request
        Returns:
            float: distance in metres 
        """
        result = 0

        if self.google_maps_api:
            string = response['routes'][0]['legs'][0]['distance']['text']

            words = string.split(" ")
            kms = 0

            if "km" in words[1]:
                kms = float(words[0])
                
            result = kms * 1000
        else:
            result = response['routes'][0]['summary']['lengthInMeters']

        return result
    
    def parse_coords(self, coords):
        """ Parses coordinates into a formatted string for an http request 
        Args:
            coords (:obj:`tuple`:): coordinate pair containing latitude and longitude
        Returns:
            string: formatted string to be set as an http request parameter 
        """
        return f"{coords[0]},{coords[1]}"

    def parse_points(self, response):
        """ Parses points into a list of coordinate pairs
        Args:
            response (:obj:`tuple`:): http response for the routing request 
        Returns:
            float: distance in kilometres 
        """
        if self.google_maps_api:
            polyline_points = response['routes'][0]['overview_polyline']['points'] 
            return polyline.decode(polyline_points)
        else:
            return [(point['latitude'], point['longitude']) for point in response['routes'][0]['legs'][0]['points']]

    def get_route(self, start, end, start_time, end_time, data_type = 'json'):
        """ Calculates route for a span, from "start" to "end", that starts at "start_time" and ends at "end_time"
        Args:
            start (string): coordinates (or location name) of the route's origin
            end (string): coordinates (or location name) of the route's destination
            start_time (string): formatted string representing the route's start time in the `%Y-%m-%dT%H:%M:%SZ` format
            end_time (string): formatted string representing the route's end time in the `%Y-%m-%dT%H:%M:%SZ` format
        Returns:
            :obj:`list` of :obj:`dict`: list containing the latitude, longitude and timestamps of the points that describe the route
        """

        start_datetime = datetime.strptime(start_time,'%Y-%m-%dT%H:%M:%SZ')
        end_datetime = datetime.strptime(end_time,'%Y-%m-%dT%H:%M:%SZ')
        total_time = (end_datetime - start_datetime).total_seconds()

        timestamp = start_datetime

        # check if the route has been calculated previously, and updates timestamps
        if (start in self.routes and end in self.routes[start]): 
            total_distance = self.routes[start]['total_distance']
            points = self.routes[start][end]
            n_points = len(points)

            avg_time_btwn_points = self.calculate_avg_time_btwn_points(total_distance, total_time, n_points)
            
            for point in points:
                point['time'] = timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
                timestamp += timedelta(seconds=avg_time_btwn_points)

            return points
        
        # http request setup
        if self.google_maps_api:
            endpoint = f"https://maps.googleapis.com/maps/api/directions/{data_type}"
            params = {"origin": start, "destination": end, "key":  self.config['life_converter']['google_maps_api_key']}
        else:
            endpoint = f"https://api.tomtom.com/routing/1/calculateRoute/{start}:{end}/{data_type}"
            params = {"routeRepresentation": "polyline", "key":  self.config['life_converter']['tom_tom_api_key']} 

        url_params = urlencode(params)
        url = f"{endpoint}?{url_params}"

        # requests directions between 2 locations from the either the Tom Tom Routing or Google Maps Directions API 
        r = requests.get(url)

        result = {}
        if r.status_code not in range(200, 299):
            return {}

        try:
            result = r.json()
            if 'OK' not in result['status'] or 'detailedError' in result: # checks if request was successful 
                return {} 
        except:
            pass

        raw_points = self.parse_points(result) # points that describe the route
        total_distance = self.parse_distance_in_metres(result)

        n_points = len(raw_points)
        
        avg_time_btwn_points = self.calculate_avg_time_btwn_points(total_distance, total_time, n_points) # calculates step between points (just an average)
        
        points = []

        for point in raw_points:
            points.append({'lat': point[0], 'lng': point[1], 'time': timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')})
            timestamp += timedelta(seconds=avg_time_btwn_points)

        self.routes[start] = {end: points, 'total_distance': total_distance} # saves calculated route for future reference 

        return points

    def calculate_avg_time_btwn_points(self, total_distance, total_time, n_points):
        """ Calculates the average time between each point in a route, taking into consideration the route's total duration and distance
        Args:
            total_distance (float): route's total distance in metres
            total_time (int): route's total duration in seconds
            n_points (int): number of points that define the route
        Returns:
            float: average time between the route's points, in seconds
        """
        avg_speed = self.calculate_speed(total_distance, total_time)
        if avg_speed > 0:
            return (total_distance) / (avg_speed * n_points)
        else: 
            return 0

    def get_segments(self, day):
        """ Calculates routes for all spans in a LIFE day. Connects two consecutive spans with a route that starts at the span location's 
        final timestamp and ends at the second  span location's start timestamp.
        Args:
            day (:obj:`life.Day`): life.Day object that contains information about the date and the spans of a day 
        Returns:
            :obj:`list` of :obj:`list` of :obj:`dict`: list that contains a list of the points that define the selected day's routes 
        """ 

        res = []
        
        for i in range(1, len(day.spans)):
            prev_span = day.spans[i - 1]
            span = day.spans[i]

            if (prev_span.multiplace() or span.place == prev_span.place):
                continue
            
            # if a route is specified in a span, we calculate it using those locations
            if (span.multiplace()):
                start_coords = self.parse_coords(self.places[span.place[0]])
                end_coords = self.parse_coords(self.places[span.place[1]])

                start_time = span.start_utc()
                end_time = span.end_utc()

            # if not, we use the previous span to get the start time and location
            else: 
                start_coords = self.parse_coords(self.places[prev_span.place])
                end_coords = self.parse_coords(self.places[span.place])
                
                start_time = prev_span.end_utc()
                end_time = span.start_utc()
            
            if (start_time == end_time):
                continue

            route = self.get_route(start_coords, end_coords, start_time, end_time)
            
            res.append(route)
        
        return res

    def point_gpx(self, point):
        """ Parses a point into an xml tag for the gpx file that defines the track 
        Args:
            point (:obj:`dict`): route point defined by latitude (lat), longitude (lng) and timestamp (time)
        Returns:
            string: xml tag <trkpt> that defines a point in the gpx format
        """
        return ''.join([
            indentation(3),
            '<trkpt lat="' + str(point['lat']) + '" lon="' + str(point['lng']) + '">\n',
            indentation(4),
            '<time>' + str(point['time']) + '</time>\n',
            indentation(3),
            '</trkpt>'
        ]) + '\n'

    def segment_gpx(self, segment):
        """ Parses a segment of a route into an xml tag for the gpx file that defines the track 
        Args:
            segment (:obj:`list` of :obj:`dict`): list of points that define a segment of the route
        Returns:
            string: xml tag <trkseg> that defines a segment in the gpx format
        """

        if (len(segment) == 0):
            return ''

        points = ''.join([self.point_gpx(point) for point in segment])
        
        return ''.join([
            indentation(2) + '<trkseg>\n',
            points,
            indentation(2) + '</trkseg>\n',
        ]) + '\n'


    def to_gpx(self, day):
        """ Parses a route into an xml representation for the gpx file that defines the track 
        Args:
            day (:obj:`life.Day`): life.Day object that contains information about the date and the spans of a day 
        Returns:
            string: xml that defines the route in the gpx format
        """

        all_segments = self.get_segments(day)

        if (len(all_segments) == 0):
            return ''

        segments = ''.join([self.segment_gpx(segment) for segment in all_segments])
        
        return ''.join([
            '<?xml version="1.0" encoding="UTF-8"?>\n',
            f'<!-- {day.date} -->\n'
            '<gpx xmlns="http://www.topografix.com/GPX/1/1">\n',
            indentation(1) + '<trk>\n', 
            segments,  
            indentation(1) + '</trk>\n',
            '</gpx>\n'
        ])
    
    def LIFE_to_gpx(self):
        """ Converts a file in the LIFE format into a file in the .gpx format describing a possible set of routes taken for each day
        """
        for day in self.days:
            # Checks if day contains more than one location (in other words, contains at least one route)
            if len(day.all_places()) > 1:
                self.generate_gpx_file(day)

    def generate_gpx_file(self, day):
        """ Creates a file in the gpx format that defines a day
        Args:
            day (:obj:`life.Day`): life.Day object that contains information about the date and the spans of a day 
        """

        with open(f"tracks\\input\\{day.date}.gpx", "w+") as f:
                f.write(self.to_gpx(day))
                f.close()
            
    
if __name__=="__main__":
    use_google_maps_api = args.use_google_maps_api
    life_file = args.life
    config_file = args.config

    if use_google_maps_api == None:
        use_google_maps_api = False

    LIFEToTrackConverter(life_file, config_file, use_google_maps_api)
 