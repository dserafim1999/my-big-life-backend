from time import time
from urllib.parse import urlencode
import requests
import random
import polyline
from datetime import datetime, timedelta

from math import radians, cos, sin, asin, sqrt

import json
from os.path import expanduser, isfile
from utils import update_dict

from life.life import Life

from main.default_config import CONFIG

#TODO refactor and make parameterizable
#TODO documentation

def indentation(n):
        return ''.join('\t' for i in range(n))

class LIFEToTrackConverter(object):
    """ 
        
    """

    def __init__(self, life_file, config_file):
        self.config = dict(CONFIG)

        if config_file and isfile(expanduser(config_file)):
            with open(expanduser(config_file), 'r') as config_file:
                config = json.loads(config_file.read())
                update_dict(self.config, config)

        self.life = Life()
        self.life.from_file(life_file)
        self.routes = {}

        self.days = self.life.days
        self.get_bounds()
        self.get_places_in_LIFE()

    def get_bounds(self):
        point1 = {'lat': self.config['life_converter']['bounds']['point1']['lat'], 'lng': self.config['life_converter']['bounds']['point1']['lng']}
        point2 = {'lat': self.config['life_converter']['bounds']['point2']['lat'], 'lng': self.config['life_converter']['bounds']['point2']['lng']}

        self.bounds = [point1, point2]

    def get_places_in_LIFE(self):
        '''
        get all places in file and associate random coords
        '''

        places = {}
        place_names = self.life.all_places()

        for place in place_names:
            if (place in places):
                continue
            
            if (place in self.life.coordinates):
                coords = self.life.coordinates[place]
                places[place] = f"{coords[0]},{coords[1]}"
            else:
                places[place] =  self.random_point_in_bounds()

            if (place in self.life.superplaces.keys()):
                places[place] = places[self.life.superplaces[place]]
            
            if (place in self.life.nameswaps):
                places[self.life.nameswaps[place][0]] = places[place] # if place changed name, copies coords from original to new
            
            if (place in self.life.locationswaps):
                places[self.life.locationswaps[place][0]] = places[place] # if something new in same location, copies coords from original to new

        self.places = places

    def distance(self, coords1, coords2): 
        """
        taken from https://www.geeksforgeeks.org/program-distance-two-points-earth/#:~:text=For%20this%20divide%20the%20values,is%20the%20radius%20of%20Earth.
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
        
        # Radius of earth in kilometers. Use 3956 for miles
        r = 6371
        
        # calculate the result
        return(c * r)

    def random_point_in_bounds(self):
        lat = random.uniform(min(self.bounds[0]['lat'], self.bounds[1]['lat']), max(self.bounds[0]['lat'], self.bounds[1]['lat']))
        lng = random.uniform(min(self.bounds[0]['lng'], self.bounds[1]['lng']), max(self.bounds[0]['lng'], self.bounds[1]['lng']))

        return f"{lat},{lng}"
        #return self.get_closest_place_coords(lat, lng)

    def calculate_km_min(self, distance, time):
        """
        km/min
        """
        if time == 0: 
            return 0 
        else: 
            return distance / time

    def parse_duration_in_minutes(self, string):
        words = string.split(" ")
        hours = mins = 0

        if "hour" in words[1]:
            hours = int(words[0])
            if len(words) == 4:
                mins = int(words[2])
        elif "min" in words[1]:
            mins = int(words[0])

        return 60 * hours + mins 

    def parse_distance_in_kms(self, string):
        words = string.split(" ")
        kms = 0

        if "km" in words[1]:
            kms = float(words[0])
            
        return kms 
        
    def get_route(self, start, end, start_time, end_time, data_type = 'json'):
        start_datetime = datetime.strptime(start_time,'%Y-%m-%dT%H:%M:%SZ')
        end_datetime = datetime.strptime(end_time,'%Y-%m-%dT%H:%M:%SZ')
        total_time = (end_datetime - start_datetime).total_seconds() / 60

        timestamp = start_datetime

        if (start in self.routes and end in self.routes[start]):
            total_distance = self.routes[start]['total_distance']
            points = self.routes[start][end]
            n_points = len(points)

            avg_time_btwn_points = self.calculate_avg_time_btwn_points(total_distance, total_time, n_points)
            
            for point in points:
                point['time'] = timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
                timestamp += timedelta(minutes=avg_time_btwn_points)

            return points
        
        endpoint = f"https://maps.googleapis.com/maps/api/directions/{data_type}"
        params= {"destination": start, "origin": end, "key":  self.config['life_converter']['google_maps_api_key']}
        url_params = urlencode(params)
        url = f"{endpoint}?{url_params}"

        r = requests.get(url)

        result = {}
        if r.status_code not in range(200, 299):
            return {}

        try:
            result = r.json()
            if 'OK' not in result['status']:
                return {} 
        except:
            pass

        polyline_points = result['routes'][0]['overview_polyline']['points']
        total_distance = self.parse_distance_in_kms(result['routes'][0]['legs'][0]['distance']['text'])
        points = []

        polylines = polyline.decode(polyline_points)
        polylines.reverse()
        n_points = len(polylines)
        
        avg_time_btwn_points = self.calculate_avg_time_btwn_points(total_distance, total_time, n_points)

        for point in polylines:
            points.append({'lat': point[0], 'lng': point[1], 'time': timestamp.strftime('%Y-%m-%dT%H:%M:%SZ'), 'total_distance': total_distance})
            timestamp += timedelta(minutes=avg_time_btwn_points)

        self.routes[start] = {end: points, 'total_distance': total_distance}

        return points

    def calculate_avg_time_btwn_points(self, total_distance, total_time, n_points):
        avg_speed = self.calculate_km_min(total_distance, total_time)
        if avg_speed > 0:
            return (total_distance) / (avg_speed * n_points)
        else: 
            return 0

    def get_closest_place_coords(self, lat, lng, data_type = 'json'):
        endpoint = f"https://maps.googleapis.com/maps/api/geocode/{data_type}"
        params= {"latlng": str(lat)+','+str(lng), "key": self.config['life_converter']['google_maps_api_key']}
        url_params = urlencode(params)
        url = f"{endpoint}?{url_params}"
        r = requests.get(url)
        result = {}

        #TODO parameters check

        if r.status_code not in range(200, 299):
            return {}
        try:
            result = r.json() 
        except:
            pass
        coords = result['results'][0]['geometry']['location']
        return f"{coords['lat']},{coords['lng']}"

    def get_segments(self, day): 
        #a followed by b, get route between a and b starting with a start time and ending with b end time
        #a->b ignore a before, get route between a and b with start and end time of span
        #point has lat, lng and time keys

        res = []
        
        for i in range(1, len(day.spans)):
            prev_span = day.spans[i - 1]
            span = day.spans[i]

            if (prev_span.multiplace() or span.place == prev_span.place):
                continue

            if (span.multiplace()):
                start_coords = self.places[span.place[0]]
                end_coords = self.places[span.place[1]]

                start_time = span.start_utc()
                end_time = span.end_utc()
            else: 
                start_coords = self.places[prev_span.place]
                end_coords = self.places[span.place]
                
                start_time = prev_span.end_utc()
                end_time = span.start_utc()
            
            if (start_time == end_time):
                continue

            route = self.get_route(start_coords, end_coords, start_time, end_time)
            
            res.append(route)
        
        return res

    def segment_gpx(self, segment):
        if (len(segment) == 0):
            return ''

        points = ''.join([self.point_gpx(point) for point in segment])
        
        return ''.join([
            indentation(2) + '<trkseg>\n',
            points,
            indentation(2) + '</trkseg>\n',
        ]) + '\n'

    def point_gpx(self, point):
        return ''.join([
            indentation(3),
            '<trkpt lat="' + str(point['lat']) + '" lon="' + str(point['lng']) + '">\n',
            indentation(4),
            '<time>' + str(point['time']) + '</time>\n',
            indentation(3),
            '</trkpt>'
        ]) + '\n'

    def to_gpx(self, day):
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
        for day in self.days:
            self.generate_gpx_file(day)
        return None

    def generate_gpx_file(self, day):
        with open(f"tracks\\input\\{day.date}.gpx", "w+") as f:
                f.write(self.to_gpx(day))
                f.close()
            
    
if __name__=="__main__":
    t = LIFEToTrackConverter('life/a.life', 'config.json')
    t.LIFE_to_gpx()