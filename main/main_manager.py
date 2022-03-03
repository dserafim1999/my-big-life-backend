# -*- coding: utf-8 -*-
"""
Contains class that orchestrates processing
"""
import re
import json
from os import listdir, stat
from os.path import join, expanduser, isfile
import tracktotrip as tt
from tracktotrip.utils import pairwise
from tracktotrip.classifier import Classifier
from main import db
from life.life import Life

from trackprocessing.default_config import CONFIG

def inside(to_find, modes):
    for elm in to_find:
        if elm.lower() in modes:
            return elm.lower()
    return None

def gte_time(small, big):
    if small.hour < big.hour:
        return True
    elif small.hour == big.hour and small.minute <= big.minute:
        return True
    else:
        return False

def is_time_between(lower, time, upper):
    return gte_time(lower, time) and gte_time(time, upper)

def find_index_point(track, time):
    for j, segment in enumerate(track.segments):
        i = 0
        for p_a, p_b in pairwise(segment.points):
            if is_time_between(p_a.time, time, p_b.time):
                return (j, i)
            i = i + 1
    return None, None

def apply_transportation_mode_to(track, life_content, transportation_modes):
    life = Life()
    life.from_string(life_content.split('\n'))

    for segment in track.segments:
        segment.transportation_modes = []

    for day in life.days:
        for span in day.spans:
            has = inside(span.tags, transportation_modes)
            if has:
                start_time = db.span_date_to_datetime(span.day, span.start)
                end_time = db.span_date_to_datetime(span.day, span.end)

                start_segment, start_index = find_index_point(track, start_time)
                end_segment, end_index = find_index_point(track, end_time)
                if start_segment is not None:
                    if end_index is None or end_segment != start_segment:
                        end_index = len(track.segments[start_segment].points) - 1

                    track.segments[start_segment].transportation_modes.append({
                        'label': has,
                        'from': start_index,
                        'to': end_index
                        })


def save_to_file(path, content, mode="w"):
    """ Saves content to file

    Args:
        path (str): filepath, including filename
        content (str): content to write to file
        mode (str, optional): mode to write, defaults to w
    """
    with open(path, mode) as dest_file:
        dest_file.write(content)

TIME_RX = re.compile(r'\<time\>([^\<]+)\<\/time\>')
def predict_start_date(filename):
    """ Predicts the start date of a GPX file

    Reads the first valid date, by matching TIME_RX regular expression

    Args:
        filename (str): file path
    Returns:
        :obj:`datetime.datetime`
    """
    with open(filename, 'r') as opened_file:
        result = TIME_RX.search(opened_file.read())
        return tt.utils.isostr_to_datetime(result.group(1))

def file_details(base_path, filepath):
    """ Returns file details

    Example:
        >>> file_details('/users/username/tracks/', '25072016.gpx')
        {
            'name': '25072016.gpx',
            'path': '/users/username/tracks/25072016.gpx',
            'size': 39083,
            'start': <datetime.datetime>,
            'date': '2016-07-25t07:40:52z'
        }

    Args:
        base_path (str): Base path
        filename (str): Filename
    Returns:
        :obj:`dict`: See example
    """
    complete_path = join(base_path, filepath)
    (_, _, _, _, _, _, size, _, _, _) = stat(complete_path)

    date = predict_start_date(complete_path)
    return {
        'name': filepath,
        'path': complete_path,
        'size': size,
        'start': date,
        'date': date.date().isoformat()
    }

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

class MainManager(object):
    """ 

    Arguments:
        
    """

    def __init__(self, config_file):
        self.config = dict(CONFIG)

        if config_file and isfile(expanduser(config_file)):
            with open(expanduser(config_file), 'r') as config_file:
                config = json.loads(config_file.read())
                update_dict(self.config, config)

    def list_gpxs(self):
        """ Lists gpx files from the input path, and some details

        Result is sorted by start date
        See `file_details`

        Returns:
            :obj:`list` of :obj:`dict`
        """
        if not self.config['input_path']:
            return []

        input_path = expanduser(self.config['input_path'])
        files = listdir(input_path)
        files = [f for f in files if f.split('.')[-1] == 'gpx']

        files = [file_details(input_path, f) for f in files]
        files = sorted(files, key=lambda f: f['date'])
        return files

    def list_lifes(self):
        """ Lists life files from the input path, and some details

        Returns:
            :obj:`list` of :obj:`dict`
        """
        if not self.config['input_path']:
            return []

        input_path = expanduser(self.config['input_path'])
        files = listdir(input_path)
        files = [f for f in files if f.split('.')[-1] == 'life']
        return files

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

