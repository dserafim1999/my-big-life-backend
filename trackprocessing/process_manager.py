# -*- coding: utf-8 -*-
"""
Contains class that orchestrates processing
"""
import re
import json
import glob 
import tracktotrip3 as tt

from os import listdir, stat, rename, replace, remove
from shutil import copyfile
from datetime import datetime
from os.path import join, expanduser, isfile
from collections import OrderedDict
from tracktotrip3.utils import estimate_meters_to_deg
from tracktotrip3.location import infer_location
from tracktotrip3.learn_trip import learn_trip, complete_trip
from main import db
from life.life import Life
from utils import Manager

def gte_time(small, big, debug = False):
    """ Determines if time is greater or equal to another 
    
    Args:
        small (:obj:`datetime.datetime`)
        big (:obj:`datetime.datetime`)
        debug (bool, optional): activates debug mode. 
            Defaults to False
    Returns:
        bool
    """

    if small.hour < big.hour:
        return True
    elif small.hour == big.hour and small.minute <= big.minute:
        return True
    else:
        return False

def is_time_between(lower, time, upper, debug):
    """ Determines if time is between two other times 
    
    Args:
        lower (:obj:`datetime.datetime`)
        time (:obj:`datetime.datetime`): time to be compared
        upper (:obj:`datetime.datetime`)
        debug (bool, optional): activates debug mode. 
            Defaults to False
    Returns:
        bool
    """
    return gte_time(lower, time, debug) and gte_time(time, upper, debug)

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
def predict_start_date(filename, debug = False):
    """ Predicts the start date of a GPX file

    Reads the first valid date, by matching TIME_RX regular expression

    Args:
        filename (str): file path
    Returns:
        :obj:`datetime.datetime`
    """
    with open(filename, 'r') as opened_file:
        result = TIME_RX.findall(opened_file.read())
        if len(result) > 1:
            date = result[1]
        else:
            date = result[0]

        return tt.utils.isostr_to_datetime(date, debug)

def file_details(base_path, filepath, debug = False):
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

    date = predict_start_date(complete_path, debug)

    return {
        'name': filepath,
        'path': complete_path,
        'size': size,
        'start': date,
        'date': date.date().isoformat()
    }

class Step(object):
    """ Step enumeration
    """
    preview = 0
    adjust = 1
    annotate = 2
    done = -1
    _len = 3

    @staticmethod
    def next(current):
        """ Advances from one step to the next

        Args:
            current (int): Current step
        Returns:
            int: next step
        """
        return (current + 1) % Step._len

    @staticmethod
    def prev(current):
        """ Backs one step

        Args:
            current (int): Current step
        Returns:
            int: previous step
        """
        return (current - 1) % Step._len

class ProcessingManager(Manager):
    """ Manages the processing phases

    Arguments:
        queue: Array of strings, with the files to be
            processed. Doesn't include the current file
        currentFile: String with the current file being
            processed
        history: Array of TrackToTrip.Track. Must always
            have length greater or equal to ONE. The
            last element is the current state of the system
        INPUT_PATH: String with the path to the input folder
        BACKUP_PATH: String with the path to the backup folder
        OUTPUT_PATH: String with the path to the output folder
        LIFE_PATH: String with the path to the LIFE output
            folder
    """

    def __init__(self, config_file, metrics, debug):
        super().__init__(config_file, debug)

        self.is_bulk_processing = False
        self.bulk_progress = -1
        self.queue = {}
        self.life_queue = []
        self.current_step = None
        self.history = []
        self.current_day = None
        self.debug = debug
        self.reset()

        self.use_metrics = metrics
        self.metrics = []

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

    def reset(self):
        """ Resets all variables and computes the first step

        Returns:
            :obj:`ProcessingManager`: self
        """

        queue = self.list_gpxs()
        
        if len(queue) > 0:
            self.current_step = Step.preview
            self.load_days()
        else:
            self.queue = {}
            self.current_day = None
            self.current_step = Step.done
            self.history = []

        return self

    def change_day(self, day):
        """ Changes current day, and computes first step

        Args:
            day (:obj:`datetime.date`): Only loads if it's an existing key in queue
        """

        if day in list(self.queue.keys()):
            key_to_use = day
            gpxs_to_use = self.queue[key_to_use]
            gpxs_to_use = [tt.Track.from_gpx(gpx['path'], self.debug)[0] for gpx in gpxs_to_use]


            self.current_day = key_to_use

            segs = []
            for gpx in gpxs_to_use:
                segs.extend(gpx.segments)

            track = tt.Track('', segments=segs, debug=self.debug)
            track.name = track.generate_name(self.config['trip_name_format'])

            self.history = [track]
            self.current_step = Step.preview
        else:
            raise TypeError('Cannot find any track for day: %s' % day)

    def reload_queue(self):
        """ Reloads the current queue, filling it with the current file's details existing
            in the input folder
        """
        queue = {}
        current_day_in_queue = False

        gpxs = self.list_gpxs()
        lifes = self.list_lifes()
        for gpx in gpxs:
            day = gpx['date']
            current_day_in_queue = current_day_in_queue or day == self.current_day
            if day in queue:
                queue[day].append(gpx)
            else:
                queue[day] = [gpx]

        self.queue = OrderedDict(sorted(queue.items()))
        self.life_queue = lifes

        if len(list(self.queue.items())) == 0:
            self.current_day = None
            self.current_step = Step.done
        elif not current_day_in_queue:
            self.current_day = list(self.queue.items())[0][0]
            self.change_day(self.current_day)

    def next_day(self, delete=True):
        """ Advances a day (to next existing one)

        Args:
            delete (bool, optional): True to delete day from queue, NOT from input folder.
                Defaults to true
        """
        
        if delete:
            del self.queue[self.current_day]
        existing_days = list(self.queue.keys())
        if self.current_day in existing_days:
            index = existing_days.index(self.current_day)
            next_day = index if len(existing_days) > index + 1 else 0
            existing_days.remove(self.current_day)
        else:
            next_day = 0
            

        if len(existing_days) > 0:
            self.change_day(existing_days[next_day])
        else:
            self.reset()

    def load_days(self):
        """ Reloads queue and sets the current day as the oldest one
        """
        self.reload_queue()

    def restore(self):
        """ Backs down a pass
        """
        if self.current_step != Step.done and self.current_step != Step.preview:
            self.current_step = Step.prev(self.current_step)
            self.history.pop()

    def process(self, data, calculate_canonical=True):
        """ Processes the current step

        Args:
            data (:obj:`dict`): JSON payload received from the client
        Returns:
            :obj:`tracktotrip3.Track`
        """
        step = self.current_step

        if 'changes' in list(data.keys()):
            changes = data['changes']
        else:
            changes = []

        if 'LIFE' in list(data.keys()):
            life = data['LIFE']
        else:
            life = ''

        if len(changes) > 0:
            track = tt.Track.from_json(data['track'], self.debug)
            self.history[-1] = track
        track = self.current_track().copy()

        if step == Step.preview:
            result = self.preview_to_adjust(track)#, changes)
        elif step == Step.adjust:
            result = self.adjust_to_annotate(track)
        elif step == Step.annotate:
            if not life or len(life) == 0:
                life = track.to_life(self.config["trip_annotations"])
            return self.annotate_to_next(track, life, calculate_canonical)
        else:
            return None

        if result:
            self.current_step = Step.next(self.current_step)
            self.history.append(result)

        return result
    
    def edit_latest_metrics(self, key, value):
        """ Adds a new key/value pair to the latest metrics object in the metrics array
        
        Args:
            key (:str) Key to add
            value (:obj) Value to add
        """

        if self.use_metrics:
            self.metrics[-1][key] = value

    def get_bulk_progress(self):
        """ Returns bulk processing progress status
        """

        return {"progress": self.bulk_progress}

    def bulk_process(self, raw):
        """ Starts bulk processing all GPXs queued
        """

        self.reload_queue()

        processed = 1
        total_num_days = len(list(self.queue.values()))
        self.is_bulk_processing = True
        self.bulk_progress = 0

        if self.use_metrics:
            self.metrics = [] 

        all_lifes = [open(expanduser(join(self.config['input_path'], f)), 'r', encoding='utf8').read() for f in self.life_queue]
        all_lifes = ''.join(all_lifes)

        lifes = Life()
        lifes.from_string(all_lifes)

        start_time = datetime.now().timestamp()
        while len(list(self.queue.values())) > 0:
            start = datetime.now().timestamp() - start_time
            
            if self.use_metrics:
                self.metrics.append({})
            
            life = next((day for day in lifes if day.date == self.current_day.replace("-", "_")), "")
            # preview -> adjust
            self.process({'changes': [], 'LIFE': ''})
            # adjust -> annotate
            self.process({'changes': [], 'LIFE': ''})
            # annotate -> store
            self.process({'changes': [], 'LIFE': str(life)}, self.config["bulk_calculate_canonical"])

            # Register metrics
            if self.use_metrics:
                self.edit_latest_metrics("start", start) # start represents seconds since bulk processing started
                self.edit_latest_metrics("day", processed)
                self.edit_latest_metrics("duration", (datetime.now().timestamp() - start_time) - start)

            print(f"{processed}/{total_num_days} days processed")
            self.bulk_progress = (processed / total_num_days) * 100
            processed += 1

        for life_file in self.life_queue:
            life_path = join(expanduser(self.config['input_path']), life_file)
            backup_path = join(expanduser(self.config['backup_path']), life_file)
            rename(life_path, backup_path)

        self.life_queue = []

        if self.use_metrics:
            with open('metrics.json', 'w') as metrics_file:
                json.dump(self.metrics, metrics_file)
            self.metrics = []

        self.is_bulk_processing = False
        self.bulk_progress = -1
 
    def preview_to_adjust(self, track):
        """ Processes a track so that it becomes a trip

        More information in `tracktotrip3.Track`'s `to_trip` method

        Args:
            track (:obj:`tracktotrip3.Track`)
            changes (:obj:`list` of :obj:`dict`): Details of, user made, changes
        Returns:
            :obj:`tracktotrip3.Track`
        """
        config = self.config

        if not track.name or len(track.name) == 0:
            track.name = track.generate_name(config['trip_name_format'])

        track.timezone(timezone=float(config['default_timezone']))
        track = track.to_trip(
            smooth=config['smoothing']['use'],
            smooth_strategy=config['smoothing']['algorithm'],
            smooth_noise=config['smoothing']['noise'],
            seg=config['segmentation']['use'],
            seg_eps=config['segmentation']['epsilon'],
            seg_min_time=config['segmentation']['min_time'],
            simplify=config['simplification']['use'],
            simplify_max_dist_error=config['simplification']['max_dist_error'],
            simplify_max_speed_error=config['simplification']['max_speed_error']
        )

        return track

    def adjust_to_annotate(self, track):
        """ Extracts location from track

        Args:
            track (:obj:`tracktotrip3.Track`)
        Returns:
            :obj:`tracktotrip3.Track`
        """

        config = self.config
        c_loc = config['location']

        conn, cur = self.db_connect()

        def get_locations(point, radius):
            """ Gets locations within a radius of a point

            See `db.query_locations`

            Args:
                point (:obj:`tracktotrip3.Point`)
                radius (float): Radius, in meters
            Returns:
                :obj:`list` of (str, ?, ?)
            """
            if cur:
                return db.query_locations(cur, point.lat, point.lon, radius, self.debug)
            else:
                return []

        # Does not use APIs to infer location in annotate step
        track.infer_location(
            get_locations,
            max_distance=c_loc['max_distance'],
            use_google=False,
            google_key=c_loc['google_key'],
            use_foursquare=False,
            foursquare_key=c_loc['foursquare_key'],
            limit=c_loc['limit']
        )

        db.dispose(conn, cur)

        return track

    def annotate_to_next(self, track, life, calculate_canonical=True):
        """ Stores the track and dequeues another track to be
        processed.

        Moves the current GPX file from the input path to the
        backup path, creates a LIFE file in the life path
        and creates a trip entry in the database. Finally the
        trip is exported as a GPX file to the output path.

        Args:
            track (:obj:tracktotrip3.Track`)
            changes (:obj:`list` of :obj:`dict`): Details of, user made, changes
            calculate_canonical (bool): If true, calculates canonical trips and locations 
        """

        if not track.name or len(track.name) == 0:
            track.name = track.generate_name(self.config['trip_name_format'])
        
        # Is editing if a file exists in output with the day's date
        output_files = glob.glob(self.config['output_path'] + f'{track.name}*')
        is_edit = len(output_files) > 0

        # Metrics

        if self.use_metrics:
            n_points = sum([len(segment.points) for segment in track.segments])
            
            self.edit_latest_metrics("segments", len(track.segments))
            self.edit_latest_metrics("points", n_points)

        # Export trip to GPX
        if self.config['output_path']:
            if self.config['multiple_gpxs_for_day']:
                i = 1
                for segment in track.segments:
                    seg = tt.Track('', [segment], debug=self.debug)
                    name = track.name.split('.')[0] 
                    output_path = join(expanduser(self.config['output_path']), name + f'_{i}.gpx')
                    i += 1
                    save_to_file(output_path, seg.to_gpx())
            else:
                output_path = join(expanduser(self.config['output_path']), track.name)
                save_to_file(output_path, track.to_gpx())

        # To LIFE
        if self.config['life_path']:
            name = '.'.join(track.name.split('.')[:-1])
            save_to_file(join(expanduser(self.config['life_path']), name + '.life'), life)

            if self.config['life_all']:
                life_all_file = expanduser(self.config['life_all'])
            else:
                life_all_file = join(expanduser(self.config['life_path']), 'all.life')

            if is_edit:
                all_lifes = open(life_all_file, 'r').read()
                lifes = Life()
                lifes.from_string(all_lifes)
                life_date = self.current_day.replace('-','_')

                lifes.update_day_from_string(life_date, life)
                save_to_file(life_all_file, repr(lifes))
            else:
                save_to_file(life_all_file, "%s\n\n" % life, mode='a+')

        conn, cur = self.db_connect()

        if conn and cur:
            if is_edit:
                if self.debug:
                    print(f"updating day: {self.current_day}")
                db.remove_trips_from_day(cur, self.current_day, self.debug)

            db.load_from_segments_annotated(
                cur,
                self.current_track(),
                life,
                self.config['location']['max_distance'],
                self.config['location']['min_samples'],
                True,
                self.debug
            )

            def insert_can_trip(can_trip, mother_trip_id):
                """ Insert a cannonical trip into the database

                See `db.insert_canonical_trip`

                Args:
                    can_trip (:obj:`tracktotrip3.Segment`): Canonical trip
                    mother_trip_id (int): Id of the trip that originated the canonical
                        representation
                Returns:
                    int: Canonical trip id
                """
                return db.insert_canonical_trip(cur, can_trip, mother_trip_id, self.debug)

            def update_can_trip(can_id, trip, mother_trip_id):
                """ Updates a cannonical trip on the database

                See `db.update_canonical_trip`

                Args:
                    can_id (int): Canonical trip id
                    trip (:obj:`tracktotrip3.Segment`): Canonical trip
                    mother_trip_id (int): Id of the trip that originated the canonical
                        representation
                """
                db.update_canonical_trip(cur, can_id, trip, mother_trip_id, self.debug)

            trips_ids = []
            for trip in track.segments:
                # To database
                trip_id = db.insert_segment(
                    cur,
                    trip,
                    self.config['location']['max_distance'],
                    self.config['location']['min_samples'],
                    self.debug
                )
                trips_ids.append(trip_id)

                if calculate_canonical:
                    d_latlon = estimate_meters_to_deg(self.config['location']['max_distance'], debug=self.debug)
                    # Build/learn canonical trip
                    canonical_trips = db.match_canonical_trip(cur, trip, d_latlon, self.debug)

                    if self.debug:
                        print("canonical_trips # = %d" % len(canonical_trips))

                    learn_trip(
                        trip,
                        trip_id,
                        canonical_trips,
                        insert_can_trip,
                        update_can_trip,
                        self.config['simplification']['eps'],
                        d_latlon,
                        debug=self.debug
                    )

            db.dispose(conn, cur)

        # Backup
        if self.config['backup_path']:
            for gpx in self.queue[self.current_day]:
                from_path = gpx['path']
                to_path = join(expanduser(self.config['backup_path']), gpx['name'])
                
                if isfile(to_path):
                    replace(from_path, to_path)
                else:
                    rename(from_path, to_path)

        self.next_day()

        if (self.current_day == None):
            self.current_step = Step.done
        else:
            self.current_step = Step.preview

        return self.current_track()


    def current_track(self):
        """ Gets the current trip/track

        It includes all trips/tracks of the day

        Returns:
            :obj:`tracktotrip3.Track` or None
        """
        if self.current_step is Step.done:
            return None
        elif len(self.history) > 0:
            return self.history[-1]
        else:
            return None

    def current_state(self):
        """ Gets the current processing/server state

        Returns:
            :obj:`dict`
        """
        current = self.current_track()
        return {
            'step': self.current_step,
            'queue': list(self.queue.items()),
            'track': current.to_json() if current else None,
            'life': self.get_life(current) if current and self.current_step is Step.annotate else '',
            'currentDay': self.current_day,
            'lifeQueue': self.life_queue,
            'isBulkProcessing': self.is_bulk_processing
        }

    def get_life(self, track):
        """ Generates LIFE file from track, or uses existing one for track's day
        """
        return track.to_life(self.config["trip_annotations"])

    def complete_trip(self, from_point, to_point):
        """ Generates possible ways to complete a set of trips

        Possible completions are only generated between start and end of each pair of
            trips (ordered by the starting time)

        Args:
            data (:obj:`dict`): Requires keys 'from' and 'to', which should countain
                point representations with 'lat' and 'lon'.
            from_point (:obj:`tracktotrip3.Point`): with keys lat and lon
            to_point (:obj:`tracktotrip3.Point`): with keys lat and lon
        Returns:
            :obj:`tracktotrip3.Track`
        """
        distance = estimate_meters_to_deg(self.config['location']['max_distance'], debug=self.debug) * 2
        b_box = (
            min(from_point.lat, to_point.lat) - distance,
            min(from_point.lon, to_point.lon) - distance,
            max(from_point.lat, to_point.lat) + distance,
            max(from_point.lon, to_point.lon) + distance
        )

        canonical_trips = []
        conn, cur = self.db_connect()
        if conn and cur:
            # get matching canonical trips, based on bounding box
            canonical_trips = db.match_canonical_trip_bounds(cur, b_box, self.debug)
            db.dispose(conn, cur)
            if self.debug:
                print((len(canonical_trips)))

        return complete_trip(canonical_trips, from_point, to_point, self.config['location']['max_distance'], debug=self.debug)

    def load_life(self, content):
        """ Adds LIFE content to the database

        See `db.load_from_life`

        Args:
            content (str): LIFE formated string
        """
        conn, cur = self.db_connect()

        if conn and cur:
            db.load_from_segments_annotated(
                cur,
                tt.Track('', [], debug=self.debug),
                str(content, 'utf-8'),
                self.config['location']['max_distance'],
                self.config['location']['min_samples'],
                debug=self.debug
            )

        db.dispose(conn, cur)

    def update_config(self, new_config):
        """ Updates the config object by overlapping with the new config object

        Args:
            new_config (obj): JSON object that contains configuration changes 
        """
        super().update_config(new_config)
        if self.current_step is Step.done:
            self.load_days()

    def location_suggestion(self, point):
        c_loc = self.config['location']
        conn, cur = self.db_connect()

        def get_locations(point, radius):
            """ Gets locations within a radius of a point

            See `db.query_locations`

            Args:
                point (:obj:`tracktotrip3.Point`)
                radius (float): Radius, in meters
            Returns:
                :obj:`list` of (str, ?, ?)
            """
            if cur:
                return db.query_locations(cur, point.lat, point.lon, radius, self.debug)
            else:
                return []

        locs = infer_location(
            point,
            get_locations,
            max_distance=c_loc['max_distance'],
            use_google=c_loc['use'] and c_loc['use_google'],
            google_key=c_loc['google_key'],
            use_foursquare=c_loc['use'] and c_loc['use_foursquare'],
            foursquare_key=c_loc['foursquare_key'],
            limit=c_loc['limit'],
            debug=self.debug
        )
        db.dispose(conn, cur)

        return locs.to_json()

    def get_canonical_trips(self):
        """ Fetches all canonical trips from the database

        See `db.get_canonical_trips`

        Returns:
            :obj:`list` of :obj:`dict`
        """

        conn, cur = self.db_connect()
        result = []
        if conn and cur:
            result = db.get_canonical_trips(cur, self.debug)
        for val in result:
            val['points'] = val['points'].to_json()
            val['points']['id'] = val['id']
        db.dispose(conn, cur)
        return [r['points'] for r in result]

    def get_canonical_locations(self):
        """ Fetches all canonical locations from the database

        See `db.get_canonical_locations`

        Returns:
            :obj:`list` of :obj:`dict`
        """

        conn, cur = self.db_connect()
        result = []
        if conn and cur:
            result = db.get_canonical_locations(cur, self.debug)
        for val in result:
            val['points'] = val['points'].to_json()
            val['points']['label'] = val['label']
        db.dispose(conn, cur)
        return [r['points'] for r in result]

    def dismiss_day(self, day):
        """ Ignores a day in the queue
        
        Args:
            day (str)
        """
        existing_days = list(self.queue.keys())
        if day in existing_days:
            if day == self.current_day:
                if len(existing_days) > 1:
                    self.next_day()
                else:
                    self.queue = {}
                    self.current_day = None
                    self.current_step = Step.done
                    self.history = []
            else:
                del self.queue[day]

    def remove_day(self, files):
        """ Removes a day from the queue and deletes the corresponding input files
        
        Args:
            files (:obj:`list` of :obj:`dict`)
        """
        for file in files:
            self.dismiss_day(file["date"])
            remove(file["path"])

    def copy_day_to_input(self, day):
        """ Copies an already processed day from the output folder to the input folder to be edited 
        
        Args:
            day (str)
        """
        
        day_datetime = datetime.strptime(day, "%Y-%m-%d")
        
        # Take output name format into consideration (format can be changed in config)
        date = day_datetime.strftime(self.config['trip_name_format']) 

        # Get all files with day's date
        output_files = glob.glob(self.config['output_path'] + f'{date}*')

        for day_gpx_path in output_files:
            file_name = day_gpx_path.replace(expanduser(self.config['output_path']), '') 
            copyfile(day_gpx_path, join(expanduser(self.config['input_path']), file_name))
        
        self.reload_queue()
        self.change_day(day)

    def update_day(self, filename):
        """ Updates current day if a GPX file with the same date is inputted

        Used when a file is dragged into the input folder using the frontend

        Args:
            filename (str): name of the inputted GPX file
        """

        self.reload_queue()
        date = predict_start_date(self.config["input_path"] + filename)
        day = date.strftime(self.config['trip_name_format'])

        if day == self.current_day:
            self.change_day(date.strftime(self.config['trip_name_format']))
        
        
        
        

