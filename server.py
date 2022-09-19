# -*- coding: utf-8 -*-
"""
Entry point
Spawns a server that coodinates the operations
"""
import argparse
from urllib import response
from flask import Flask, request, jsonify
from tracktotrip3 import Point
from queries.query_manager import QueryManager
from trackprocessing.process_manager import ProcessingManager
from main.main_manager import MainManager

parser = argparse.ArgumentParser(description='Starts the server that manages/processes tracks')
parser.add_argument('-p', '--port', dest='port', metavar='p', type=int,
        default=5000,
        help='port to use')
parser.add_argument('-H', '--host', dest='host', metavar='h', type=str,
        default='0.0.0.0',
        help='host name')
parser.add_argument('--debug', dest='debug', action='store_true',
        default=False,
        help='print server debug information')
parser.add_argument('--verbose', dest='verbose',
        action='store_false',
        help='print debug information of processing stage')
parser.add_argument('--config', '-c', dest='config', metavar='c', type=str,
        help='configuration file')
parser.add_argument('--metrics', '-m', dest='metrics', metavar='m', type=bool,
        help='register processing metrics')
args = parser.parse_args()

app = Flask(__name__)

processing_metrics = args.metrics if args.metrics != None else False

manager = MainManager(args.config, args.debug)
processing_manager = ProcessingManager(args.config, args.metrics, args.debug)
query_manager = QueryManager(args.config, args.debug)


# ROUTES

# General

@app.route('/tripsLocations', methods=['GET'])
def get_trips_and_locations():
    """ Gets canonical trips and locations
    Returns:
        :obj:`flask.response`
    """

    response = jsonify(manager.get_trips_and_locations())
    return set_headers(response)

@app.route('/locations', methods=['GET'])
def get_locations():
    """ Gets canonical locations
    Returns:
        :obj:`flask.response`
    """

    response = jsonify(manager.get_locations())
    return set_headers(response)

@app.route('/canonicalTrips', methods=['GET'])
def get_canonical_trips():
    """ Gets canonical trips
    Returns:
        :obj:`flask.response`
    """

    response = jsonify(manager.get_canonical_trips())
    return set_headers(response)

@app.route('/trips', methods=['GET'])
def get_trips():
    """ Gets trips in bounds 
    Returns:
        :obj:`flask.response`
    """

    latMin = request.args.get('latMin')
    lonMin = request.args.get('lonMin')
    latMax = request.args.get('latMax')
    lonMax = request.args.get('lonMax')
    canonical = request.args.get('canonical') == 'true'
    
    response = jsonify(manager.get_trips(latMin, lonMin, latMax, lonMax, canonical))
    return set_headers(response)

@app.route('/hasMoreTrips', methods=['GET'])
def can_get_more_trips():
    """ Loads more trips in bounds
    Returns:
        :obj:`flask.response`
    """

    latMin = request.args.get('latMin')
    lonMin = request.args.get('lonMin')
    latMax = request.args.get('latMax')
    lonMax = request.args.get('lonMax')
    canonical = request.args.get('canonical') == 'true'
    
    response =  jsonify(manager.can_get_more_trips(latMin, lonMin, latMax, lonMax, canonical))

    return set_headers(response)

@app.route('/moreTrips', methods=['GET'])
def get_more_trips():
    """ Loads more trips in bounds
    Returns:
        :obj:`flask.response`
    """

    latMin = request.args.get('latMin')
    lonMin = request.args.get('lonMin')
    latMax = request.args.get('latMax')
    lonMax = request.args.get('lonMax')
    canonical = request.args.get('canonical') == 'true'
    
    response = jsonify(manager.get_more_trips(latMin, lonMin, latMax, lonMax, canonical))
    return set_headers(response)

@app.route('/uploadFile', methods=['POST'])
def upload_file():
    """ Receives file name and data to create file in input
    Returns:
        :obj:`flask.response`
    """

    payload = request.get_json(force=True)
    manager.upload_file(payload)
    processing_manager.update_day(payload["name"])
    return send_state()

@app.route('/allTrips', methods=['GET'])
def get_all_trips():    
    """ Loads all trips
    Returns:
        :obj:`flask.response`
    """
    
    response = jsonify(manager.get_all_trips())
    return set_headers(response)


@app.route('/config', methods=['POST'])
def set_configuration():
    """ Sets the current configuration, and returns it

    Returns:
        :obj:`flask.response`
    """
    payload = request.get_json(force=True)
    manager.update_config(payload)
    processing_manager.update_config(payload)
    query_manager.update_config(payload)
    return set_headers(jsonify(manager.config))

@app.route('/config', methods=['GET'])
def get_configuration():
    """ Gets the current configuration, and returns it

    Returns:
        :obj:`flask.response`
    """
    return set_headers(jsonify(manager.config))

@app.route('/lifeFromDay', methods=['POST'])
def get_life_from_day():
    """ Loads LIFE string for a specific day
    Returns:
        :obj:`flask.response`
    """
    payload = request.get_json(force=True)
    response = jsonify(manager.get_life_from_day(payload["date"]))

    return set_headers(response)

@app.route('/life', methods=['GET'])
def get_global_life():
    """ Loads global LIFE JSON 
        :obj:`flask.response`
    """
    
    response = jsonify(manager.get_global_life_json())

    return set_headers(response)

@app.route('/deleteDay', methods=['POST'])
def delete_day():
    """ Deletes a day from the database and existing files
    Returns:
        :obj:`flask.response`
    """
    payload = request.get_json(force=True)
    
    manager.delete_day(payload["date"])

    return send_state()

# Track Processing

@app.route('/process/previous', methods=['GET'])
def previous():
    """Restores a previous state

    Returns:
        :obj:`flask.response`
    """
    processing_manager.restore()
    return send_state()

@app.route('/process/next', methods=['POST'])
def next():
    """Advances the progress

    Returns:
        :obj:`flask.response`
    """
    payload = request.get_json(force=True)
    processing_manager.process(payload)
    return send_state()

@app.route('/process/current', methods=['GET'])
def current():
    """Gets the current state of the execution

    Returns:
        :obj:`flask.response`
    """
    return send_state()

@app.route('/process/completeTrip', methods=['POST'])
def complete_trip():
    """Gets trips already made from one point, to another

    Returns:
        :obj:`flask.response`
    """
    payload = request.get_json(force=True)
    from_point = Point.from_json(payload['from'], args.debug)
    to_point = Point.from_json(payload['to'], args.debug)
    return set_headers(jsonify(processing_manager.complete_trip(from_point, to_point)))

@app.route('/process/changeDay', methods=['POST'])
def change_day():
    """ Changes the current day

    Returns:
        :obj:`flask.response`
    """
    payload = request.get_json(force=True)
    processing_manager.change_day(payload['day'])
    return send_state()

@app.route('/process/reloadQueue', methods=['GET'])
def reload_queue():
    """ Changes the current day

    Returns:
        :obj:`flask.response`
    """
    processing_manager.reload_queue()
    return send_state()

@app.route('/process/bulkProgress', methods=['GET'])
def bulk_progress():
    """ Returns bulk processing progress status

    Returns:
        :obj:`flask.response`
    """
    response = jsonify(processing_manager.get_bulk_progress())
    return set_headers(response)


@app.route('/process/bulk', methods=['GET'])
def bulk_process():
    """ Starts bulk processing

    Returns:
        :obj:`flask.response`
    """
    processing_manager.bulk_process(raw=False)
    return send_state()

@app.route('/process/loadLIFE', methods=['POST'])
def load_life():
    """ Loads a life formated string into the database

    Returns:
        :obj:`flask.response`
    """
    payload = request.data
    processing_manager.load_life(payload)
    return send_state()

@app.route('/process/location', methods=['GET'])
def location_suggestion():
    """ Gets a location suggestion

    Returns:
        :obj:`list` of :obj:`str`
    """
    lat = float(request.args.get('lat'))
    lon = float(request.args.get('lon'))
    response = jsonify(processing_manager.location_suggestion(Point(lat, lon, None)))
    return set_headers(response)

@app.route('/process/dismissDay', methods=['POST'])
def dismiss_day():
    """ Ignores day in queue
    Returns:
        :obj:`flask.response`
    """

    payload = request.get_json(force=True)
    processing_manager.dismiss_day(payload["day"])
    return send_state()

@app.route('/process/removeDay', methods=['POST'])
def remove_day():
    """ Removes day from input
    Returns:
        :obj:`flask.response`
    """

    payload = request.get_json(force=True)
    processing_manager.remove_day(payload["files"])
    return send_state()

@app.route('/process/skipDay', methods=['POST'])
def skip_day():
    """ Skips day in queue
    Returns:
        :obj:`flask.response`
    """
    processing_manager.next_day(delete=False)
    return send_state()

@app.route('/process/copyDayToInput', methods=['POST'])
def copy_day_to_input():
    """ Copies GPX file from day from output to input folder
    Returns:
        :obj:`flask.response`
    """
    payload = request.get_json(force=True)
    processing_manager.copy_day_to_input(payload["date"])
    return send_state()

# Queries 

@app.route('/queries/execute', methods=['POST'])
def execute_query():
    """ Executes query based on query JSON object
    Returns:
        :obj:`flask.response`
    """
    payload = request.get_json(force=True)
    response = jsonify(query_manager.execute_query(payload))
    
    return set_headers(response)

@app.route('/queries/loadMoreResults', methods=['POST'])
def load_more_query_results():
    """ Loads query results
    Returns:
        :obj:`flask.response`
    """
    response = jsonify(query_manager.load_results(False))
    
    return set_headers(response)

# Helpers

def set_headers(response):
    """ Sets appropriate headers

    Args:
        response (:obj:`flask.response`)
    Returns:
        :obj:`flask.response`
    """
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response

def send_state():
    """ Helper function to send state

    Creates a response with the current state, converts it to JSON and sets its headers

    Returns:
        :obj:`flask.response`
    """
    response = jsonify(processing_manager.current_state())
    return set_headers(response)

def undo_step():
    """ Undo current state
    """
    processing_manager.restore()

if __name__ == '__main__':
    app.run(debug=args.debug, port=args.port, host=args.host, threaded=True)
