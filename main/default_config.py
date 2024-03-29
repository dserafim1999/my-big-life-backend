"""
Base line settings
"""

CONFIG = {
    'input_path': None,
    'backup_path': None,
    'output_path': None,
    'life_path': None,
    'life_all': None,
    'db': {
        'host': None,
        'port': None,
        'name': None,
        'user': None,
        'pass': None
    },
    'default_timezone': 0,
    'trip_annotations': False,
    'bulk_calculate_canonical': True,
    'load_more_amount': 10,
    'trip_name_format': '%Y-%m-%d',
    'multiple_gpxs_for_day': False,
    'smoothing': {
        'use': True,
        'algorithm': 'inverse',
        'noise': 1000
    },
    'segmentation': {
        'use': True,
        'epsilon': 0.01,
        'min_time': 60
    },
    'simplification': {
        'use': True,
        'max_dist_error': 2.0,
        'max_speed_error': 1.0,
        'eps': 0.000015
    },
    'location': {
        'use': True,
        'max_distance': 20,
        'min_samples': 2,
        'limit': 5,
        'use_google': True,
        'google_key': '',
        'use_foursquare': True,
        'foursquare_key': ''
    },
}
