# MyBigLife Backend

_MyBigLife_ enables users to easily process, analyse
and query their spatial and temporal information with a strong emphasis on personal semantics and
the power they have over their own data. The frontend for this project can be found [here](https://github.com/dserafim1999/my-big-life).

_TrackToTrip3_ is used for track processing operations, and can be found [here](https://github.com/dserafim1999/tracktotrip3).

## Setup

### Requirements

The required Python libraries to run the backend server can be found in the "requirements.txt" file. To install them, you can run:

```
 $ pip install -r requirements.txt
```

If you are using a virtual environment to install MyBigLife Backend's requirements, make sure to run the previous command with the virtual environment active.

**NOTE:** tracktotrip3 requires Microsoft Visual C++ 14.0. It can be found using the [Build Tools for Visual Studio 2022](https://visualstudio.microsoft.com/downloads/?q=build+tools)


## Config

A few parameters need to be adjusted in order to run the backend. A JSON file should be created so that it can be passed as a parameter when launching the program.

- **input_path**: defines the directory where the input .gpx files are located 
- **backup_path**: defines the directory where the original .gpx files are saved after processing  
- **output_path**: defines the directory where the processed .gpx files will be stored
- **life_path**: defines the directory where the [LIFE](https://github.com/domiriel/LIFE) files are located
- **life_all**: defines the path of the global [LIFE](https://github.com/domiriel/LIFE) file that will be updated after processing
- **db.host**: database host
- **db.port**: database port
- **db.name**: database name
- **db.user**: database user
- **db.pass**: database password

JSON File Example with required parameters:

```
{
    "input_path": "\input",
    "backup_path": "\backup",
    "output_path": "\output",
    "life_path": "\life",
    "life_all": "all.life",
    "db": {
        "host": "localhost",
        "port": "5432",
        "name": "postgres",
        "user": "postgres",
        "pass": "postgres"
    }
}
```

## Database

Database access is not mandatory, however some functionalities may not work. Create a PostgreSQL 14 database with PostGis 3.2. The latter can be installed using Stack Builder, which comes bundled with the PostgreSQL instalation.

The database should be populated with the "schema.sql" file.

## Run

## 

The program can be run by using the following command:

```
 $ python server.py --config [path_to_config_json]
```

The server is highly parameterable, use the following command for more options:

```
$ python server.py --help
```

## Reset Tracks

The database can be reset byr running the following command:

```
 $ python reset_tracks.py
```

This command will also move the tracks saved in the backup folder back into the input folder, removing files from the output and life folders, in order to revert to the initial state for development.

## Add a new manager

A manager's goal is to keep module specific logic self contained within their respective folders. The idea
is: as new functionality is introduced, its logic is contained in its own manager file which helps keep the system modular for work to come. 

Therefore, the first step is creating a new folder where the manager will be stored. After that, a manager file should be created containing a class. This class should have, at least, a configuration attribute to store the current system configurations, and a debug boolean, to use when certain logic is restricted to debug mode.

A default manager should look something like this (this template can be found in the utils file):
```python
from os.path import expanduser, isfile
from utils import update_dict
from main.default_config import CONFIG
from main import db
import json

class Manager(object):
    def __init__(self, config_file, debug):
        self.config = dict(CONFIG) # default configuration
        self.debug = debug

        if config_file and isfile(expanduser(config_file)):
            with open(expanduser(config_file), 'r') as config_file:
                config = json.loads(config_file.read())
                update_dict(self.config, config)
        
    def update_config(self, new_config):
        update_dict(self.config, new_config)

    def db_connect(self):
        dbc = self.config['db']
        conn = db.connect_db(dbc['host'], dbc['name'], dbc['user'], dbc['port'], dbc['pass'])
        if conn:
            return conn, conn.cursor()
        else:
            return None, None
```

After implementing the logic for your manager, it's time to link it to the server. Head to the `server.py` file and create a new instance of the manager. Then you can create the endpoints you wish to add to this manager. To keep endpoints consistent within their managers, a convention was set where you prefix the endpoint's route with a name that identifies the managers behaviour. For instance, if we wanted to add a 'play' endpoint to a video manager, we could name give it the route '/video/play'. 