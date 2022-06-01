# MyBigLife Backend

## Setup

### Requirements

The required Python libraries to run the backend server can be found in the "requirements.txt" file. To install them, you can run:

```
 $ pip install -r requirements.txt
```

### TrackToTrip Manual Install

As it stands, [TrackToTrip](https://github.com/dserafim1999/TrackToTrip/) must be installed separately. Once downloaded, you can install it by running the following command in the project's directory:

```
 $ python setup.py install
```

If you are using a virtual environment to install MyBigLife Backend"s requirements, make sure to run the previous command with the virtual environment active.

**NOTE:** TrackToTrip requires Microsoft Visual C++ 14.0. It can be found using the [Build Tools for Visual Studio 2022](https://visualstudio.microsoft.com/downloads/?q=build+tools)


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
