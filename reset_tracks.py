from dataclasses import replace
import json
from os import listdir, replace, remove
from os.path import join, expanduser, isfile
from main import db

from main.default_config import CONFIG

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

def file_details(base_path, filepath):
    """ Returns file details

    Example:
        >>> file_details('/users/username/tracks/', '25072016.gpx')
        {
            'name': '25072016.gpx',
            'path': '/users/username/tracks/25072016.gpx'
        }

    Args:
        base_path (str): Base path
        filename (str): Filename
    Returns:
        :obj:`dict`: See example
    """
    complete_path = join(base_path, filepath)

    return {
        'name': filepath,
        'path': complete_path
    }

class TracksReset(object):
    """ Restores track files and database to the original state, to ease development

    Arguments:
        INPUT_PATH: String with the path to the input folder
        BACKUP_PATH: String with the path to the backup folder
        OUTPUT_PATH: String with the path to the output folder
        LIFE_PATH: String with the path to the LIFE output
            folder
    """

    def __init__(self, config_file):
        self.config = dict(CONFIG)

        if config_file and isfile(expanduser(config_file)):
            with open(expanduser(config_file), 'r') as config_file:
                config = json.loads(config_file.read())
                update_dict(self.config, config)


    def list_files(self, path):
        """ Lists files from path, and some details

        Returns:
            :obj:`list` of :obj:`dict`
        """
        if not path:
            return []

        input_path = expanduser(path)
        files = listdir(input_path)

        files = [file_details(input_path, f) for f in files]
        return files

    def backup_to_input(self):
        """ Moves files from backup folder to input
        """
        files = self.list_files(self.config['backup_path'])
        for f in files:
            replace(f['path'], self.config['input_path'] + f['name'])

    def remove_files(self, folder_path):
        """Removes all files in folder

        Args:
            path (str): Path to folder where files to be eliminated are located
        """
        files = self.list_files(folder_path)
        for f in files:
            remove(f['path'])

    def reset_life_file(self):
        """Cleans main Life file located in 'life_all' path
        """

        if isfile(expanduser(self.config['life_all'])):
            with open(expanduser(self.config['life_all']), 'r+') as life:
                life.truncate(0)
                life.close()
  
    def reset_track_files(self):
        """ Applies all steps to reset tracks state
        """
        self.backup_to_input()
        self.remove_files(self.config['output_path'])
        self.remove_files(self.config['life_path'])
        self.reset_life_file()
        self.reset_db()

    def reset_db(self):
        """ Drops all tables in database and recreates them
        """
        conn, cur = self.db_connect()

        drop_sql = open('drop.sql', 'r')
        schema_sql = open('schema.sql', 'r')


        drop_query = drop_sql.read()
        schema_query = schema_sql.read()

        if cur:
            db.execute_query(cur, drop_query)
            db.execute_query(cur, schema_query)

        db.dispose(conn, cur)

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




def main():
    reset = TracksReset('config.json')
    reset.reset_track_files()

if __name__ == "__main__":
    main()