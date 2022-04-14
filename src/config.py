import os.path
from os.path import exists
from yaml import safe_load


def load_config(file_name='/../config.yaml'):
    file = os.path.dirname(__file__) + file_name
    if exists(file):
        with open(file, 'r') as f:
            config = safe_load(f)
        return config
    return {}


def db_config():
    try:
        return load_config()['DATABASE']
    except KeyError:
        print('[!!] Missing field in config file!')
        return {}
