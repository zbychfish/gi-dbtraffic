import datetime
import argparse
from configparser import ConfigParser
from files.common import connect_to_database, set_activity_defaults, clean_schema, deploy_schema
from files.definitions import application_traffic
parser = argparse.ArgumentParser()
config = ConfigParser()
config.read('files/config.cfg')
print(config.get('db', 'type'))

parser.add_argument('-v', help='Verbose output', required=False, action='store_true')
parser.add_argument('-a', help='action', default='app_flow', choices=['app_flow', 'clean', 'schema', 'rebuild'])
parser.add_argument('-t', help='execution time in minutes', default=60, type=int)
parser.add_argument('-s', help='speed', default='slow', choices=['slow', 'normal', 'fast', 'insane'])
c_args = parser.parse_args()

if config.get('db', 'type') in ['postgres', 'mysql']:
    conn = connect_to_database(config, config.get('db', 'user'), config.get('db', 'password'))
else:
    print('No supported database in config')
    exit(102)
if conn[1] != 'OK':
    print('Connection problem {}'.format(conn[1]))
    exit(103)
end_time = datetime.datetime.now() + datetime.timedelta(minutes=c_args.t)
print('Script will finish execution:', end_time)
if c_args.a == 'clean' or c_args.a == 'rebuild':
    clean_schema(conn[0], config)
if c_args.a == 'schema' or c_args.a == 'rebuild':
    deploy_schema(conn[0], config)
if c_args.a == 'app_flow':
    session_defaults = set_activity_defaults(conn[0], config)
conn[0].close()
if c_args.a == 'app_flow':
    application_traffic(config, session_defaults, end_time, c_args)
