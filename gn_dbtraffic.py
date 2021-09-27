import random
import datetime
import argparse
from configparser import ConfigParser

parser = argparse.ArgumentParser()
config = ConfigParser()
config.read('files/config.cfg')
print(config.get('db', 'type'))

parser.add_argument('-v', help='Verbose output', required=False, action='store_true')
parser.add_argument('-a', help='action', default='app_flow', choices=['app_flow', 'clean', 'schema', 'rebuild'])
parser.add_argument('-t', help='execution time in minutes', default=60, type=int)
c_args = parser.parse_args()
if config.get('db', 'type') == 'aws_kinesis_postgres':
    from files.aws_kinesis_postgres import connect_to_database, deploy_schema, cleanup_schema,\
        set_activity_defaults, application_traffic
    conn = connect_to_database(config, config.get('db', 'user'), config.get('db', 'password'))
else:
    print('No supported database in config')
    exit(102)

if conn[1] != 'OK':
    print('Connection problem {}'.format(conn[1]))
    exit(103)

end_time = datetime.datetime.now() + datetime.timedelta(minutes=c_args.t)
print('Script will finish execustion:', end_time)
if c_args == 'clean' or c_args == 'rebuild':
    cleanup_schema(conn[0], config)
if c_args == 'schema' or c_args == 'rebuild':
    deploy_schema(conn[0], config)
if c_args.a == 'app_flow':
    session_defaults = set_activity_defaults(conn[0])
conn[0].close()
if c_args.a == 'app_flow':
    application_traffic(config, session_defaults, end_time, c_args)
