import random
import datetime
from configparser import ConfigParser

config = ConfigParser()
config.read('files/config.cfg')
print(config.get('db', 'type'))

if config.get('db', 'type') == 'aws_kinesis_postgres':
    from files.aws_kinesis_postgres import connect_to_database, deploy_schema, cleanup_schema,\
        set_activity_defaults, application_traffic
    conn = connect_to_database(config, config.get('db', 'user'), config.get('db', 'password'))
else:
    print('No supported database in config')
    exit(102)

if conn[1] != 'OK':
    print('Connection problem {}'.format(conn[1]))
    exit (103)

execution_time = 720
end_time = datetime.datetime.now() + datetime.timedelta(minutes=execution_time)
print(end_time)
#cleanup_schema(conn[0], config)
#deploy_schema(conn[0], config)

session_defaults = set_activity_defaults(conn[0])
conn[0].close()
application_traffic(config, session_defaults, end_time)
