import configparser
from configparser import ConfigParser
from files.aws_kinesis_postgres import cleanup_schema_postgres,      deploy_schema_postgres
from files.mysql import cleanup_schema_mysql, deploy_schema_mysql
from files.definitions import execute_sql, connect_to_postgres, connect_to_mysql


def connect_to_database(config: ConfigParser, user: str, password: str) -> [object, str]:
    if config.get('db', 'type') == 'postgres':
        return connect_to_postgres(config, user, password)
    elif config.get('db', 'type') == 'mysql':
        return connect_to_mysql(config, user, password)


def set_activity_defaults(conn: object) -> [int]:
    cur = conn.cursor()
    execute_sql(cur, 'SELECT COUNT(*) from gn_app.customers')
    customers_number = cur.fetchone()[0] - 1
    return [customers_number]


def clean_schema(conn: object, config: configparser.ConfigParser):
    if config.get('db', 'type') == 'postgres':
        cleanup_schema_postgres(conn, config)
    elif config.get('db', 'type') == 'mysql':
        cleanup_schema_mysql(conn, config)


def deploy_schema(conn: object, config: configparser.ConfigParser):
    if config.get('db', 'type') == 'postgres':
        deploy_schema_postgres(conn, config)
    elif config.get('db', 'type') == 'mysql':
        deploy_schema_mysql(conn, config)
