from configparser import ConfigParser
from files.definitions import execute_sql, is_object, add_customer
try:
    import psycopg2
except ImportError:
    print("Install psycopg2 to start \"pip3 install psycopg2_binary\"")
    print("This module can require OS packages: postgres, postgres-devel, gcc, python3-devel")
    exit(101)
try:
    from faker import Faker
except ImportError:
    print("Install faker to start \"pip3 install faker\"")
    exit(101)


def deploy_schema_postgres(conn: psycopg2._psycopg.connection, config: ConfigParser):
    fake = Faker(config.get('settings', 'language'))
    cur = conn.cursor()
    # create schema
    # control error here for uuid!
    execute_sql(cur, 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
    execute_sql(cur, "CREATE SCHEMA IF NOT EXISTS gn_app")
    if is_object(cur, "SELECT COUNT(rolname) FROM pg_roles WHERE rolname = 'gn_users'") == 0:
        execute_sql(cur, "CREATE ROLE gn_users")
        execute_sql(cur, "CREATE ROLE app_users")
    for user in config.get('settings', 'app_users').split(','):
        if is_object(cur,
                     "SELECT COUNT(rolname) FROM pg_roles WHERE rolcanlogin = True AND rolname = '{}'".format(
                         user)) == 0:
            execute_sql(cur, "CREATE USER {} LOGIN PASSWORD '{}'".format(user, config.get('settings',
                                                                                          'default_password')))
            execute_sql(cur, "GRANT app_users TO {}".format(user))
    for user in config.get('settings', 'db_admins').split(','):
        if is_object(cur,
                     "SELECT COUNT(rolname) FROM pg_roles WHERE rolcanlogin = True AND rolname = '{}'".format(
                         user)) == 0:
            execute_sql(cur, "CREATE USER {} LOGIN PASSWORD '{}'".format(user, config.get('settings',
                                                                                          'default_password')))
            execute_sql(cur, "GRANT gn_users TO {}".format(user))
    # create customers table
    sql = "CREATE TABLE IF NOT EXISTS gn_app.customers (" \
          "customer_id UUID DEFAULT uuid_generate_v4()," \
          "customer_fname varchar(50)," \
          "customer_lname varchar(50)," \
          "birthday date," \
          "citizen_id varchar(20)," \
          "birth_place varchar(50)," \
          "street varchar(50)," \
          "flat_number varchar(10)," \
          "city varchar(50)," \
          "zipcode varchar(10)," \
          "driving_license varchar(30)," \
          "passport_id varchar(30)," \
          "citizen_doc_id varchar(30)," \
          "mail varchar(50)," \
          "phone varchar(30))"
    execute_sql(cur, sql)
    execute_sql(cur, "ALTER TABLE gn_app.customers ALTER COLUMN customer_id SET NOT NULL")
    execute_sql(cur, "ALTER TABLE gn_app.customers ADD CONSTRAINT customers_pk PRIMARY KEY (customer_id)")
    execute_sql(cur, "CREATE TABLE IF NOT EXISTS gn_app.credit_cards (card_id UUID DEFAULT uuid_generate_v4(), "
                     "customer_id UUID REFERENCES gn_app.customers (customer_id), "
                     "card_number varchar(30), card_validity varchar(12))")
    execute_sql(cur, "ALTER TABLE gn_app.credit_cards ADD CONSTRAINT cc_pk PRIMARY KEY (card_id)")
    execute_sql(cur, "CREATE TABLE IF NOT EXISTS gn_app.features (feature_id UUID DEFAULT uuid_generate_v4(), "
                     "feature_name varchar(40), feature_price real)")
    execute_sql(cur, "ALTER TABLE gn_app.features ADD CONSTRAINT features_pk PRIMARY KEY (feature_id)")
    prices = config.get('game_addons', 'feature_prices').split(',')
    i = 0
    for feature in config.get('game_addons', 'feature_descriptions').split(','):
        execute_sql(cur, "INSERT INTO gn_app.features (feature_name, feature_price) VALUES ('{}', {})".format(
            feature, prices[i]))
        i += 1
    execute_sql(cur, "CREATE TABLE IF NOT EXISTS gn_app.extras (extra_id UUID DEFAULT uuid_generate_v4(), "
                     "extra_name varchar(40), extra_price real)")
    execute_sql(cur, "ALTER TABLE gn_app.extras ADD CONSTRAINT extras_pk PRIMARY KEY (extra_id)")
    prices = config.get('game_addons', 'extra_prices').split(',')
    i = 0
    for feature in config.get('game_addons', 'extra_descriptions').split(','):
        execute_sql(cur, "INSERT INTO gn_app.extras (extra_name, extra_price) VALUES ('{}', {})".format(
            feature, prices[i]))
        i += 1
    execute_sql(cur, "CREATE TABLE IF NOT EXISTS gn_app.transactions ("
                     "trans_id UUID DEFAULT uuid_generate_v4(), "
                     "feature_id UUID REFERENCES gn_app.features (feature_id), "
                     "extra_id UUID REFERENCES gn_app.extras (extra_id), "
                     "price real, "
                     "customer_id UUID REFERENCES gn_app.customers (customer_id), "
                     "card_id UUID REFERENCES gn_app.credit_cards (card_id), " 
                     "transaction_time TIMESTAMP DEFAULT now())"
                )
    execute_sql(cur, "GRANT USAGE ON SCHEMA gn_app TO gn_users")
    execute_sql(cur, "GRANT USAGE ON SCHEMA gn_app TO app_users")
    execute_sql(cur, "GRANT SELECT ON ALL TABLES IN SCHEMA gn_app TO gn_users")
    execute_sql(cur, "GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA gn_app TO app_users")
    if is_object(cur, "SELECT COUNT(*) FROM gn_app.customers") < config.getint('settings', 'minimal_customer_count'):
        for i in range(config.getint('settings', 'minimal_customer_count')):
            add_customer(config, cur)
    cur.close()


def cleanup_schema_postgres(conn: psycopg2._psycopg.connection, config: ConfigParser):
    cur = conn.cursor()
    for user in config.get('settings', 'app_users').split(','):
        if is_object(cur,
                     "SELECT COUNT(rolname) FROM pg_roles WHERE rolcanlogin = True AND rolname = '{}'".format(
                         user)) == 1:
            execute_sql(cur, "DROP USER {}".format(user))
    for user in config.get('settings', 'db_admins').split(','):
        if is_object(cur,
                     "SELECT COUNT(rolname) FROM pg_roles WHERE rolcanlogin = True AND rolname = '{}'".format(
                         user)) == 1:
            execute_sql(cur, "DROP USER {}".format(user))
    execute_sql(cur, "DROP SCHEMA IF EXISTS gn_app CASCADE")
    execute_sql(cur, "DROP ROLE IF EXISTS gn_users")
    execute_sql(cur, "DROP ROLE IF EXISTS app_users")
    cur.close()


