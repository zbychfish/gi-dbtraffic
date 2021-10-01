import configparser
from files.definitions import execute_sql, is_object, add_customer
try:
    import mysql.connector as cn
except ImportError:
    print("Install mysql connector to start \"pip3 install mysql.connector\"")
    #print("This module can require OS packages: postgres, postgres-devel, gcc, python3-devel")
    exit(101)
try:
    from faker import Faker
except ImportError:
    print("Install faker to start \"pip3 install faker\"")
    exit(101)


def cleanup_schema_mysql(conn: cn.connection, config: configparser.ConfigParser):
    cur = conn.cursor()
    for user in config.get('settings', 'app_users').split(','):
        if is_object(cur, "SELECT COUNT(*) FROM mysql.user WHERE user='{}' AND host='%'".format(user)) == 1:
            execute_sql(cur, "DROP USER {}".format(user))
    for user in config.get('settings', 'db_admins').split(','):
        if is_object(cur, "SELECT COUNT(*) FROM mysql.user WHERE user='{}' AND host='%'".format(user)) == 1:
            execute_sql(cur, "DROP USER {}".format(user))
    for table in ['customers', 'credit_cards', 'features', 'extras', 'transactions']:
        execute_sql(cur, "DROP TABLE IF EXISTS gn_app.{} CASCADE".format(table))
    execute_sql(cur, "DROP ROLE IF EXISTS gn_users")
    execute_sql(cur, "DROP ROLE IF EXISTS app_users")
    cur.close()


def deploy_schema_mysql(conn: cn.connection, config: configparser.ConfigParser):
    fake = Faker(config.get('settings', 'language'))
    cur = conn.cursor()
    # create schema
    execute_sql(cur, "CREATE ROLE IF NOT EXISTS gn_users")
    execute_sql(cur, "CREATE ROLE IF NOT EXISTS app_users")
    for user in config.get('settings', 'app_users').split(','):
        execute_sql(cur, "CREATE USER IF NOT EXISTS {}@'%' IDENTIFIED BY '{}' DEFAULT ROLE app_users"
                    .format(user, config.get('settings', 'default_password')))
        execute_sql(cur, "GRANT app_users TO {}".format(user))
    for user in config.get('settings', 'db_admins').split(','):
        execute_sql(cur, "CREATE USER IF NOT EXISTS {}@'%' IDENTIFIED BY '{}' DEFAULT ROLE gn_users"
                    .format(user, config.get('settings', 'default_password')))
        execute_sql(cur, "GRANT gn_users TO {}".format(user))
    # create customers table
    sql = "CREATE TABLE IF NOT EXISTS gn_app.customers (" \
          "customer_id varchar(40) DEFAULT (uuid())," \
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
          "phone varchar(30), " \
          "PRIMARY KEY (customer_id))"
    execute_sql(cur, sql)
    execute_sql(cur, "CREATE TABLE IF NOT EXISTS gn_app.credit_cards (card_id varchar(40) DEFAULT (uuid()), "
                     "customer_id varchar(40) REFERENCES gn_app.customers (customer_id), "
                     "card_number varchar(30), card_validity varchar(12), PRIMARY KEY (card_id))")
    execute_sql(cur, "CREATE TABLE IF NOT EXISTS gn_app.features (feature_id varchar(40) DEFAULT (uuid()), "
                      "feature_name varchar(40), feature_price real, PRIMARY KEY (feature_id))")
    prices = config.get('game_addons', 'feature_prices').split(',')
    i = 0
    for feature in config.get('game_addons', 'feature_descriptions').split(','):
        execute_sql(cur, "INSERT INTO gn_app.features (feature_name, feature_price) VALUES ('{}', {})".format(
            feature, prices[i]))
        i += 1
    execute_sql(cur, "CREATE TABLE IF NOT EXISTS gn_app.extras (extra_id varchar(40) DEFAULT (uuid()), "
                     "extra_name varchar(40), extra_price real, PRIMARY KEY (extra_id))")
    prices = config.get('game_addons', 'extra_prices').split(',')
    i = 0
    for feature in config.get('game_addons', 'extra_descriptions').split(','):
        execute_sql(cur, "INSERT INTO gn_app.extras (extra_name, extra_price) VALUES ('{}', {})".format(
            feature, prices[i]))
        i += 1
    execute_sql(cur, "CREATE TABLE IF NOT EXISTS gn_app.transactions ("
                     "trans_id varchar(40) DEFAULT (uuid()), "
                     "feature_id varchar(40) REFERENCES gn_app.features (feature_id), "
                     "extra_id varchar(40) REFERENCES gn_app.extras (extra_id), "
                     "price real, "
                     "customer_id varchar(40) REFERENCES gn_app.customers (customer_id), "
                     "card_id varchar(40) REFERENCES gn_app.credit_cards (card_id), "
                     "transaction_time TIMESTAMP DEFAULT now(), "
                     "PRIMARY KEY (trans_id))"
                )
    execute_sql(cur, "GRANT SELECT ON gn_app.* TO gn_users")
    execute_sql(cur, "GRANT SELECT, INSERT, UPDATE ON gn_app.* TO app_users")
    if is_object(cur, "SELECT COUNT(*) FROM gn_app.customers") < config.getint('settings', 'minimal_customer_count'):
        for i in range(config.getint('settings', 'minimal_customer_count')):
            add_customer(config, cur)
    cur.close()