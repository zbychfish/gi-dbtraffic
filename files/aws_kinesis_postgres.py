import datetime
import time
import random
import string
from configparser import ConfigParser

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


def connect_to_database(config: ConfigParser, user: str, password: str) -> [psycopg2._psycopg.connection, str]:
    try:
        conn = psycopg2.connect("host={} port={} dbname={} user={} password={}".format(
            config.get('db', 'host'),
            config.get('db', 'port'),
            config.get('db', 'database'),
            user,
            password
        ))
        conn.autocommit = True
        error = "OK"
    except Exception as err:
        conn = None
        error = err
    return [conn, error]


def is_object(cursor: psycopg2._psycopg.cursor, sql: str) -> int:
    cursor.execute(sql)
    return cursor.fetchone()[0]


def execute_sql(cur: psycopg2._psycopg.cursor, sql: str):
    cur.execute(sql)


def generate_date_in_range(config: ConfigParser) -> datetime.datetime:
    birth_start = config.get('settings', 'birth_start').split(',')
    birth_end = config.get('settings', 'birth_end').split(',')
    start = datetime.datetime(int(birth_start[0]), int(birth_start[1]), int(birth_start[2]))
    end = datetime.datetime(int(birth_end[0]), int(birth_end[1]), int(birth_end[2]))
    return start + datetime.timedelta(seconds=random.randint(0, int((end - start).total_seconds())))


def generate_citizen_id(config: ConfigParser, date: datetime.datetime, sex: int, f: Faker) -> str:
    if config.get('settings', 'language') == 'pl_PL':
        if date.date().year < 1999:
            pesel = date.date().strftime('%y%m%d')
        else:
            pesel = date.date().strftime('%y%m%d')[:2] \
                    + str(int(date.date().strftime('%y%m%d')[2:4]) + 20) + date.date().strftime('%y%m%d')[4:]
        pesel = pesel + ''.join(random.choice(string.digits) for _ in range(3))
        if sex == 0:
            pesel = pesel + random.choice(['0', '2', '4', '6', '8'])
        else:
            pesel = pesel + random.choice(['1', '3', '5', '7', '9'])
        vsum = 0
        position = 0
        for factor in [1, 3, 7, 9, 1, 3, 7, 9, 1, 3]:
            vsum += int(pesel[position:position + 1]) * factor
            position += 1
        sum_check = 10 - vsum % 10 if vsum % 10 != 0 else 0
        return pesel + str(sum_check)
    elif config.get('settings', 'language') == 'en_US':
        return f.ssn()
    else:
        return ''


def leading_zeros(value: str, length: int) -> str:
    value = ('0000000000' + str(value))
    return value[value.__len__() - length:]


def generate_driver_license(config: ConfigParser) -> str:
    if config.get('settings', 'language') == 'pl_PL':
        counties_codes = [[2, 26], [4, 19], [6, 20], [8, 12], [10, 21], [12, 19], [14, 38], [16, 11], [18, 21],
                          [20, 14], [22, 16], [24, 17], [26, 13], [28, 19], [30, 31], [32, 18]]
        cities_codes = [[2, 65], [4, 64], [6, 64], [8, 62], [10, 63], [12, 63], [14, 65], [16, 61], [18, 64], [20, 63],
                        [22, 64], [24, 78], [26, 61], [28, 62], [30, 64], [32, 63]]
        # generates new license format starting from 2002
        dl = leading_zeros(str(random.randint(1, 99999)), 5)
        dl += "/"
        dl += leading_zeros(str(random.randint(2, int(str(datetime.datetime.now().year)[2:]))), 2) + '/'
        if random.choice([0, 1]):
            county = random.choice(counties_codes)
            dl += leading_zeros(str(county[0]), 2)
            dl += leading_zeros(str(random.randint(1, county[1])), 2)
        else:
            county = random.choice(cities_codes)
            dl += leading_zeros(str(county[0]), 2)
            dl += leading_zeros(str(random.randint(61, county[1])), 2)
        return dl
    else:
        return ''


def generate_passport_id(config) -> str:
    if config.get('settings', 'language') == 'pl_PL':
        #  wag_full = [7, 3, 9, 1, 7, 3, 1, 7, 3]
        wag_gen = [7, 3, 9, 7, 3, 1, 7, 3]
        symbols = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'P', 'R', 'S', 'T', 'U', 'V',
                   'W', 'X', 'Y', 'Z']
        d_values = []
        dl = ''
        for i in range(0, 2):
            value = random.choice(symbols)
            dl += value
            d_values.append(ord(value) - 55)
        value = leading_zeros(str(random.randint(0, 999999)), 6)
        for digit in list(value):
            d_values.append(int(digit))
        vsum = 0
        for i in range(0, 8):
            vsum += d_values[i] * wag_gen[i]
        dl += value[:1]
        dl += str(10 - vsum % 10) if vsum % 10 != 0 else '0'
        return dl + value[1:]
    else:
        return ''


def generate_citizen_document_id(config: ConfigParser) -> str:
    if config.get('settings', 'language'):
        #  wag_full = [7, 3, 1, 9, 7, 3, 1, 7, 3]
        wag_gen = [7, 3, 1, 7, 3, 1, 7, 3]
        symbols = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'P', 'R', 'S', 'T', 'U', 'V',
                   'W', 'X', 'Y', 'Z']
        d_values = []
        dl = ''
        for i in range(0, 3):
            value = random.choice(symbols)
            dl += value
            d_values.append(ord(value) - 55)
        value = leading_zeros(str(random.randint(0, 99999)), 5)
        for digit in list(value):
            d_values.append(int(digit))
        vsum = 0
        for i in range(0, 8):
            vsum += d_values[i] * wag_gen[i]
        return dl + str(vsum % 10) + value
    else:
        return ''


def remove_accents(input_text: str) -> str:
    strange = 'ŮôῡΒძěἊἦëĐᾇόἶἧзвŅῑἼźἓŉἐÿἈΌἢὶЁϋυŕŽŎŃğûλВὦėἜŤŨîᾪĝžἙâᾣÚκὔჯᾏᾢĠфĞὝŲŊŁČῐЙῤŌὭŏყἀхῦЧĎὍОуνἱῺèᾒῘᾘὨШūლἚύсÁóĒἍŷö' \
              'ὄЗὤἥბĔõὅῥŋБщἝξĢюᾫაπჟῸდΓÕűřἅгἰშΨńģὌΥÒᾬÏἴქὀῖὣᾙῶŠὟὁἵÖἕΕῨčᾈķЭτἻůᾕἫжΩᾶŇᾁἣჩαἄἹΖеУŹἃἠᾞåᾄГΠКíōĪὮϊὂᾱიżŦИὙἮὖÛĮ' \
              'ἳφᾖἋΎΰῩŚἷРῈĲἁéὃσňİΙῠΚĸὛΪᾝᾯψÄᾭêὠÀღЫĩĈμΆᾌἨÑἑïოĵÃŒŸζჭᾼőΣŻçųøΤΑËņĭῙŘАдὗპŰἤცᾓήἯΐÎეὊὼΘЖᾜὢĚἩħĂыῳὧďТΗἺĬὰὡὬὫÇ' \
              'ЩᾧñῢĻᾅÆßшδòÂчῌᾃΉᾑΦÍīМƒÜἒĴἿťᾴĶÊΊȘῃΟúχΔὋŴćŔῴῆЦЮΝΛῪŢὯнῬũãáἽĕᾗნᾳἆᾥйᾡὒსᾎĆрĀüСὕÅýფᾺῲšŵкἎἇὑЛვёἂΏθĘэᾋΧĉᾐĤὐὴι' \
              'ăąäὺÈФĺῇἘſგŜæῼῄĊἏØÉПяწДĿᾮἭĜХῂᾦωთĦлðὩზკίᾂᾆἪпἸиᾠώᾀŪāоÙἉἾρаđἌΞļÔβĖÝᾔĨНŀęᾤÓцЕĽŞὈÞუтΈέıàᾍἛśìŶŬȚĳῧῊᾟάεŖᾨᾉς' \
              'ΡმᾊᾸįᾚὥηᾛġÐὓłγľмþᾹἲἔбċῗჰხοἬŗŐἡὲῷῚΫŭᾩὸùᾷĹēრЯĄὉὪῒᾲΜᾰÌœĥტ'
    ascii_replacements = 'UoyBdeAieDaoiiZVNiIzeneyAOiiEyyrZONgulVoeETUiOgzEaoUkyjAoGFGYUNLCiIrOOoqaKyCDOOUniOeiII' \
                         'OSulEySAoEAyooZoibEoornBSEkGYOapzOdGOuraGisPngOYOOIikoioIoSYoiOeEYcAkEtIuiIZOaNaicaaIZE' \
                         'UZaiIaaGPKioIOioaizTIYIyUIifiAYyYSiREIaeosnIIyKkYIIOpAOeoAgYiCmAAINeiojAOYzcAoSZcuoTAEn' \
                         'iIRADypUitiiIiIeOoTZIoEIhAYoodTIIIaoOOCSonyKaAsSdoACIaIiFIiMfUeJItaKEISiOuxDOWcRoiTYNLY' \
                         'TONRuaaIeinaaoIoysACRAuSyAypAoswKAayLvEaOtEEAXciHyiiaaayEFliEsgSaOiCAOEPYtDKOIGKiootHLd' \
                         'OzkiaaIPIIooaUaOUAIrAdAKlObEYiINleoOTEKSOTuTEeiaAEsiYUTiyIIaeROAsRmAAiIoiIgDylglMtAieBc' \
                         'ihkoIrOieoIYuOouaKerYAOOiaMaIoht'
    translator = str.maketrans(strange, ascii_replacements)
    return input_text.translate(translator)


def generate_mail(config: ConfigParser, first: str, last: str) -> str:
    first = remove_accents(first.lower())
    last = remove_accents(last.lower())
    if random.choice([True, False]):
        mail = first + '.' + last + '@'
    elif random.choice([True, False]):
        mail = first[:1] + last + '@'
    else:
        mail = first[:1] + '.' + last + '@'
    global_domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'mail.com', 'lycos.com', 'live.com']
    if config.get('settings', 'language') == 'pl_PL':
        polish_domains = ['wp.pl', 'poczta.onet.pl', 'o2.pl', 'interia.pl', 'op.pl', 'tlen.pl', 'poczta.fm',
                          'gazeta.pl', 'vp.pl', 'aol.pl', 'gery.pl']
        global_domains.extend(polish_domains)
    else:
        pass
    return mail + random.choice(global_domains)


def generate_wired_phone_number(lang: str) -> str:
    if lang == 'pl_PL':
        ranges = [[12, 18], [22, 25], [29, 29], [32, 34], [41, 44], [46, 46], [48, 48], [52, 52], [54, 56], [58, 59],
                  [61, 63], [65, 65], [67, 68], [71, 71], [74, 77], [81, 87], [89, 89], [91, 91], [94, 95]]
        zone = random.choice(ranges)
        return str(random.randint(zone[0], zone[1])) + leading_zeros(str(random.randint(0, 9999999)), 7)
    else:
        return ''


def generate_mobile_phone_number(lang: str) -> str:
    if lang == 'pl_PL':
        prefixes = ['45', '50', '51', '53', '57', '60', '66', '69', '72', '73', '78', '79', '88']
        return random.choice(prefixes) + leading_zeros(str(random.randint(0, 9999999)), 7)
    else:
        return ''


def generate_phone_number(config: ConfigParser, f: Faker) -> str:
    phone = generate_mobile_phone_number(config.get('settings', 'language')) if random.choice([True, False]) \
        else generate_wired_phone_number(config.get('settings', 'language'))
    if config.get('settings', 'language') == 'pl_PL':
        if random.choice([True, False]):
            if random.choice([True, False]):
                phone = generate_mobile_phone_number(config.get('settings', 'language'))
                phone = phone[:3] + '-' + phone[3:6] + '-' + phone[6:9]
            else:
                phone = generate_wired_phone_number(config.get('settings', 'language'))
                phone = '(' + phone[:2] + ')' + phone[2:5] + '-' + phone[5:7] + '-' + phone[7:9]
            return phone if random.choice([True, False]) else '+48' + phone
        else:
            return phone if random.choice([True, False]) else '+48' + phone
    else:
        return f.phone_number()


def deploy_schema(conn: psycopg2._psycopg.connection, config: ConfigParser):
    fake = Faker(config.get('settings', 'language'))
    cur = conn.cursor()
    # create schema
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
    execute_sql(cur, "GRANT USAGE ON SCHEMA gn_app TO gn_users")
    execute_sql(cur, "GRANT USAGE ON SCHEMA gn_app TO app_users")
    execute_sql(cur, "GRANT SELECT ON ALL TABLES IN SCHEMA gn_app TO gn_users")
    execute_sql(cur, "GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA gn_app TO app_users")
    if is_object(cur, "SELECT COUNT(*) FROM gn_app.customers") < config.getint('settings', 'minimal_customer_count'):
        for i in range(config.getint('settings', 'minimal_customer_count')):
            add_customer(config, cur)
            # sex = random.randint(0, 2)
            # birth_date = generate_date_in_range(config)
            # first_name = fake.first_name() if sex == 1 else fake.first_name_female()
            # last_name = fake.last_name() if sex == 1 else fake.last_name_female()
            # sql = "INSERT INTO {reference}.customers (" \
            #       "customer_fname," \
            #       "customer_lname," \
            #       "birthday," \
            #       "citizen_id," \
            #       "birth_place," \
            #       "street," \
            #       "flat_number," \
            #       "city," \
            #       "zipcode," \
            #       "driving_license," \
            #       "passport_id," \
            #       "citizen_doc_id," \
            #       "mail," \
            #       "phone)" \
            #       " VALUES('{fn}','{ln}',{bd},'{ci}','{bc}','{sn}','{fl}','{ct}','{zc}'," \
            #       "'{dl}','{ps}','{do}','{ma}'," \
            #       "'{pn}')".format(
            #     reference='gn_app',
            #     fn=first_name,
            #     ln=last_name,
            #     bd="TO_DATE('{}','yyyy-mm-dd')".format(birth_date.date()),
            #     # if dbtype == 'Oracle' else "'" + str(bdate.date()) + "'",
            #     ci=generate_citizen_id(config, birth_date, sex, fake),
            #     bc=fake.city(),
            #     sn=fake.street_name(),
            #     fl=fake.numerify(text="###"),
            #     ct=fake.city(),
            #     zc=fake.postcode(),
            #     dl=generate_driver_license(config),
            #     ps=generate_passport_id(config),
            #     do=generate_citizen_document_id(config),
            #     ma=generate_mail(config, first_name, last_name),
            #     pn=generate_phone_number(config, fake))
            # print(sql)
            # execute_sql(cur, sql)
    cur.close()


def cleanup_schema(conn: psycopg2._psycopg.connection, config: ConfigParser):
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
    execute_sql(cur, "DROP TABLE IF EXISTS gn_app.customers CASCADE")
    execute_sql(cur, "DROP SCHEMA IF EXISTS gn_app CASCADE")
    execute_sql(cur, "DROP ROLE IF EXISTS gn_users")
    execute_sql(cur, "DROP ROLE IF EXISTS app_users")

    cur.close()


def set_activity_defaults(conn: psycopg2._psycopg.connection) -> [int]:
    cur = conn.cursor()
    execute_sql(cur, 'SELECT COUNT(*) from gn_app.customers')
    customers_number = cur.fetchone()[0] - 1
    return [customers_number]


def is_time_reached(end_time, check_time=None):
    check_time = datetime.datetime.now()
    return True if check_time < end_time else False


def add_customer(config: ConfigParser, cur: psycopg2._psycopg.cursor):
    fake = Faker(config.get('settings', 'language'))
    sex = random.randint(0, 2)
    birth_date = generate_date_in_range(config)
    first_name = fake.first_name() if sex == 1 else fake.first_name_female()
    last_name = fake.last_name() if sex == 1 else fake.last_name_female()
    sql = "INSERT INTO {reference}.customers (" \
          "customer_fname," \
          "customer_lname," \
          "birthday," \
          "citizen_id," \
          "birth_place," \
          "street," \
          "flat_number," \
          "city," \
          "zipcode," \
          "driving_license," \
          "passport_id," \
          "citizen_doc_id," \
          "mail," \
          "phone)" \
          " VALUES('{fn}','{ln}',{bd},'{ci}','{bc}','{sn}','{fl}','{ct}','{zc}'," \
          "'{dl}','{ps}','{do}','{ma}'," \
          "'{pn}')".format(
        reference='gn_app',
        fn=first_name,
        ln=last_name,
        bd="TO_DATE('{}','yyyy-mm-dd')".format(birth_date.date()),
        # if dbtype == 'Oracle' else "'" + str(bdate.date()) + "'",
        ci=generate_citizen_id(config, birth_date, sex, fake),
        bc=fake.city(),
        sn=fake.street_name(),
        fl=fake.numerify(text="###"),
        ct=fake.city(),
        zc=fake.postcode(),
        dl=generate_driver_license(config),
        ps=generate_passport_id(config),
        do=generate_citizen_document_id(config),
        ma=generate_mail(config, first_name, last_name),
        pn=generate_phone_number(config, fake))
    cur.execute(sql)


def application_traffic(config: ConfigParser, defaults: [int], end_time: datetime.datetime):
    customer_number = defaults[0]
    task_timeout = 5 if config.get('settings', 'processing_speed') == 'slow' \
        else 1 if config.get('settings', 'processing_speed') == 'normal' else 0
    tasks_list = ['get_customer_info', 'add_customer']
    info_types = ['name_surname', 'email', 'users_from_city', 'credit_card']
    while is_time_reached(end_time):
        session_steps_number = random.randint(1, int(config.get('settings', 'maximum_steps_in_session')))
        app_session_user = random.randint(0, len(config.get('settings', 'app_users').split(',')) - 1)
        print("Switch context to user {} for {} tasks".format(app_session_user, session_steps_number))
        app_conn = connect_to_database(config, config.get('settings', 'app_users').split(',')[app_session_user],
                                   config.get('settings', 'default_password'))
        for i in range(0, session_steps_number):
            session_task = random.choices(tasks_list, weights=(0.95, 0.05), k=1)
            if session_task[0] == 'get_customer_info':
                print('get_customer ')
                app_cursor = app_conn[0].cursor()
                app_cursor.execute("SELECT customer_id FROM gn_app.customers LIMIT 1 OFFSET {}".
                                   format(random.randint(0, customer_number)))
                get_info_type = random.choices(info_types, weights=(0.5, 0.3, 0.2, 0))
                if get_info_type[0] == 'name_surname':
                    app_cursor.execute("SELECT customer_fname, customer_lname, city, zipcode, street FROM "
                                       "gn_app.customers WHERE customer_id='{}'".format(app_cursor.fetchone()[0]))
                elif get_info_type[0] == 'email':
                    app_cursor.execute("SELECT customer_fname, customer_lname, mail FROM "
                                       "gn_app.customers WHERE customer_id='{}'".format(app_cursor.fetchone()[0]))
                    if random.choice([True, False]):
                        app_cursor.execute("SELECT COUNT(mail) FROM gn_app.customers WHERE mail='{}'"
                                           .format(app_cursor.fetchone()[2]))
                elif get_info_type[0] == 'users_from_city':
                    app_cursor.execute("SELECT city, street FROM "
                                       "gn_app.customers WHERE customer_id='{}'".format(app_cursor.fetchone()[0]))
                    result_set = app_cursor.fetchone()
                    app_cursor.execute("SELECT COUNT(*) FROM gn_app.customers WHERE city='{}' AND street='{}'"
                                       .format(result_set[0], result_set[1]))
                elif get_info_type[0] == 'credit_card':
                    pass
                app_cursor.close()
                time.sleep(task_timeout)
            elif session_task[0] == 'add_customer':
                print('add_customer')
                app_cursor = app_conn[0].cursor()
                add_customer(config, app_cursor)
                customer_number += 1
                app_cursor.close()
                time.sleep(task_timeout)
        app_conn[0].close()
