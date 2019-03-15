import sqlite3
from datetime import datetime

DBNAME = 'AWS.db'
TABLENAME = 'road_data'

def create_conn(dbname):
    try:
        conn = sqlite3.connect(dbname)
        cur = conn.cursor()
        return conn, cur
    except sqlite3.Error as e:
        print('SQLite3 Error connecting to db: %s' % e)
        return False, False
    except Exception as e:
        print('Other Error connecting to db: %s' % e)
        return False, False

def create_table(tablename, cur, conn):
    """
    columns = ('timestamp', 'camera_id', 'longitude', 'latitude',
            'road_condition', 'confidence_score', 'severity')
    """
    query = """
    CREATE TABLE {table}
    (timestamp datetime, camera_id int, longitude real, latitude real, 
    road_condition text, confidence_score real, severity text)
    """
    try:
        cur.execute(query.format(table=tablename))
        conn.commit()
        print('Success creating table!')
    except sqlite3.Error as e:
        print('Error during create table: %s' % e)

def insert_data(tablename, jsondata, cur, conn):
    """
    tablename: string
    data: single json/dict object
    """
    assert isinstance(tablename, str), "tablename is not string!"
    assert isinstance(jsondata, dict), "data is not dict!"
    print('JSON data:\n', jsondata)

    camera_id = jsondata['camera_id']
    longitude = jsondata['longitude']
    latitude = jsondata['latitude']
    road_condition = jsondata['road_condition']
    confidence_score = jsondata['confidence_score']

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(timestamp)
    if confidence_score <= 50.0:
        severity = 'LIGHT'
    elif confidence_score > 50.0 and confidence_score <= 80.0:
        severity = 'MEDIUM'
    elif confidence_score > 80.0:
        severity = 'HIGH'

    query = """
    INSERT INTO {table} VALUES (?, ?, ?, ?, ?)
    """

    query = """
    INSERT INTO {table} (timestamp, camera_id, longitude, latitude, road_condition, confidence_score, severity) 
    VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')
    """ % (timestamp, camera_id, longitude, latitude, road_condition, confidence_score, severity)

    try:
        cur.execute(query.format(table=tablename), jsondata)
        conn.commit()
        print('Success inserting data!')
    except sqlite3.Error as e:
        print('Error during insert: %s' % e)

def update_data(conn, cur, jsondata):
    camera_id = jsondata['camera_id']
    longitude = jsondata['longitude']
    latitude = jsondata['latitude']
    road_condition = jsondata['road_condition']
    confidence_score = jsondata['confidence_score']
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(timestamp)
    if confidence_score <= 50.0:
        severity = 'LIGHT'
    elif confidence_score > 50.0 and confidence_score <= 80.0:
        severity = 'MEDIUM'
    elif confidence_score > 80.0:
        severity = 'HIGH'
    
    query = """
    INSERT INTO road_data (timestamp, camera_id, longitude, latitude, road_condition, confidence_score, severity) 
    VALUES (TIMESTAMP '%s', '%s', '%s', '%s', '%s', '%s', '%s')
    """ % (timestamp, camera_id, longitude, latitude, road_condition, confidence_score, severity)
    
    cur.execute(query)
    conn.commit()

def retrieve_data(tablename, cur, conn, query = ''):
    if query == '':
        query = """
        SELECT * FROM {table}
        """
    try:
        cur.execute(query.format(table=tablename))
    except sqlite3.Error as e:
        print('Error retrieving data: %s' % e)
    
    columns = ('timestamp', 'camera_id', 'longitude', 'latitude',
               'road_condition', 'confidence_score', 'severity')

    results = []
    for row in cur.fetchall():
        results.append(dict(zip(columns, row)))
    
    return results

def truncate_table(tablename, cur, conn):
    query = """
    DELETE FROM {table}
    """
    try:
        print(query.format(table=tablename))
        cur.execute(query.format(table=tablename))
        conn.commit()
        print('Success truncating table!')
    except sqlite3.Error as e:
        print('Error during truncating table: %s' % e)
