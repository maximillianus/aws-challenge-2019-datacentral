import psycopg2
from datetime import datetime
from pprint import pprint
import json

def make_postgres_conn(host, username, pwd, dbname):
    try:
        conn = psycopg2.connect(host=host,
                                dbname=dbname,
                                user=username,
                                password=pwd)
    except psycopg2.Error as e:
        print('Connection Issue')
        print(e)
        exit()
    else:
        print('Database conn successful')
        cur = conn.cursor()
        return conn, cur

def get_all_data(conn, cur):
    query = """
    SELECT * FROM road_data
    """
    cur.execute(query)
    columns = ('timestamp', 'camera_id', 'longitude', 'latitude',
               'road_condition', 'confidence_score', 'severity')
    results = []
    for row in cur.fetchall():
        results.append(dict(zip(columns, row)))
    
    # df = pd.io.sql.read_sql_query(query, conn)
    return results


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

def truncate_data(conn, cur, table='road_data'):
    query = """
    TRUNCATE TABLE %s
    """ % (table)
    cur.execute(query)
    conn.commit()

## PostgreSQL creds
DB_CREDENTIALS = {
    'local': {
        'HOSTNAME' : '127.0.0.1',
        'DBNAME' : 'testdb',
        'USERNAME' : 'adityap',
        'PASSWORD' : None
    }
}


def main():
    conn, cur = make_postgres_conn(DB_CREDENTIALS['local']['HOSTNAME'], 
                                DB_CREDENTIALS['local']['USERNAME'], 
                                DB_CREDENTIALS['local']['PASSWORD'], 
                                DB_CREDENTIALS['local']['DBNAME']
                                )

    # df = get_data(conn)
    # print(df)

    # jsondata = {
    #     "camera_id": 1,
    #     "longitude": 110.81861,
    #     "latitude": -7.60111,
    #     "road_condition": "holes",
    #     "confidence_score": 81.0
    # }
    # update_data(conn, cur, jsondata)

    res = get_all_data(conn, cur)
    # print(json.dumps(res))
    pprint(res)

    conn.close()

if __name__ == "__main__":
    main()