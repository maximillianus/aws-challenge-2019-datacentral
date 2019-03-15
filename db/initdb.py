import db_sqlite

DBNAME = 'AWS.db'
TABLE = 'road_data'

# Create connection
conn, cur = db_sqlite.create_conn(DBNAME)

# create table
# db_sqlite.create_table(tablename=TABLE, cur=cur, conn=conn)

# truncate table
# db_sqlite.truncate_table(tablename=TABLE, cur=cur, conn=conn)

# select from table
data = db_sqlite.retrieve_data(tablename=TABLE, 
                               cur=cur, 
                               conn=conn, 
                               query=''
                              )
print(data)

# insert data
jsondata = {
    "camera_id": 1,
    "longitude": 110.81861,
    "latitude": -7.60111,
    "road_condition": "holes",
    "confidence_score": 81.0
}
# db_sqlite.insert_data(tablename=TABLE, 
#                       jsondata=jsondata, 
#                       cur=cur, 
#                       conn=conn
#                       )