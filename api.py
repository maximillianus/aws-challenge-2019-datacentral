from flask import Flask, request, jsonify
from flask_restful import Resource, Api, reqparse
# from db_conn import get_all_data, make_postgres_conn, update_data, DB_CREDENTIALS
from db.db_sqlite import create_conn, retrieve_data, insert_data, truncate_table
from db.db_sqlite import DBNAME, TABLENAME
from jsonschema import validate

app = Flask(__name__)
api = Api(app)

# parser = reqparse.RequestParser()
# parser.add_argument('cameraID')
# parser.add_argument('longitude')
# parser.add_argument('latitude')
# parser.add_arguments('road_condition')
# parser.add_arguments('severity')

payload_schema = {
    'type': 'object',
    'properties': {
        'camera_id': {'type': 'number'},
        'longitude': {'type': 'number'},
        'latitude': {'type': 'number'},
        'road_condition': {'type': 'string'},
        'severity': {'type': 'string'},
    }
}


class Landing(Resource):
    def get(self):
        return 'Welcome to Centralized Road Data'

class RoadDataShow(Resource):
    def get(self):
        """
        Endpoints to get all data
        """
        # jsonarray = [
        #     {
        #         "camera_id": 1,
        #         "longitude": 110.81861,
        #         "latitude": -7.60111,
        #         "road_condition": "holes",
        #         "confidence_score": 81.0
        #     },
        #     {
        #         "camera_id": 2,
        #         "longitude": 111.81861,
        #         "latitude": -7.60111,
        #         "road_condition": "holes",
        #         "confidence_score": 31.0
        #     }
        # ]

        # DB Connection by POSTGRES
        # conn, cur = make_postgres_conn(DB_CREDENTIALS['local']['HOSTNAME'], 
        #                 DB_CREDENTIALS['local']['USERNAME'], 
        #                 DB_CREDENTIALS['local']['PASSWORD'], 
        #                 DB_CREDENTIALS['local']['DBNAME']
        #                 )
        
        # DB Connection by SQLITE3
        conn, cur = create_conn(dbname=DBNAME)
        res = retrieve_data(tablename=TABLENAME, 
                            cur=cur,
                            conn=conn
                            )
        for r in res:
            r['timestamp'] = str(r['timestamp'])
        
        conn.close()
        return res, 200

class RoadData(Resource):
    def post(self):
        """
        payload_schema:
        cameraID : int
        longitude: float
        latitude: float
        road_condition: string - hole, damaged road, uneven bumps
        confidence_score: number - 0 - 100
        """
        jsondata = request.get_json(force=True)
        validate(instance=jsondata, schema=payload_schema)
        print('data', jsondata)

        # DB Connection by POSTGRES
        # conn, cur = make_postgres_conn(DB_CREDENTIALS['local']['HOSTNAME'], 
        #                         DB_CREDENTIALS['local']['USERNAME'], 
        #                         DB_CREDENTIALS['local']['PASSWORD'], 
        #                         DB_CREDENTIALS['local']['DBNAME']
        #                         )
        # DB Connection by SQLITE3
        conn, cur = create_conn(DBNAME)

        # Store in database
        if jsondata['road_condition'] not in ['hole', 'holes']:
            print('Hole undetected. Pass!')
            jsondata['status'] = 'NOT INPUTTED'
            pass
        else:
            print('Hole detected. Store!')
            insert_data(tablename=TABLENAME, 
                        jsondata=jsondata, 
                        cur=cur, 
                        conn=conn)
            jsondata['status'] = 'INPUTTED'

        conn.close()
        return jsondata, 200

class RoadDataTruncate(Resource):
    def get(self):
        """
        Endpoint to truncate and clean all data
        """
        # DB Connection by SQLITE3
        conn, cur = create_conn(dbname=DBNAME)

        truncate_table(tablename=TABLENAME, 
                       cur=cur,
                       conn=conn
                      )
        res = {
            'status': 'Table truncated!'
        }

        return res, 200

class HealthCheck(Resource):
    def get(self):
        return {'health': 'OK'}, 200

api.add_resource(Landing, '/')
api.add_resource(RoadData, '/roaddata_input')
api.add_resource(RoadDataTruncate, '/truncate_data')
api.add_resource(RoadDataShow, '/roaddata')
api.add_resource(HealthCheck, '/health')


if __name__ == '__main__':
    app.run(debug=True, port=5050)