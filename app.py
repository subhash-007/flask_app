from flask import Flask, request,jsonify, make_response
from datetime import datetime
import pandas as pd
import pyarrow as pa
import redis


redis_conn = redis.StrictRedis(host='localhost', port=6379, db=0)
app = Flask(__name__)
context = pa.default_serialization_context()


@app.route('/load_data')
def load_data_into_redis():
    df = pd.read_csv('/home/subhash/Downloads/raw_data.csv').sort_values(ascending=False, by=['sts'])
    print(df)
    if df.any:
        
        device_id = df.device_fk_id.unique().tolist()
        for device in device_id:
            device_data = df.loc[df['device_fk_id'] == device]
            redis_conn.hmset('latest_info_'+str(device),device_data.iloc[0].to_dict())
            redis_conn.set('start_location_'+str(device),str((device_data['latitude'].iloc[0],device_data['longitude'].iloc[0])))
            redis_conn.set('end_location_'+str(device),str((device_data['latitude'].iloc[-1],device_data['longitude'].iloc[-1])))
            redis_conn.set("device_"+str(device), context.serialize(device_data).to_buffer().to_pybytes())            
        return 'True'
    else:
        return 'False'


@app.route('/location_detail')
def location_detail():
    device_args = request.args.get('device_id')
    if redis_conn.exists("start_location_"+str(device_args)) and redis_conn.exists("end_location_"+str(device_args)):
        start_location = redis_conn.get("start_location_"+str(device_args)).decode("utf-8") 
        end_location = redis_conn.get("end_location_"+str(device_args)).decode("utf-8") 
        data = {
            "start_loaction":  start_location,
            "end_location": end_location
        }
        return make_response(jsonify(data), 200)
    else:
        return make_response(jsonify('No data available in redis',403))
        
@app.route('/latest_info')
def latest_info():
    device_args = request.args.get('device_id')
    
    if redis_conn.exists("latest_info_"+str(device_args)):
        latest_info = redis_conn.hgetall("latest_info_"+str(device_args))
        latest_info = { y.decode('ascii'): latest_info.get(y).decode('ascii') for y in latest_info.keys() }
        return make_response(jsonify(latest_info), 200)
    else:
        return make_response(jsonify('No data available in redis',403))

@app.route('/details_data/')
def details_data():
    device_args = request.args.get('device_id')
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')
    if redis_conn.exists("device_"+str(device_args)):
        df = context.deserialize(redis_conn.get("device_"+str(device_args)))
        df_new = df[['latitude','longitude','time_stamp']].loc[(df['sts'] >= start_time) & (df['sts'] <= end_time)]
        if df_new.any:
            return make_response(jsonify(df_new.to_dict(orient='records')), 200)
        else:
            return make_response(jsonify('No data available for the give time',204))
    else:
        return make_response(jsonify('No data available in redis',403))

if __name__ == '__main__':
    app.run(host='0.0.0.0')
