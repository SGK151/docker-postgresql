import json
from flask import Flask, jsonify, request
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
load_dotenv()
import logging
import psycopg2


app = Flask(__name__)
def db_conn():
    app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
    db = SQLAlchemy(app)
    db.create_all()


# #create function
# def db_conn():
#      return f'{os.getenv("CONN_STRING")}://{os.getenv("USER")}:{os.getenv("PASSWORD")}@{os.getenv("URL")}:{os.getenv("PORT")}/{os.getenv("DATABASE")}'
# #     url = os.environ["URL"]
# #     database = os.environ["DATABASE"]
# #     username = os.environ["USERNAME"]
# #     password = os.environ["PASSWORD"]
# #     conn_string=os.environ["CONN_STRING"]
# #     port=os.environ["PORT"]
# #     print(url)
# #     print(database)
# #     print(username)
# #     print(password)
# #     print(conn_string)
# #     print(port)
# #     connection_string=conn_string +"://"+username+":"+password+"@"+url+":"+port+"/"+database
# #     return connection_string
# # # #
# # #
# # #
def insert_to_table(df):
    result=db_conn()
    print(result)
    #conn_string = 'postgresql://postgres:Gowtham98070142@localhost:5432/postgres'
    print(result)
    db = create_engine(result)
    conn = db.connect()
    df.to_sql('gowtham_flask_1', con=conn, if_exists='append', index=False)
    conn.close()
    return True



@app.route('/Ingest', methods=['POST'])

def ingest():
    r = request.get_json()
    data = r['data']
    print(data,flush=True)
    df = pd.DataFrame.from_records(data)
    print(df,flush=True)
    #logging.info("hello world")
    insert_to_table(df)
    return jsonify({"status": 200})

if __name__=="__main__":
    app.run(debug=True,host="0.0.0.0",port=8000)