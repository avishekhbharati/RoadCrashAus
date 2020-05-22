from flask import Flask, render_template, request, redirect, url_for, flash, Response, session, json, jsonify
from flask_bootstrap import Bootstrap
from flask_mysqldb import MySQL
import MySQLdb.cursors
from filters import datetimeformat, file_type
from resources import get_bucket, get_dynamo_client, get_records_count, csv_get_dict_records
from flask_cors import CORS
from boto3.dynamodb.conditions import Key, Attr
import boto3
from helper import PasswordHelper, Response_Message, DecimalEncoder
import datetime
import sys


#application varibale is for aws. As it looks for this key
application = app = Flask(__name__)
Bootstrap(app)

#allows CORS for all domains on all routes
CORS(app)

app.secret_key = 'hdskajdhksadewuywuyrweuyrtweu8937483274324uewyry873473yurwueyr78r46r873yeuwryuwe'

app.jinja_env.filters['datetimeformat'] = datetimeformat
app.jinja_env.filters['file_type'] = file_type


# Enter your database connection details below
app.config['MYSQL_HOST'] = 'database-1.c8nhtnrz7bo3.us-east-1.rds.amazonaws.com'
app.config['MYSQL_USER'] = 'admin'
app.config['MYSQL_PASSWORD'] = 'password'
app.config['MYSQL_DB'] = 'roadcrashaus'

# Intialize MySQL
try:
    mysql = MySQL(app)
except Exception as e:
    print("ERROR: Unexpected error: Could not connect to MySql instance.")
    sys.exit()


@app.route('/index')
def index():
    return render_template('index.html')


@app.route('/')
@app.route('/files', methods=['GET', 'POST'])
def files():
    my_bucket = get_bucket()
    summaries = my_bucket.objects.all()
    print(summaries)

    return render_template('files.html', my_bucket=my_bucket, files=summaries)


@app.route('/upload', methods=['POST'])
def upload():
    file = request.files['file']

    my_bucket = get_bucket()
    fileList = my_bucket.objects.all()

    #check if the file is duplicate
    for data in fileList:
        if data.key == file.filename:
            flash('Upload Failed!! File with same name already exists.')
            return redirect(url_for('files'))

    try:
        #initial_count = get_records_count(file.filename)

        my_bucket.Object(file.filename).put(Body=file)
        print("Upload done.")

        #only upload crash related files in db records.
        if file.filename.endswith("Crash.csv"):   
            #get csv records for the file
            records = csv_get_dict_records(file.filename)
            records_inserted = DbOperation(mysql).insert_records(records)
            #final_count = get_records_count(file.filename)
            flash('File uploaded successfully. Total records inserted in db: ' + str(records_inserted))
        else:
            flash('File uploaded successfully.')
    except Exception as ex:
        print(ex)
        flash('Upload failed. Exception: ', str(ex))

    return redirect(url_for('files'))


@app.route('/delete', methods=['POST'])
def delete():
    key = request.form['key']

    my_bucket = get_bucket()
    my_bucket.Object(key).delete()

    flash('File deleted successfully')
    return redirect(url_for('files'))


@app.route('/download', methods=['POST'])
def download():
    key = request.form['key']

    my_bucket = get_bucket()
    file_obj = my_bucket.Object(key).get()

    return Response(
        file_obj['Body'].read(),
        mimetype='text/plain',
        headers={"Content-Disposition": "attachment;filename={}".format(key)}
    )


'''
    *************APIS*****************
'''

@app.route('/api/register', methods=['POST'])
def register():
    if request.method == 'POST' and 'username' in request.json and 'password' in request.json:
        username = request.json['username']
        password = request.json['password']

        print(username, ", ", password)
        password_hashed = PasswordHelper().hash_password(password)

        records = DbOperation(mysql).check_user_exists(username)

        if records is not None:
            return json.dumps(Response_Message(False, "Duplicate user.").__dict__)

        #else insert the new user
        try: 
            DbOperation(mysql).insert_user(username, password_hashed)
            return json.dumps(Response_Message(True).__dict__)
        except Exception as ex:
            return json.dumps(Response_Message(False, str(ex)).__dict__)


@app.route('/api/login', methods=['POST'])
def login():
    try:
        username = request.json['username']
        password = request.json['password']

        #check if user exixts
        records = DbOperation(mysql).check_user_exists(username)
        print(records)
        if records is None:
            return json.dumps(Response_Message(False, "User doesn't exist.").__dict__)
        else:
            hashed_password = records['password']
            password_matched = PasswordHelper().verify_password(hashed_password, password)

            if password_matched:
                return json.dumps(Response_Message(True).__dict__)
            else:
                return json.dumps(Response_Message(False, "Invalid Password").__dict__)
    except Exception as ex:
        return json.dumps(Response_Message(False, str(ex)).__dict__)

#need to check
@app.route('/api/upload', methods=['POST'])
def uploadfile():
    file = request.files['file']

    my_bucket = get_bucket()
    fileList = my_bucket.objects.all()

    #check if the file is duplicate
    for data in fileList:
        if data.key == file.filename:
            return json.dumps(Response_Message(False, 'Upload Failed!! File with same name already exists.').__dict__)
    try:
        my_bucket.Object(file.filename).put(Body=file)

        #only upload crash related files in db records.
        if file.filename.endswith("Crash.csv"):        
            #get csv records for the file
            records = csv_get_dict_records(file.filename)
            records_inserted = DbOperation(mysql).insert_records(records)
            return json.dumps(Response_Message(True, 'File uploaded successfully. Total records inserted in database: ' + str(records_inserted)).__dict__)
        else:
            return json.dumps(Response_Message(True, 'File uploaded successfully.').__dict__)
    except Exception as ex:
        return json.dumps(Response_Message(False, 'Upload failed. Exception: ' + str(ex)))


@app.route('/api/download', methods=['POST'])
def download_file():
    key = request.json['key']

    my_bucket = get_bucket()
    file_obj = my_bucket.Object(key).get()

    return Response(
        file_obj['Body'].read(),
        mimetype='text/plain',
        headers={"Content-Disposition": "attachment;filename={}".format(key)}
    )


@app.route('/api/delete', methods=['POST'])
def delete_file():
    try:
        key = request.json['key']
        my_bucket = get_bucket()
        my_bucket.Object(key).delete()
        return json.dumps(Response_Message(True, "File deleted sucessfully.").__dict__)
    except Exception as ex:
        return json.dumps(Response_Message(False, "Filed to delete the file.").__dict__)


@app.route('/api/files', methods=['GET', 'POST'])
def all_files():
    my_bucket = get_bucket()
    summaries = my_bucket.objects.all()
    payload = []
    data = {}
    # json.dumps(summaries.__dict__)
    for summary in summaries:
        file = summary.key
        lastmodified = datetimeformat(summary.last_modified)
        filetype = file_type(summary.key)
        
        data['file'] = file 
        data['lastmodified'] = lastmodified
        data['filetype'] = filetype

        payload.append(data)
    return jsonify(payload)


@app.route('/api/crashbyyear', methods=['POST', 'GET'])
def data_by_year():
    if request.method == 'POST' and 'year' in request.json:
        year = request.json['year']
        print(year)
        records = DbOperation(mysql).get_records_for_year(year)
        return jsonify(records)
    else:
        records = DbOperation(mysql).get_records_for_year(None)
        return jsonify(records)


@app.route('/api/datagender', methods=['POST', 'GET'])
def data_by_gender():
    if request.method == 'POST' and 'year' in request.json:
        year = request.json['year']
        records = DbOperation(mysql).get_records_male_females(year)
        return jsonify(records)
    else:
        records = DbOperation(mysql).get_records_male_females(None)
        #return json.dumps(records, cls=DecimalEncoder)
        return jsonify(records)


@app.route('/api/dataregion', methods=['POST', 'GET'])
def data_by_region():
    if request.method == 'POST' and 'year' in request.json:
        year = request.json['year']
        records = DbOperation(mysql).get_records_region(year)
        return jsonify(records)
    else:
        records = DbOperation(mysql).get_records_region(None)
        #return json.dumps(records, cls=DecimalEncoder)
        print(records)
        return jsonify(records)

class DbOperation:
    def __init__(self, mysqlConn):
        self.mysql = mysqlConn


    def check_user_exists(self, username):
        cursor = self.mysql.connection.cursor(MySQLdb.cursors.DictCursor)  
        try:
            cursor.execute("SELECT * from accounts where username = %s", (username,))
            content = cursor.fetchone()
            return content
        except Exception as ex:
            print("Exceptions :: ", str(ex))
        finally:
            cursor.close()

    def insert_user(self, username, hashed_password):
        cur = self.mysql.connection.cursor() 

        try:
            cur.execute("INSERT INTO accounts(username, password) VALUES(%s, %s)", (username, hashed_password ))
            mysql.connection.commit()
            return True
        except Exception as ex:
            print("Exception: ", str(ex))
        finally:
            cur.close()


    def insert_records(self, records):
        cur = self.mysql.connection.cursor()      

        query = "INSERT INTO crashdetails(ACCIDENT_NO,ACCIDENT_DATE,ACCIDENT_TIME,ACCIDENT_TYPE,DAY_OF_WEEK,LIGHT_CONDITION,SPEED_ZONE,LONGITUDE,LATITUDE,REGION_NAME,TOTAL_PERSONS, INJ_OR_FATAL,MALES,FEMALES,ALCOHOL_RELATED,RMA) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "

        values_list = []
        tuplecount = 0
        records_processed = 0

        for record in records:                
            try:
                if record['ACCIDENT_NO'] == '':
                    break

                #convert to specific data types (numbers)
                LONGITUDE = float(record['LONGITUDE']) if record['LONGITUDE'] != '' else 0.0
                LATITUDE = float(record['LATITUDE']) if record['LATITUDE'] != '' else 0.0
                TOTAL_PERSONS = float(record['TOTAL_PERSONS']) if record['TOTAL_PERSONS'] != '' else 0.0
                INJ_OR_FATAL = float(record['INJ_OR_FATAL']) if record['INJ_OR_FATAL'] != '' else 0.0
                MALES = float(record['MALES']) if record['MALES'] != '' else 0.0
                FEMALES = float(record['FEMALES']) if record['FEMALES'] != '' else 0.0 
                ACCIDENT_DATE = self.convert_date(record['ACCIDENT_DATE'])
                ACCIDENT_TIME = self.convert_time(record['ACCIDENT_TIME'])

                if ACCIDENT_DATE is None or ACCIDENT_TIME is None:
                    continue
                
                values_list.append((record['ACCIDENT_NO'], ACCIDENT_DATE, ACCIDENT_TIME, record['ACCIDENT_TYPE'], 
                record['DAY_OF_WEEK'], record['LIGHT_CONDITION'], record['SPEED_ZONE'], LONGITUDE, 
                LATITUDE, record['REGION_NAME'], TOTAL_PERSONS, INJ_OR_FATAL, 
                MALES, FEMALES, record['ALCOHOL_RELATED'], record['RMA']))

                tuplecount = tuplecount + 1

                if tuplecount % 1000 == 0:
                    cur.executemany(query, values_list)
                    #empty the values list.
                    values_list.clear()
                    print(tuplecount)
                
                records_processed = records_processed + 1 
            except Exception as ex:
                print("Exception: ", str(ex))
                print(record)
        
        #for the remaining rows < 100
        if len(values_list) > 0 : 
            try:
                cur.executemany(query, values_list)
                #empty the values list.
                values_list.clear()
                print(tuplecount)
            except Exception as ex:
                print("Exception: ", str(ex))
                print(record)


        mysql.connection.commit()
        cur.close()
        print("Records :: ", str(records_processed))
        print("Cursor :: ", str(cur.rowcount))
        return records_processed
    
    def get_records_for_year(self, year_param = None):
        cursor = self.mysql.connection.cursor(MySQLdb.cursors.DictCursor)  
        try:
            if year_param is None or year_param == '':
                cursor.execute("SELECT YEAR(ACCIDENT_DATE) as _year, MONTH(ACCIDENT_DATE) as _month, COUNT(*) as _count FROM crashdetails GROUP BY _year, _month " )
            else:
                cursor.execute("SELECT YEAR(ACCIDENT_DATE) as _year, MONTH(ACCIDENT_DATE) as _month, COUNT(*) as _count FROM crashdetails WHERE YEAR(ACCIDENT_DATE) = %s GROUP BY _year, _month ORDER BY _year, _month ", (year_param,))
            results = cursor.fetchall()

            print(results)
            payload = []
            content = {}
            
            year_match = 0
            for result in results:
                print(result)
                content = {'year': result['_year'], 'month': self.get_month(result['_month']), 'count': result['_count']}
                payload.append(content)
                content = {}
            
            print("Payloaddddddddddddddddd")
            print(payload)
            return payload 

        except Exception as ex:
            print("Exception: ", str(ex))
        finally:
            cursor.close()

    def get_records_male_females(self, year_param):
        cursor = self.mysql.connection.cursor(MySQLdb.cursors.DictCursor)  
        try:
            if year_param is None or year_param == '':
                cursor.execute("SELECT  SUM(MALES) as males, SUM(FEMALES) as females FROM crashdetails" )
            else:
                cursor.execute("SELECT SUM(MALES) as males, SUM(FEMALES) as females FROM crashdetails where YEAR(ACCIDENT_DATE) = %s", (year_param,))
            result = cursor.fetchone()
            content = {}
            content['males'] = float(result['males']) 
            content['females'] = float(result['females']) 
            return content
        except Exception as ex:
            print("Exceptions :: ", str(ex))
        finally:
            cursor.close()
    
    def get_records_region(self, year_param):
        cursor = self.mysql.connection.cursor(MySQLdb.cursors.DictCursor)  
        try:
            if year_param is None or year_param == '':
                cursor.execute("SELECT DISTINCT(REGION_NAME) as region, COUNT(ACCIDENT_NO) as _count from crashdetails GROUP BY region" )
            else:
                cursor.execute("SELECT DISTINCT(REGION_NAME) as region, COUNT(ACCIDENT_NO) as _count from crashdetails where YEAR(ACCIDENT_DATE) = %s GROUP BY region", (year_param,))
            results = cursor.fetchall()
            
            return results
        except Exception as ex:
            print("Exceptions :: ", str(ex))
        finally:
            cursor.close()                                                  

    #class specific helper functions
    def convert_date(self, date_str):
        if date_str == '':
            return None
            
        date_obj = datetime.datetime.strptime(date_str, '%d/%m/%Y')
        return date_obj.date()
    
    def convert_time(self, time_str):
        if time_str == '':
            return None

        time_obj = datetime.datetime.strptime(time_str, '%H.%M.%S')
        return time_obj

    def get_month(self, month_index):
        month_list = ['n/a','Jan','Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul','Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        return month_list[month_index]



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
