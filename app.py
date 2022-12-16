#UTA ID:1001911486 
#NAME: Rushikesh Mahesh Bhagat

from flask import Flask, render_template, request
import pyodbc
import haversine as hs
import requests

server = 'Add database server name'
database = 'Add database name'
username = 'Add database username'
password = 'Add user password'   
driver= '{ODBC Driver 17 for SQL Server}'

connstr = 'DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password

mapQuest_key = 'Add your MapQuest API key'
mapQuest_url = 'http://open.mapquestapi.com/geocoding/v1/address?key='

app = Flask(__name__)

def select_query(query):
    list_result=[]
    try:
        conn = pyodbc.connect(connstr)
        cursor = conn.cursor()
        cursor.execute(query)
        
        list_result = cursor.fetchall()

    except Exception as e:
        print(e,"Error connecting DB")

    finally:
        if conn:
            cursor.close()
            conn.close()
    return list_result

@app.route("/", methods=['GET','POST'])
@app.route("/index", methods=['GET','POST'])
def index():
    if request.method == 'POST':
        try:
            if 'magnitude' in request.form:
                magnitude = request.form["magnitude"]
                from_date = request.form["mag_from_date"]
                to_date = request.form["mag_to_date"]
                query1 = "SELECT * FROM all_month WHERE 1 = 1 "
                if len(magnitude)!=0:
                    magnitude = float(magnitude)
                    query1+= "AND mag > " + str(magnitude)
                if len(from_date) !=0  and len(to_date) !=0 :
                    query1+=" AND time between '" + str(from_date) + "' AND '" + str(to_date) + "'"

                list_result1 = select_query(query1)
                count_rows = f"The count of earthquakes occured with magnitude greather than {magnitude} is {(len(list_result1))} from {from_date} to {to_date}"                
                b_headings1 = ["time", "latitude","longitude", "depth","mag","magType","nst","gap","dmin","rms","net","id","updated","place","type","horizontalError","depthError","magError","magNst","status","locationSource","magSource"]

                return render_template('index.html',count_rows=count_rows,b_headings1=b_headings1,list_result1=list_result1)
            
            if 'distance' in request.form:
                location = request.form["distance_loc"]
                latitude = request.form["distance_lat"]
                longitude = request.form["distance_long"]
                from_date = request.form["distance_from_date"]
                to_date = request.form["distance_to_date"]
                #list_result2 = []

                if len(location)!=0:
                    main_url = mapQuest_url+mapQuest_key+'&location='+location
                    location_data = requests.get(main_url).json()['results'][0]['locations'][0]['latLng']
                    latitude = location_data['lat']
                    longitude = location_data['lng']

                distance = request.form["distance"]
                if len(distance)!=0:
                    distance = float(distance)

                query2 = "SELECT * FROM all_month WHERE 1 = 1 "
                if len(from_date) !=0  and len(to_date) !=0 :
                    query2+=" AND time between '" + str(from_date) + "' AND '" + str(to_date) + "'"
                query2+=" ORDER By mag DESC"
                list_result2 = select_query(query2)
                location1 = (float(latitude),float(longitude))
                    
                list_result2_arr=[]
                for item in list_result2:
                    location2 = (float(item[1]),float(item[2]))
                    actual_diff = hs.haversine(location1,location2)
                    if distance >= actual_diff:
                        list_result2_arr.append(item)
                
                count_rows2 = f"The count of earthquakes from {location} lat= {latitude} and lng= {longitude} within {distance} km is {len(list_result2)} from {from_date} to {to_date}"
                
                b_headings2 = ["time", "latitude","longitude", "depth","mag","magType","nst","gap","dmin","rms","net","id","updated","place","type","horizontalError","depthError","magError","magNst","status","locationSource","magSource"]

                return render_template('index.html',count_rows2=count_rows2,b_headings2=b_headings2,list_result2=list_result2_arr)
            
            if 'group_from_date' in request.form:
                group_from_date = request.form["group_from_date"]
                group_to_date = request.form["group_to_date"]
                list_result3_arr = []

                for i in range(1,7):
                    query3 = "SELECT * FROM all_month WHERE mag BETWEEN " + str(float(i)) + " AND " + str(float(i+1)) + " AND time between '" + str(group_from_date) + "' AND '" + str(group_to_date) + "'"
                    list_result3 = select_query(query3)
                    list_result3_arr.append([(i,i+1),len(list_result3)])

                count_rows3 = f"Total earthquakes from {group_from_date} to {group_to_date} are:"
                b_headings3 = ["Magnitude Group","No of Earthquakes"]
                return render_template('index.html',scroll1="mag_group",count_rows3=count_rows3,b_headings3=b_headings3,list_result3=list_result3_arr)


            if 'no_of_quakes' in request.form:
                no_of_quakes =  request.form["no_of_quakes"]
                query4 = "SELECT TOP " +no_of_quakes+ " * FROM all_month ORDER BY mag DESC"
                list_result4 = select_query(query4)

                count_rows4 = f"The top {no_of_quakes} Earthquakes are as below"
                b_headings4 = ["time", "latitude","longitude", "depth","mag","magType","nst","gap","dmin","rms","net","id","updated","place","type","horizontalError","depthError","magError","magNst","status","locationSource","magSource"]

                return render_template('index.html',scroll2="largest_quakes",count_rows4=count_rows4,b_headings4=b_headings4,list_result4=list_result4)
     
        except Exception as e:
            print(e,"Error has occured")
            

    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
