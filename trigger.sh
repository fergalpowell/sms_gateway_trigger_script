#!/usr/bin/env python
from __future__ import print_function
from subprocess import call
import os
import sys
import psycopg2
import pandas as pd, numpy as np, matplotlib.pyplot as plt
from sklearn.cluster import DBSCAN
from geopy.distance import great_circle
from shapely.geometry import MultiPoint
from shapely import geometry
from math import radians, cos, sin, asin, sqrt

# CLUSTERING TO DETECT HOME AND WORK LOCATION
def Cluster(points, radius, min):
    kms_per_radian = 6371.0088
    epsilon = radius / kms_per_radian
    db = DBSCAN(eps=epsilon, min_samples=min, algorithm='ball_tree', metric='haversine').fit(np.radians(points))
    cluster_labels = db.labels_
    num_clusters = len(set(cluster_labels))
    clusters = pd.Series([df[cluster_labels == n] for n in range(num_clusters)])
    biggest_cluster = 0
    if(num_clusters > 1):
        for i in range(num_clusters):
            print(i)
            if(clusters[i].size > clusters[biggest_cluster].size):
                biggest_cluster = i
    return (MultiPoint(clusters[biggest_cluster]).centroid.x, MultiPoint(clusters[biggest_cluster]).centroid.y)

# CALCULATING DISTANCE BETWEEN TWO POINTS IN KM
def CalculateDistance(lat1, lon1, lat2, lon2):
    lon1 = radians(lon1)
    lon2 = radians(lon2)
    lat1 = radians(lat1)
    lat2 = radians(lat2)
    # Haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    #Display the result
    print("Distance is: ",c*r,"Kilometers")
    return c*r

numparts = int(os.environ['DECODED_PARTS'])

text = ''
text = os.environ['SMS_1_TEXT']
text = text.lower()
parseText = text.split(" ")

# PARSING MESSAGE
f= open("triggerMessage.txt","w+")
lat = ''
long = ''
speed = ''
date_time = ''
for word in parseText:
    if "last:" in word:
        os.system('gammu-smsd-inject TEXT 0871832141 -text "smslink123456"')
        sys.exit()
    if "lat:" in word:
        f.write("lat: " + word + "\n")
        lat = word[4:]

    if "long:" in word:
        f.write("lat: " + word + "\n")
        long = word[5:]

    if "speed:" in word:
        f.write("lat: " + word + "\n")
        speed = word[6:]

    if "t:" in word:
        f.write("lat: " + word + "\n")
        date_time = word[4:] + " " + parseText[parseText.index(word) + 1]
        month = date_time[:2]
        day = date_time[3:5]
        format_date_time = str(day) + "/" + str(month) + "/" + date_time[5:]
        f.write(format_date_time)

if lat != '' and long != '' and speed != '' and date_time != '':
    try:
        conn = psycopg2.connect("dbname=fyp user=admin host=localhost password=admin")
        cursor = conn.cursor()

        # INSERT INTO LOCATION DATA
        cursor.execute("INSERT INTO track_locationdata(location, speed, date_time, user_id) VALUES (ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, 11);", (float(lat), float(long), float(speed), format_date_time))

        # UPDATE JOURNEYS
        cursor.execute('SELECT ST_X(location), ST_Y(location), date_time FROM track_locationdata WHERE "user_id" = 11 ORDER BY "date_time" DESC;')
        row = cursor.fetchone()
        curX = row[0]
        curY = row[1]
        current_location = geometry.Point(curX, curY)
        current_location_date_time = row[2]
        cursor.execute("SELECT ST_X(work_location), ST_Y(work_location), ST_X(home_location), ST_Y(home_location) FROM track_location WHERE id = 1;")
        row = cursor.fetchone()
        workX = row[0]
        workY = row[1]
        workRadius = .075
        atWork = False
        homeX = row[2]
        homeY = row[3]
        homeRadius = .03
        atHome = False
        home = geometry.Point(homeX, homeY)
        work = geometry.Point(workX, workY)
        homeDistance = CalculateDistance(homeX, homeY, current_location.x, current_location.y)
        workDistance = CalculateDistance(workX, workY, current_location.x, current_location.y)
        print(str(homeX) + " " + str(homeY) + " " + str(workX) + " " + str(workY))
        print(str(homeDistance) + " " + str(workDistance))
        
        if(homeDistance > homeRadius and workDistance > workRadius):
            cursor.execute('SELECT id, finished FROM track_journey WHERE "user_id" = 11 ORDER BY "date_time" DESC;')
            row = cursor.fetchone()
            if(row and row[1] == False):
                print("update")
                cursor.execute("UPDATE track_journey SET route = ST_AddPoint(route, ST_MakePoint(%s, %s)), date_time = %s WHERE id = %s;", [current_location.x, current_location.y,current_location_date_time, row[0]])
            else:
                cursor.execute('SELECT id, finished FROM track_journey WHERE "user_id" = 11 ORDER BY "date_time" DESC;')                
                cursor.execute("INSERT INTO track_journey(route, user_id, saved_journey, outside_geofence, date_time, finished) VALUES (ST_GeomFromText('LINESTRING(%s %s, %s %s)',4326), 11, false, false, %s, false);", [current_location.x, current_location.y, current_location.x, current_location.y, current_location_date_time])
        else:
            print("inside")
            cursor.execute('SELECT id, finished FROM track_journey WHERE "user_id" = 11 ORDER BY "date_time" DESC;')
            row = cursor.fetchone()
            if(row[1] == False):
                cursor.execute("UPDATE track_journey SET route = ST_AddPoint(route, ST_MakePoint(%s, %s)), date_time = %s, finished = true;", [current_location.x, current_location.y, current_location_date_time])                                                                                                                  
        
        # UPDATE CURRENT LOCATION           
        cursor.execute('SELECT location, date_time FROM track_locationdata WHERE "user_id" = 11 ORDER BY "date_time" DESC;')
        row = cursor.fetchone()
        print(str(row[1]))
        cursor.execute('UPDATE track_location SET current_location = %s, date_time = %s WHERE id = 1;', (row[0], row[1]))

        # UPDATE WORK LOCATION
        cursor.execute("SELECT ST_X(location), ST_Y(location) FROM track_locationdata WHERE user_id = 11 AND date_time::time BETWEEN time '09:00:00' AND '17:00:00' AND EXTRACT(dow FROM date_time) BETWEEN 1 AND 5;")
        rows = cursor.fetchall()
        df = np.array(rows)
        work_location = Cluster(df,.075,10)
        print(work_location)
        cursor.execute("UPDATE track_location SET work_location = ST_SetSRID(ST_MakePoint(%s, %s), 4326) WHERE id = 1;", (float(work_location[0]), float(work_location[1])))

        # UPDATE HOME LOCATION
        cursor.execute("SELECT ST_X(location), ST_Y(location) FROM track_locationdata WHERE user_id = 11 AND date_time::time BETWEEN time '19:00:00' AND '23:59:59' OR date_time::time BETWEEN time '00:00:00' AND '07:00:00';")
        rows = cursor.fetchall()
        df = np.array(rows)
        home_location = Cluster(df,.03,10)
        print(home_location)
        cursor.execute("UPDATE track_location SET home_location = ST_SetSRID(ST_MakePoint(%s, %s), 4326) WHERE id = 1;", (float(home_location[0]), float(home_location[1])))
        
        conn.commit()
        cursor.close()
        conn.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    os.system('gammu-smsd-inject TEXT 0871832141 -text "smslink123456"')
f.close()