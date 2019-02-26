# -*- coding: utf-8 -*-
"""
Created on Sun Feb 24 17:18:15 2019

@author: rober
"""
#conn.rollback()
import csv
import psycopg2
conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
cur = conn.cursor()

#Because I am creating a table and there is no data in it, I used DROP TABLE to change some things.
#For example, LAT was integer, but it would not take 23.5 because of the decimal. 
#So I needed to change it to numeric (I realize there is most likely a more data-efficient type to change it to)
#If I had a bunch of data in the StormData, I would not use DROP TABLE to change the metadata.

#cur.execute("DROP TABLE StormData")
cur.execute("""
            CREATE TABLE StormData(
            FID integer PRIMARY KEY,
            YEAR integer,
            MONTH integer,
            DAY integer,
            AD_TIME text,
            BTID integer,
            NAME text,
            LAT numeric,
            LONG numeric,
            WIND_KTS integer,
            PRESSURE integer,
            CAT text,
            BASIN text,
            Shape_Leng numeric
            )
            """)
conn.commit()            

with open(r'C:\Users\rober\OneDrive\Desktop\DataQuest\Guided Project Storm\storm_data.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        cur.execute(
                "INSERT INTO StormData VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                row
        )
conn.commit()

cur.execute("CREATE USER noob")
conn.commit()
cur.execute("REVOKE ALL ON StormData FROM noob")
conn.commit()
cur.execute("GRANT SELECT ON StormData TO noob")
conn.commit()


