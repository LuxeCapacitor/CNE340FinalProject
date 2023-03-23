# Joshua Brown, Dre Owens, Farrius Dean
# 3/13/2023 - Final Project
# CNE 340, Winter - Christine Sutton

import mysql.connector
import json
import requests
from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt

mydb = mysql.connector.connect( # Provided by Dre Owens
    host='127.0.0.1',
    user='root',
    password='root',
    port='8889'
)

mycursor = mydb.cursor()

mycursor.execute("CREATE DATABASE IF NOT EXISTS weather")

def connect_to_sql():
    conn = mysql.connector.connect(user='root', password='root',
                                   host='127.0.0.1', database='weather', port='8889')
    return conn

def create_table(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS weather (id INT PRIMARY KEY auto_increment,
                    year DATETIME, yearly_rain FLOAT)''')

def query_sql(cursor, query):
    cursor.execute(query)
    return cursor

def add_rain_data2013(weatherdetails2013): # Blocks of code that sum the daily rain data by year and import the totals into the MySql database, Josh Brown
    conn = mysql.connector.connect(user='root', password='root',
                                    host='127.0.0.1', database='weather', port='8889')
    cursor = conn.cursor()
    rain_data = weatherdetails2013["daily"]["rain_sum"]
    yearly_rain = sum(rain for rain in rain_data if rain is not None)
    query = cursor.execute('INSERT INTO weather(year, yearly_rain) VALUES(%s, %s)',
                           ("2013-12-31", yearly_rain))
    conn.commit()
    cursor.close()
    conn.close()
    return query_sql(cursor, query)

def fetch_new_data2013():
    query = requests.get("https://archive-api.open-meteo.com/v1/archive?latitude=47.61&longitude=-122.33&start_date=2013-01-01&end_date=2013-12-31&daily=rain_sum&timezone=America%2FLos_Angeles&precipitation_unit=inch")
    datas = json.loads(query.text)
    return datas

def add_rain_data2014(weatherdetails2014):
    conn = mysql.connector.connect(user='root', password='root',
                                    host='127.0.0.1', database='weather', port='8889')
    cursor = conn.cursor()
    rain_data = weatherdetails2014["daily"]["rain_sum"]
    yearly_rain = sum(rain for rain in rain_data if rain is not None)
    query = cursor.execute('INSERT INTO weather(year, yearly_rain) VALUES(%s, %s)',
                           ("2014-12-31", yearly_rain))
    conn.commit()
    cursor.close()
    conn.close()
    return query_sql(cursor, query)

def fetch_new_data2014():
    query = requests.get("https://archive-api.open-meteo.com/v1/archive?latitude=47.61&longitude=-122.33&start_date=2014-01-01&end_date=2014-12-31&daily=rain_sum&timezone=America%2FLos_Angeles&precipitation_unit=inch")
    datas = json.loads(query.text)
    return datas

def add_rain_data2015(weatherdetails2015):
    conn = mysql.connector.connect(user='root', password='root',
                                    host='127.0.0.1', database='weather', port='8889')
    cursor = conn.cursor()
    rain_data = weatherdetails2015["daily"]["rain_sum"]
    yearly_rain = sum(rain for rain in rain_data if rain is not None)
    query = cursor.execute('INSERT INTO weather(year, yearly_rain) VALUES(%s, %s)',
                           ("2015-12-31", yearly_rain))
    conn.commit()
    cursor.close()
    conn.close()
    return query_sql(cursor, query)

def fetch_new_data2015():
    query = requests.get("https://archive-api.open-meteo.com/v1/archive?latitude=47.61&longitude=-122.33&start_date=2015-01-01&end_date=2015-12-31&daily=rain_sum&timezone=America%2FLos_Angeles&precipitation_unit=inch")
    datas = json.loads(query.text)
    return datas

def add_rain_data2016(weatherdetails2016):
    conn = mysql.connector.connect(user='root', password='root',
                                    host='127.0.0.1', database='weather', port='8889')
    cursor = conn.cursor()
    rain_data = weatherdetails2016["daily"]["rain_sum"]
    yearly_rain = sum(rain for rain in rain_data if rain is not None)
    query = cursor.execute('INSERT INTO weather(year, yearly_rain) VALUES(%s, %s)',
                           ("2016-12-31", yearly_rain))
    conn.commit()
    cursor.close()
    conn.close()
    return query_sql(cursor, query)

def fetch_new_data2016():
    query = requests.get("https://archive-api.open-meteo.com/v1/archive?latitude=47.61&longitude=-122.33&start_date=2016-01-01&end_date=2016-12-31&daily=rain_sum&timezone=America%2FLos_Angeles&precipitation_unit=inch")
    datas = json.loads(query.text)
    return datas

def add_rain_data2017(weatherdetails2017):
    conn = mysql.connector.connect(user='root', password='root',
                                    host='127.0.0.1', database='weather', port='8889')
    cursor = conn.cursor()
    rain_data = weatherdetails2017["daily"]["rain_sum"]
    yearly_rain = sum(rain for rain in rain_data if rain is not None)
    query = cursor.execute('INSERT INTO weather(year, yearly_rain) VALUES(%s, %s)',
                           ("2017-12-31", yearly_rain))
    conn.commit()
    cursor.close()
    conn.close()
    return query_sql(cursor, query)

def fetch_new_data2017():
    query = requests.get("https://archive-api.open-meteo.com/v1/archive?latitude=47.61&longitude=-122.33&start_date=2017-01-01&end_date=2017-12-31&daily=rain_sum&timezone=America%2FLos_Angeles&precipitation_unit=inch")
    datas = json.loads(query.text)
    return datas

def add_rain_data2018(weatherdetails2018):
    conn = mysql.connector.connect(user='root', password='root',
                                    host='127.0.0.1', database='weather', port='8889')
    cursor = conn.cursor()
    rain_data = weatherdetails2018["daily"]["rain_sum"]
    yearly_rain = sum(rain for rain in rain_data if rain is not None)
    query = cursor.execute('INSERT INTO weather(year, yearly_rain) VALUES(%s, %s)',
                           ("2018-12-31", yearly_rain))
    conn.commit()
    cursor.close()
    conn.close()
    return query_sql(cursor, query)

def fetch_new_data2018():
    query = requests.get("https://archive-api.open-meteo.com/v1/archive?latitude=47.61&longitude=-122.33&start_date=2018-01-01&end_date=2018-12-31&daily=rain_sum&timezone=America%2FLos_Angeles&precipitation_unit=inch")
    datas = json.loads(query.text)
    return datas

def add_rain_data2019(weatherdetails2019):
    conn = mysql.connector.connect(user='root', password='root',
                                    host='127.0.0.1', database='weather', port='8889')
    cursor = conn.cursor()
    rain_data = weatherdetails2019["daily"]["rain_sum"]
    yearly_rain = sum(rain for rain in rain_data if rain is not None)
    query = cursor.execute('INSERT INTO weather(year, yearly_rain) VALUES(%s, %s)',
                           ("2019-12-31", yearly_rain))
    conn.commit()
    cursor.close()
    conn.close()
    return query_sql(cursor, query)

def fetch_new_data2019():
    query = requests.get("https://archive-api.open-meteo.com/v1/archive?latitude=47.61&longitude=-122.33&start_date=2019-01-01&end_date=2019-12-31&daily=rain_sum&timezone=America%2FLos_Angeles&precipitation_unit=inch")
    datas = json.loads(query.text)
    return datas

def add_rain_data2020(weatherdetails2020):
    conn = mysql.connector.connect(user='root', password='root',
                                    host='127.0.0.1', database='weather', port='8889')
    cursor = conn.cursor()
    rain_data = weatherdetails2020["daily"]["rain_sum"]
    yearly_rain = sum(rain for rain in rain_data if rain is not None)
    query = cursor.execute('INSERT INTO weather(year, yearly_rain) VALUES(%s, %s)',
                           ("2020-12-31", yearly_rain))
    conn.commit()
    cursor.close()
    conn.close()
    return query_sql(cursor, query)

def fetch_new_data2020():
    query = requests.get("https://archive-api.open-meteo.com/v1/archive?latitude=47.61&longitude=-122.33&start_date=2020-01-01&end_date=2020-12-31&daily=rain_sum&timezone=America%2FLos_Angeles&precipitation_unit=inch")
    datas = json.loads(query.text)
    return datas

def add_rain_data2021(weatherdetails2021):
    conn = mysql.connector.connect(user='root', password='root',
                                    host='127.0.0.1', database='weather', port='8889')
    cursor = conn.cursor()
    rain_data = weatherdetails2021["daily"]["rain_sum"]
    yearly_rain = sum(rain for rain in rain_data if rain is not None)
    query = cursor.execute('INSERT INTO weather(year, yearly_rain) VALUES(%s, %s)',
                           ("2021-12-31", yearly_rain))
    conn.commit()
    cursor.close()
    conn.close()
    return query_sql(cursor, query)

def fetch_new_data2021():
    query = requests.get("https://archive-api.open-meteo.com/v1/archive?latitude=47.61&longitude=-122.33&start_date=2021-01-01&end_date=2021-12-31&daily=rain_sum&timezone=America%2FLos_Angeles&precipitation_unit=inch")
    datas = json.loads(query.text)
    return datas

def add_rain_data2022(weatherdetails2022):
    conn = mysql.connector.connect(user='root', password='root',
                                    host='127.0.0.1', database='weather', port='8889')
    cursor = conn.cursor()
    rain_data = weatherdetails2022["daily"]["rain_sum"]
    yearly_rain = sum(rain for rain in rain_data if rain is not None)
    query = cursor.execute('INSERT INTO weather(year, yearly_rain) VALUES(%s, %s)',
                           ("2022-12-31", yearly_rain))
    conn.commit()
    cursor.close()
    conn.close()
    return query_sql(cursor, query)

def fetch_new_data2022():
    query = requests.get("https://archive-api.open-meteo.com/v1/archive?latitude=47.61&longitude=-122.33&start_date=2022-01-01&end_date=2022-12-31&daily=rain_sum&timezone=America%2FLos_Angeles&precipitation_unit=inch")
    datas = json.loads(query.text)
    return datas

def weatherdata():
    weatherdata2013 = fetch_new_data2013()
    add_rain_data2013(weatherdata2013)

    weatherdata2014 = fetch_new_data2014()
    add_rain_data2014(weatherdata2014)

    weatherdata2015 = fetch_new_data2015()
    add_rain_data2015(weatherdata2015)
    
    weatherdata2016 = fetch_new_data2016()
    add_rain_data2016(weatherdata2016)
    
    weatherdata2017 = fetch_new_data2017()
    add_rain_data2017(weatherdata2017)
    
    weatherdata2018 = fetch_new_data2018()
    add_rain_data2018(weatherdata2018)
    
    weatherdata2019 = fetch_new_data2019()
    add_rain_data2019(weatherdata2019)
    
    weatherdata2020 = fetch_new_data2020()
    add_rain_data2020(weatherdata2020)
    
    weatherdata2021 = fetch_new_data2021()
    add_rain_data2021(weatherdata2021)
    
    weatherdata2022 = fetch_new_data2022()
    add_rain_data2022(weatherdata2022)

def main():
    conn = connect_to_sql()
    cursor = conn.cursor()
    create_table(cursor)
    weatherdata()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()

def get_weather_entries(): # Provided by Farrius dean
    hostname = "127.0.0.1"
    uname = "root"
    pwd = "root"
    dbname = "weather"
    prt = "8889"

    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}:{port}/{db}"
                           .format(host=hostname, db=dbname, user=uname, pw=pwd, port=prt))

    df = pd.read_sql_table('weather', engine.connect())
    entries = {'yearly_rain': list(df['yearly_rain'])}
    return entries

weather_data = get_weather_entries()

# Dictionary with lists of data points stored in keys.
data = {'year': [2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022],
        'avg_aqi': [54.1, 51.4, 54.5, 46.1, 56.8, 52.8, 48.7, 50.4, 47.8, 57.6],
        'yearly_rain': weather_data["yearly_rain"]}

# Citation - JO 
# https://www.geeksforgeeks.org/how-to-plot-a-dataframe-using-pandas/
# https://www.tutorialspoint.com/how-to-make-two-plots-side-by-side-using-python
# https://www.activestate.com/resources/quick-reads/how-to-display-a-plot-in-python/

# Convert data to DataFrame
df = pd.DataFrame(data)

# Create figure and axes
fig, (ax1, ax2) = plt.subplots(ncols=2, figsize=(10, 5), sharey=True)

# Plot data on first axis
ax1.plot(df['avg_aqi'], df['year'], color='red', linewidth=2)
ax1.set_ylabel('Year')
ax1.set_xlabel('Average AQI')
ax1.set_title('Average AQI Over 10 Years')

# Set y-axis limits and ticks
ax1.set_ylim(2012, 2023)
ax1.set_yticks(df['year'].tolist() + [2012, 2023])

# Set x-axis limits and ticks
ax1.set_xlim(40, 60)
ax1.set_xticks(range(40, 61, 5))

# Plot data on second axis
ax2.plot(df['yearly_rain'], df['year'], color='blue', linewidth=2)
ax2.set_ylabel('Year')
ax2.set_xlabel('Total Rainfall per year In Inches')
ax2.set_title('Total Rainfall per year Over 10 years')

# Set y-axis limits and ticks
ax2.set_ylim(2012, 2023)
ax2.set_yticks(df['year'].tolist() + [2012, 2023])

# Set x-axis limits and ticks
ax2.set_xlim(30, 70)
ax2.set_xticks(range(30, 71, 10))

# Show the plot
plt.show()
