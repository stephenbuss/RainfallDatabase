#! python3
# Steve Buss, 24 April 2017

# details here https://environment.data.gov.uk/flood-monitoring/doc/rainfall

import urllib
import urllib.request
import json
import csv
import datetime
import requests
import numpy as np
from datetime import date
from datetime import time
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import pprint
import sys
##import iso8601
from dateutil.parser import parse

from io import BytesIO

maxDailyDataPoints = 96

def getData(url):
    response=BytesIO()    
    f = urllib.request.urlopen(url)
    response = f.read()
    strResponse = response.decode('utf-8')    # json.loads object must be a string, not bytes
    dicResponse = json.loads(strResponse)    
    return dicResponse

def getStationRef(long,lat):

    url=('http://environment.data.gov.uk/flood-monitoring/id/stations?parameter=rainfall&long='
         +long+'&lat='+lat+'&dist=10')
    stationRef=getData(url)['items'][0]['stationReference']

    return stationRef
       
def writeLocations(dic):
    # dic is actually a list of dictionaries
    with open(monitoringPointsFilename, 'w') as f:
        # lineterminator='\n' is required to prevent empty lines between data 
        w = csv.writer(f, delimiter=',', lineterminator='\n')
        w.writerow(["StationRef", "x", "y"])
        for item in dic['items']:
            # each str is wrapped in [] so that writerow doesn't 
            # put commas between all the characters
            w.writerow(
                       [str(item['stationReference'])] + 
                       [str(item['easting'])] + 
                       [str(item['northing'])]
                       )
    return 
    
def writeTimeSeries(lst, filename):
    # see comments on formatting in writePoints
    timeSeries = []
    with open(filename, 'w') as f:
        w = csv.writer(f, delimiter=',', lineterminator='\n')
        w.writerow(["Date", "Time", "Value"])
        for item in lst['items']:
            e = {}
            dt = parse(item['dateTime'])
            e['date'] = dt.date()
            e['time'] = dt.time()
            e['value'] = item['value']
            timeSeries.append(e)
            w.writerow(
                       [str(e['date'])] + 
                       [str(e['time'])] +
                       [str(e['value'])]
                       )
            
    return timeSeries

def writeAggregateTimeSeries(lst, filename):
    # see comments on formatting in writePoints
    with open(filename, 'w') as f:
        # TO DO write some headers for this file
        w = csv.writer(f, delimiter=',', lineterminator='\n')
        w.writerow(["Date", "Value", "PercentComplete"])
        for item in lst:
            w.writerow(
                       [str(item['date'])] + 
                       [str(item['value'])] +
                       [str(item['percent'])]
                       )
            
    return 
    
def aggregateTimeSeries(listData, filename):
    # pandas would do this more efficiently, I'm sure
    # the dates need to be sorted
    
    daysCount = 1 # number of days in the dataset
    dataCount = 0 # number of dataPoints in each day
    daySum = 0 # daily total
    a = []
    
    d = listData[0]['date']
    for dt in listData:
        if d == dt['date']:
            dataCount += 1
            daySum += dt['value']
            lastDate = d
        else:
            a.append({'date': lastDate, 'value': daySum,
                      'percent': 100*dataCount/maxDailyDataPoints})
            
            daysCount =+ 1
            dataCount = 1
            daySum = dt['value']
            d = dt['date']
            lastDate = d
    
    a.append({'date': lastDate, 'value': daySum,
              'percent': 100*dataCount/maxDailyDataPoints}) # write the last lot of data not part of a complete day
            
    writeAggregateTimeSeries(a, filename)
    
    return

def compileData(pointID):
    rawFilename = 'RainDataRaw_' + pointID + '.csv'
    aggFilename = 'RainDataAggreagate_' + pointID + '.csv'
        
    url = ('http://environment.data.gov.uk/flood-monitoring/id/stations/'
           + pointID + '/readings?_sorted&_limit=10000')
    d = writeTimeSeries(getData(url), rawFilename)
    da = aggregateTimeSeries(d, aggFilename)
   
    ### not yet implemented
    # return the value for a given date in the time series
    # return the last n days rainfall
    # change aggregation from 00:00 to 23:59 to 09:00 to 08:59 (or whatever)
   
    return da

def compileDatafromlist(folder, rundate):
    # function interrogates CSV reference file which has list of boreholes and the nearest three qauges
    # tries to get rainfall data for each gauge in turn
    # writes aggregate to CSV for last 28 days

    datestr = rundate.strftime("%d-%m-%Y")

    data = pd.read_csv(reference_filename, converters={'EA Rain Gauge': lambda x: str(x)})
    x = len(data.index)
    a = 0
    print ("An attempt will be made to obtain gauge data for "
           + str(int(x/3)) + " boreholes")
    while a < x:
        # try nearest gauge to borehole
        try:
            gaugeid = data.loc[a,"EA Rain Gauge"]
            boreholename = data.loc[a,"Model Borehole ID"]
            rank = data.loc[a,"Rank"]
            
            rawFilename = (folder + 'RainDataRaw_' + boreholename
                           + '_' + gaugeid + '_' + datestr + '.csv')
            aggFilename = (folder + 'RainDataAggregate_' + boreholename
                           + '_' + gaugeid + '_' + datestr + '.csv')
        
            url = ('http://environment.data.gov.uk/flood-monitoring/id/stations/'
                   + gaugeid + '/readings?_sorted&_limit=10000')
            d = writeTimeSeries(getData(url), rawFilename)
            da = aggregateTimeSeries(d, aggFilename)

            print('Rainfall data for ' +
                  str(boreholename) +
                  ' is from ' + str(gaugeid) +
                  ', Rank ' + str(rank))
            a += 3

        # if error, try next nearest gauge    
        except (IndexError,KeyError):
            try:
                a += 1
                gaugeid = data.loc[a,"EA Rain Gauge"]
                boreholename = data.loc[a,"Model Borehole ID"]
                rank = data.loc[a,"Rank"]
                
                rawFilename = (folder + 'RainDataRaw_' + boreholename
                               + '_' + gaugeid + '_' + datestr + '.csv')
                aggFilename = (folder + 'RainDataAggregate_' + boreholename
                               + '_' + gaugeid + '_' + datestr + '.csv')
        
                url = ('http://environment.data.gov.uk/flood-monitoring/id/stations/'
                       + gaugeid + '/readings?_sorted&_limit=10000')
                d = writeTimeSeries(getData(url), rawFilename)
                da = aggregateTimeSeries(d, aggFilename)

                print('Rainfall data for ' +
                      str(boreholename) + ' is from ' +
                      str(gaugeid) +
                      ', Rank ' + str(rank))
                a += 2

            # if error, try next nearest gauge      
            except (IndexError,KeyError):
                try:
                    a += 1
                    gaugeid = data.loc[a,"EA Rain Gauge"]
                    boreholename = data.loc[a,"Model Borehole ID"]
                    rank = data.loc[a,"Rank"]
                    
                    rawFilename = (folder + 'RainDataRaw_' + boreholename
                                   + '_' + gaugeid + '_' + datestr + '.csv')
                    aggFilename = (folder + 'RainDataAggregate_' + boreholename
                                   + '_' + gaugeid + '_' + datestr + '.csv')
        
                    url = ('http://environment.data.gov.uk/flood-monitoring/id/stations/'
                           + gaugeid + '/readings?_sorted&_limit=10000')
                    d = writeTimeSeries(getData(url), rawFilename)
                    da = aggregateTimeSeries(d, aggFilename)

                    print('Rainfall data for ' +
                          str(boreholename) +
                          ' is from ' + str(gaugeid) +
                          ', Rank ' + str(rank))
                    a += 1
                    
                # if error for all three gauges, quit      
                except (IndexError,KeyError):
                    print('All three gauges provide no data for ' +
                          str(boreholename))
                    a += 1
   
    return

    
def main():

    global reference_filename

    reference_filename = "NearestThreeGauges_v1_ForecastBoreholes.csv"

    print("Connecting...")
    print("Using " + str(reference_filename) + " file as reference")

    folder = ('F:\\GWFForecast\\044 GWF forecasting\\' +
              'EA rainfall gauge API Outputs\\EAgauges_28day_historical\\')
    rundate = date.today()

    compileDatafromlist(folder, rundate)
    
    print("...Done.") 
    
#main()
if __name__ == "__main__":
    sys.exit(main())

