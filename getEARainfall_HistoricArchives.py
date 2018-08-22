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
#from dateutil.relativedelta import relativedelta
import pandas as pd
import pprint
import sys
##import iso8601
from dateutil.parser import parse

from io import BytesIO

def getData(url):
    response=BytesIO()    
    f = urllib.request.urlopen(url)
    response = f.read()
    strResponse = response.decode('utf-8')    # json.loads object must be a string, not bytes
    dicResponse = json.loads(strResponse)    

    return dicResponse
       

def compileDatafromlist():

    # uses a reference file which associates borehole with nearest rain gauge
    data = pd.read_csv("NearestThreeGauges_v1_ForecastBoreholes.csv", converters={'EA Rain Gauge': lambda x: str(x)})
    # just uses the nearest gauge (Rank = 1) # in future improvements, can check lower ranks when data at rank 1 is incomplete.
    datacut = data[(data['Rank'] == 1)]
    datacut = datacut.copy() # redundant?
    datacut = datacut.reset_index(drop=True)
    
    x = len(datacut.index)
    a = 0

    # loops through all the boreholes in the Nearest Three Gauges file
    print("Historic rain gauge data will be obtained for " + str(x) + " boreholes")
    while a < x:

        gaugeID = datacut.loc[a,"EA Rain Gauge"]
        pointID = datacut.loc[a,"Model Borehole ID"]

        print("Fetching historic rainfall data for gauge " + str(gaugeID) + " which is nearest to borehole " + str(pointID))

        # run the getHistoricQuick function for each borehole
        getHistoricQuick(gaugeID, pointID)

        a += 1

  
    return


def getHistoricQuick(gaugeID, pointID):
    
# A more efficient way to do this would be to obtain the csv file for the relevant date once, and then
# query it for relevant gauges, rather than obtain the csv file 'from scratch' for each gauge

    # archive data available from two days ago to one year ago
    # yesterday's data is not always available so needs to start two days ago
    # today = datetime.now()
    today = datetime.now() # this was run on feb 5 2018 - if want data from early feb 2017 (beginning of data availability)
    # regardless of when the script is run
    # need to edit the 'one_year_ago' variable
    # however, it does seem that only one year of historical data is kept available in archives on EA server
    one_day_ago = datetime.now() - timedelta(days=1) # no point using relativedelta (extra package required)
    one_yr_ago = datetime.now() - timedelta(days=366)
    delta = timedelta(days=1)

    ## since this list of dates is common to all boreholes, does not need to go into this function,
    ## but rather can go into main program below.
    
    # loop to generate all days from two days ago to one year ago 
    e = []
    while one_yr_ago < one_day_ago:
        e.append(one_yr_ago.strftime("%Y-%m-%d"))
        one_yr_ago += delta

    # for each day, get the historic data, filter by station ref and parameter, then keep just columns we need
    # there is a except to catch the case where there is no data from the EA, in which case an error report is written
    historic_data = pd.DataFrame([])
    no_data_record = pd.DataFrame([])
    for x in e:
        try:
            url = 'http://environment.data.gov.uk/flood-monitoring/archive/readings-' + x + '.csv'
            data = pd.read_csv(url, low_memory=False)
            # all stations near our boreholes have the identifier below (tipping bucket rain gauge with 15 min sampling period)
            # other possible identifiers for rainfall measurements (but not relevant to our current forecast boreholes) are:
            #'-rainfall-tipping_bucket_raingauge-t-1_h-mm', '-rainfall-water-t-15_min-mm', '-rainfall-2-t-15_min-mm'
            identifier = 'http://environment.data.gov.uk/flood-monitoring/id/measures/' + gaugeID + '-rainfall-tipping_bucket_raingauge-t-15_min-mm'
            datacut = data[(data['measure'] == identifier)]
            datacut = datacut.copy() # not sure why copy?
            datacut.loc[:,('value')] = datacut.loc[:,('value')].astype(float)
            dailytotal = datacut.value.sum()
            completeness = (datacut.shape[0]) * 1.04167
            completeness = round(completeness,0)
        
            print("Found data for gauge " + str(pointID) + " on " + str(x) + " (completeness is " + str(completeness) + ")")

            df_day = pd.DataFrame([[x,dailytotal,completeness]], columns=['date','rainfall','completeness'])
            historic_data = historic_data.append(df_day, ignore_index=True)

                # try, except needs improvement: any error above produces the 'No data' message, and when there is actually
                # no data (eg. rudston north), you get an empty datafram, and completeness of 0.0, but not the 'no data' message
        except: 
            print("NO DATA for gauge " + str(pointID) + " on " + str(x))
            nd_day = pd.DataFrame([[gaugeID,pointID,x]], columns=['EA Rain Gauge','Borehole','date'])
            no_data_record = no_data_record.append(nd_day, ignore_index=True)

    folder = 'F:\\GWFForecast\\044 GWF forecasting\\EA rainfall gauge API Outputs\\EAgauges_forecastboreholes_Feb2017Jan2018\\'
        
    # write rainfall data file to CSV
    if len(historic_data) > 0:
        filename = folder + "RainDataHistoric_" + pointID + "_" + gaugeID + ".csv"
        historic_data.to_csv(filename, sep=',', index=False)

    # write list of dates where there is no file to CSV
    if len(no_data_record) > 0:
        days_no_data = len(no_data_record)
        print("There are " + str(days_no_data) + " days when no data was obtained - please see no_data_record.csv file")
        filename2 = folder + "NoRainData_" + pointID + "_" + gaugeID + ".csv"
        no_data_record.to_csv(filename2, sep=',', index=False)

    return 

   
def main():

    compileDatafromlist()

    
#main()
if __name__ == "__main__":
    sys.exit(main())

