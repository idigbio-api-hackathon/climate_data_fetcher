#!/usr/bin/env python2.7
#Purpose: To geocode only unique values and merge back to all repetitive occurrences of the same value to minimizes server calls
		#Then return the top K nearest weather station to the geocode specimens
#Currently using google and can only make 2500 calls a day
#Run: python geocoding filename.csv
#By: Brian Franzone
#E-mail: Franzone89@gmail.com
#Use this one
#http://data.rcc-acis.org/StnData?uid=283&sdate=1917-06-01&edate=1917-07-01&elems=1,2,43
#http://data.rcc-acis.org/StnData?sid=068138&sdate=1917-06-01&edate=1917-07-01&elems=1,2,43
#day, maxt, mint, avgt43


#fundamental package for scientific computing
from library.googleGeocoder import *
from library.dateConverter import *
from library.cKDTree import *
from library.acisRequest import *
from library.errorCheck import *


def main ():
    df = errorchecking()
    print 'Gathering all climate stations for New England and adjacement states from ACIS Data Services\n'
    #weather_stations_call = weatherStations()
    weather_stations_call = read_csv('output/acis_station_ID.csv')

    start_date = weather_stations_call.set_index('uid')['Julianstartofoperation'].to_dict()
    end_date = weather_stations_call.set_index('uid')['Julianendofoperation'].to_dict()

    print 'Starting geocoding:\n'
    geocodedata = joinDate(geocoder(df))

    print 'Starting the cKDTree for the nearest-neighbor lookup\n'
    #Exports the orginal data into the output folder with concatenation of georeferened data with the top N nearest weather unique ID number 
    nearestN = nearestNeighborsSetup(geocodedata, weather_stations_call)

    print 'Starting to gather abotic variable for each specimen:\n'
    result = concatenateDlyAndMly(retrieveDlyClimateData(nearestN, start_date, end_date), retrieveMlyClimateData(nearestN, start_date, end_date), nearestN)

    print "\nAll files exported to the output folder"

    print result.info()
    result.to_csv('output/specimen_results.csv', index = False)


if __name__ == '__main__':
  main()