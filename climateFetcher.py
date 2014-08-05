#!/usr/bin/env python2.7
################################################################
#Run: python geocoding filename.csv
#By: Brian Franzone
#E-mail: Franzone89@gmail.com
################################################################
#Summary : Takes a string of specimens' locality information and geocodes the info through Google Maps' API,
#which returns a string of text and the lat/long coordinates. The next step is fetching all the weather
#stations and meta data in New England and adjacent states. Following this, all the geocoded
#specimen lat/longs are placed into a numpy array, and matched with a separate numpy array that is
#comprised of the lat/longs of the weather stations. Using a cKDTree algorithm (quick nearest-
#neighbor lookup), each specimen is matched with its closest weather station using euclidean
#distance. The returns include up to 10 of the closest weather stations, up to a 50 km radius. Iterating
#through each specimen, doing a dictionary lookup of whether the specimen was collected with the
#operation dates of the station. If true, it will make a request to ACIS data services for the climate
#variables. If the day of collection is known, ACIS will return that day's maximum, minimum, average,
#and precipitation (pcpn) variables. If only the month of collection is known, it will return the monthly
#max, min, avg, pncp variables as well as the number of days of missing climate observations
#occurred that month
#Notes
#http://data.rcc-acis.org/StnData?uid=283&sdate=1917-06-01&edate=1917-07-01&elems=1,2,43
#http://data.rcc-acis.org/StnData?sid=068138&sdate=1917-06-01&edate=1917-07-01&elems=1,2,43
#day, maxt[1], mint[2], avgt43[43]
################################################################
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