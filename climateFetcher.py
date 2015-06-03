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
#from climateDF.googleGeocoder import *
from climateDF.cKDTree import *
import glob



def main ():
    familyNotDone = glob.glob('input/*.csv')
    for family in familyNotDone:
        familyDone = glob.glob('output/*.csv')
        familyCome = [x.split('/')[1].split('_')[0] + '.csv' for x in familyDone]

        familyName = family.split('/')[1].split('_')[0] + family[-4:]
        if familyName in familyCome:
            print 'Completed climate fetchering for file', familyName
            continue
        stateList = ["CT","RI", "MA", "ME", "NY", "VT", "NH"]
        nearestN, distance, weatherStationsMetaData = nearestNeighborsSetup(filename=family, stateList = stateList)

        daily = retDailyData(nearestStations = nearestN, stationDates = weatherStationsMetaData)
        monthly = retMonthlyData(nearestStations = nearestN, stationDates = weatherStationsMetaData)

        dailyMonthlyResult = concatenateDlyAndMly(daily = daily, monthly = monthly, nearestStations = nearestN)
        pathName = 'output/' + familyName[:-4] + '_Daily_Monthly.csv'
        dailyMonthlyResult.to_csv(pathName, index = False)

        yearlyResult = retYearlyData(nearestStations = nearestN, stationDates = weatherStationsMetaData, distance = distance)
        pathName = 'output/' + familyName[:-4] + '_Yearly.csv'
        yearlyResult.to_csv(pathName, index = False)

if __name__ == '__main__':
  main()
