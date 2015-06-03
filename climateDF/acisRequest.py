import urllib, urllib2, sys
from urllib2 import Request, urlopen
from pandas.io.json import json_normalize
#Library for data structures and data analysis
from pandas import read_csv, DataFrame, concat
from dateConverter import *
import time
try :
  import json
except ImportError :
  import simplejson as json
import math

#Gathering all climate stations for New England and adjacement states from ACIS Data Services. Transforms the json file into dataframe and then return it
def weatherStations(stateList):
  '''Requests all the weather stations in New England, dates of operation, and 
  much more meta data. Converts these dates to Julian dates '''
  # Creates a dictionary of all the New England and adjacement states and the metadata assoiated with station 
  # input_dict = {"state":["CT","RI", "MA", "ME", "NY", "VT", "NH"],"elems":["maxt"],"meta":["name","state", "LL", "valid_daterange", "county", "uid", "climdiv", "elev", "sids"]}
  # input_dict = {"state":["FL", "AL", "GA"],"elems":["maxt"],"meta":["name","state", "LL", "valid_daterange", "county", "uid", "climdiv", "elev", "sids"]}
  input_dict = {"state":stateList,"elems":["maxt"],"meta":["name","state", "LL", "valid_daterange", "county", "uid", "climdiv", "elev", "sids"]}

  params = urllib.urlencode({'params':json.dumps(input_dict)})
  req = urllib2.Request('http://data.rcc-acis.org/StnMeta', params, {'Accept':'application/json'})
  response = urllib2.urlopen(req)
  hold = response.read()
  data = json.loads(hold)


  weatherDic = dict()
  delValuesDic = []
  for lenght in xrange(0, len(data['meta'])):
    stationUID = data['meta'][lenght]['uid']
    weatherDic.setdefault(stationUID, {
      })
    for key, value in data['meta'][lenght].iteritems():
      try:
        if data['meta'][lenght]['ll'] is None:
          print ''
      except:
        delValuesDic.append(stationUID)
      if key == 'll':
        latLong =['longitude', 'latitude']
        for title, count in zip(latLong, xrange(0,2)):
          keyValue = data['meta'][lenght][key][count] if data['meta'][lenght][key][count] is not None else ''
          weatherDic[stationUID].update({title:keyValue})
        continue
      if key == 'valid_daterange':
        Datesrange = ['startofoperation', 'endofoperation']
        for title, count in zip(Datesrange, xrange(0,2)):
          keyValue = data['meta'][lenght][key][0][count] if data['meta'][lenght][key][0][count] is not None else '' 
          weatherDic[stationUID].update({title:keyValue})

          if title == 'startofoperation':
            weatherDic[stationUID].update({'Julianstartofoperation':stringDateToJulianDate(keyValue)})
          if title == 'endofoperation':
            weatherDic[stationUID].update({'Julianendofoperation':stringDateToJulianDate(keyValue)})
        continue

      keyValue = data['meta'][lenght][key] if data['meta'][lenght][key] is not None else ''
      weatherDic[stationUID].update({key:keyValue})

  for i in set(delValuesDic):
    del weatherDic[i]

  # df = DataFrame.from_dict(weatherDic, orient='index', dtype=None)
  # df = df[['uid', 'state', 'climdiv', 'county', 'name', 'startofoperation', 'endofoperation', 'Julianstartofoperation', 'Julianendofoperation', 'latitude', 'longitude', 'elev', 'sids']]
  # df.to_csv('weatherStation/acis_station_ID.csv', index = False)
  
  return weatherDic
  # return df


def retMonthlyData(nearestStations, stationDates):
  '''Within each specimen, it iterates though each of the nearest neighbor weather stations in chronological 
  order until it finds a specimen's collection date within the dates of operation of its nearest neighbor. 
  If "IC" is returned, the date on the record was incomplete. If "NFS" is returned, there were no nearest 
  neighbors found for that record. If "M" is returned, it means there is missing data from ASIC.'''
  date_collected, maxt, maxt_miss, mint, mint_miss, avgT, avgT_miss, pcpn, pcpn_miss, mly_id = [],[],[],[],[],[],[],[],[],[]
  #Loop though the closest weather stations and the date the specimen got collected
  cws_list = cwsList(nearestStations)
  for uid, date, jdate, month, closest in zip(nearestStations['1_CWSs'], nearestStations['concatDate'], nearestStations['julianDate'], nearestStations['month'], cws_list):
    for index, z in enumerate(closest):
      if math.isnan(z):
        continue
      if month == 0 or month == 00:
        print 'Mly IC', uid, date
        date_collected.append('IC')
        maxt.append('IC')
        maxt_miss.append('IC')
        mint.append('IC')
        mint_miss.append('IC')
        avgT.append('IC')
        avgT_miss.append('IC')
        pcpn.append('IC')
        pcpn_miss.append('IC')
        mly_id.append('IC')
        break
      #print index,  "\t", start.get(z), "\t", jdate, "\t", end.get(z), "\t", close, "\t", z
      elif stationDates[z]['Julianstartofoperation'] < jdate and jdate < stationDates[z]['Julianendofoperation']:
        print 'Mly request', uid, date
        # print str(int(date.split('-')[0]) - 1) + '-' + date.split('-')[1], '-'.join(date.split('-')[0:2]), 'This is the new date', date
        #Json request question
        input_dict = {'uid': z, 'sdate':date[0:len(date) -2], 'edate':date[0:len(date) -2], 'elems':
        [{'name':'maxt','interval':'mly','duration':'mly','reduce':{'reduce':'max','add':'mcnt'}}, 
        {'name':'mint','interval':'mly','duration':'mly','reduce':{'reduce':'min','add':'mcnt'}}, 
        {'name':'avgt','interval':'mly','duration':'mly','reduce':{'reduce':'mean','add':'mcnt'}},
        {'name':'pcpn','interval':'mly','duration':'mly','reduce':{'reduce':'sum','add':'mcnt'}},]}
        #The function to make call the ACIS
        data = callASIC(input_dict)
        if data is None:
          date_collected.append('NSF')
          maxt.append('NSF')
          mint.append('NSF')
          avgT.append('NSF')
          pcpn.append('NSF')
          mly_id.append('NSF')
          break
        #Loop over the data field and append maxt, mint, avgt, pcpn to an array
        for result in data['data']:
          date_collected.append(result[0])
          mly_id.append(z)
          for index, (maxtemp, mintemp, avgtemp, pcpnfall)  in  enumerate(zip(result[1], result[2], result[3], result[4])):
            if index  == 0:
              maxt.append(maxtemp)
              mint.append(mintemp)
              avgT.append(avgtemp)
              pcpn.append(pcpnfall)
            else:
              maxt_miss.append(maxtemp)
              mint_miss.append(mintemp)
              avgT_miss.append(avgtemp)
              pcpn_miss.append(pcpnfall)
        break
      elif index == 9:
        print 'Mly NFS', uid, date
        date_collected.append('NSF')
        maxt.append('NSF')
        maxt_miss.append('NSF')
        mint.append('NSF')
        mint_miss.append('NSF')
        avgT.append('NSF')
        avgT_miss.append('NSF')
        pcpn.append('NSF')
        pcpn_miss.append('NSF')
        mly_id.append('NSF')
        break
      else:
        continue
  #Transfer json file into flat dataframe
  df = DataFrame([date_collected,maxt, maxt_miss, mint, mint_miss, avgT, avgT_miss, pcpn, pcpn_miss, mly_id]).T
  #Rename number columns to name fields
  df = df.rename(columns={0:'mly_date', 1: 'mly_maxt', 2: 'mly_maxt_nmppm', 3: 'mly_mint', 4: 'mly_mint_nmppm', 5:'mly_avgt', 6:'mly_avgt_nmppm', 7:'mly_pcpn', 8: 'mly_pcpn_nmppm', 9:'mly_id_use'})
  return df

#Web services calls to ACIS for daily:
# maximum value temp(F), 
# minimum value temp, 
# average temperature,
# precipitation (inches)  


        # print weather_stations_call[16383]['Julianstartofoperation']
        # print weather_stations_call[16383]['Julianendofoperation']

def retYearlyData(nearestStations, stationDates, distance):
  '''Within each specimen, it iterates though each of the nearest neighbor weather stations in chronological 
  order until it finds a specimen's collection date within the dates of operation of its nearest neighbor. 
  If "IC" is returned, the date on the record was incomplete. If "NFS" is returned, there were no nearest 
  neighbors found for that record. If "M" is returned, it means there is missing data from ASIC.'''
  date_collected, maxt, maxt_miss, mint, mint_miss, avgT, avgT_miss, pcpn, pcpn_miss, mly_id, iDigBio, yearCol, monthCol, dis = [],[],[],[],[],[],[],[],[],[],[],[],[],[]
  #Loop though the closest weather stations and the date the specimen got collected
  cws_list = cwsList(nearestStations)
  countArray = -1
  for uid, date, jdate, month, closest, iDigBioUUID in zip(nearestStations['1_CWSs'], nearestStations['concatDate'], nearestStations['julianDate'], nearestStations['month'], cws_list, nearestStations['url']):
    countArray += 1
    countIndex = 0
    for index, z in enumerate(closest):
      if math.isnan(z):
        continue
      if month == 0 or month == 00:
        print 'Mly IC', uid, date
        date_collected.append('IC')
        maxt.append('IC')
        maxt_miss.append('IC')
        mint.append('IC')
        mint_miss.append('IC')
        avgT.append('IC')
        avgT_miss.append('IC')
        pcpn.append('IC')
        pcpn_miss.append('IC')
        mly_id.append('IC')
        iDigBio.append(iDigBioUUID)
        yearCol.append('IC')
        monthCol.append('IC')
        dis.append('IC')
        countIndex +=1
        break
      #print index,  "\t", start.get(z), "\t", jdate, "\t", end.get(z), "\t", close, "\t", z

      elif stationDates[z]['Julianstartofoperation'] < jdate and jdate < stationDates[z]['Julianendofoperation']:
        print 'Yly request', uid, date

        ###########################floating date#######################
        # startMonth = int(date.split('-')[1]) + 1
        # if startMonth > 12:
        #   startMonth = 1
        # input_dict = {'uid': z, 'sdate': str(int(date.split('-')[0]) - 1) + '-' + str(startMonth), 'edate':'-'.join(date.split('-')[0:2]), 'elems':
        ################################################################
        sdate = str(int(date.split('-')[0])) + '-01'
        edate = str(int(date.split('-')[0])) + '-12'

        # print 'Mly data', uid, date
        #Json request question
        input_dict = {'uid': z, 'sdate': sdate, 'edate':edate, 'elems':
        [{'name':'maxt','interval':'mly','duration':'mly','reduce':{'reduce':'max','add':'mcnt'}}, 
        {'name':'mint','interval':'mly','duration':'mly','reduce':{'reduce':'min','add':'mcnt'}}, 
        {'name':'avgt','interval':'mly','duration':'mly','reduce':{'reduce':'mean','add':'mcnt'}},
        {'name':'pcpn','interval':'mly','duration':'mly','reduce':{'reduce':'sum','add':'mcnt'}},]}

        # input_dict = {'uid': z, 'sdate': str(int(date.split('-')[0]) - 1) + '-' + date.split('-')[1], 'edate':'-'.join(date.split('-')[0:2], 'elems':
        # [{'name':'maxt','interval':'mly','duration':'mly','reduce':{'reduce':'max','add':'mcnt'}}, 
        # {'name':'mint','interval':'mly','duration':'mly','reduce':{'reduce':'min','add':'mcnt'}}, 
        # {'name':'avgt','interval':'mly','duration':'mly','reduce':{'reduce':'mean','add':'mcnt'}},
        # {'name':'pcpn','interval':'mly','duration':'mly','reduce':{'reduce':'sum','add':'mcnt'}},]}

        # input_dict = {uid: z, 
        # sdate: str(int(date.split('-')[0]) - 1) + '-' + date.split('-')[1],
        # edate: '-'.join(date.split('-')[0:2],
        # elems: "mly_max_maxt,mly_min_mint",
        # }
        #The function to make call the ACIS
        data = callASIC(input_dict)

        if data is None:
          date_collected.append('NSF')
          maxt.append('NSF')
          mint.append('NSF')
          avgT.append('NSF')
          pcpn.append('NSF')
          mly_id.append('NSF')
          iDigBio.append(iDigBioUUID)
          yearCol.append('NSF')
          monthCol.append('NSF')
          dis.append('NSF')
          countIndex +=1
          break
        #Loop over the data field and append maxt, mint, avgt, pcpn to an array
        for result in data['data']:
          date_collected.append(result[0])
          yearCol.append(result[0].split('-')[0])
          monthCol.append(result[0].split('-')[1])
          # print z
          mly_id.append(z)
          iDigBio.append(iDigBioUUID)
          distanceKM = '%.3f' % (distance[countArray][countIndex] * 100)
          dis.append(distanceKM)
          for index, (maxtemp, mintemp, avgtemp, pcpnfall)  in  enumerate(zip(result[1], result[2], result[3], result[4])):
            if index  == 0:
              maxt.append(maxtemp)
              mint.append(mintemp)
              avgT.append(avgtemp)
              pcpn.append(pcpnfall)
            else:
              maxt_miss.append(maxtemp)
              mint_miss.append(mintemp)
              avgT_miss.append(avgtemp)
              pcpn_miss.append(pcpnfall)
        countIndex +=1
        break
      elif index == 9:
        # print 'Mly NFS', uid, date
        date_collected.append('NSF')
        maxt.append('NSF')
        maxt_miss.append('NSF')
        mint.append('NSF')
        mint_miss.append('NSF')
        avgT.append('NSF')
        avgT_miss.append('NSF')
        pcpn.append('NSF')
        pcpn_miss.append('NSF')
        mly_id.append('NSF')
        iDigBio.append(iDigBioUUID)
        yearCol.append('NSF')
        monthCol.append('NSF')
        dis.append('NSF')
        countIndex +=1
        break
      else:
        countIndex +=1
        continue
  #Transfer json file into flat dataframe
  df = DataFrame([date_collected,maxt, maxt_miss, mint, mint_miss, avgT, avgT_miss, pcpn, pcpn_miss, mly_id, iDigBio, yearCol, monthCol, dis]).T
  #Rename number columns to name fields
  df = df.rename(columns={0:'mly_date', 1: 'mly_maxt', 2: 'mly_maxt_nmppm', 3: 'mly_mint', 4: 'mly_mint_nmppm', 5:'mly_avgt', 6:'mly_avgt_nmppm', 7:'mly_pcpn', 8: 'mly_pcpn_nmppm', 9:'uid_mly_used', 10:'iDigBioUUID', 11:'yearM', 12:'monthM', 13:'distance'})
  return df

def retDailyData(nearestStations, stationDates):
  '''Within each specimen, it iterates though each of the nearest neighbor weather stations in chronological
  order until it finds a specimen's collection date within the dates of operation of its nearest neighbor.
  If "IC" is returned, the date on the record was incomplete. If "NFS" is returned, there were no nearest 
  neighbors found for that record. If "M" is returned, it means there is missing data from ASIC.'''
  date_collected, maxt, mint, avgT, pcpn, dly_id = [], [], [], [], [], []
  #Loop though the closest weather stations and the date the specimen got collected
  cws_list = cwsList(nearestStations)
  count = 0
  for uid, date, jdate, day, closest in zip(nearestStations['1_CWSs'], nearestStations['concatDate'], nearestStations['julianDate'], nearestStations['day'], cws_list):
    for index, z in enumerate(closest):
      if math.isnan(z):
        continue
      #print index,  "\t", start.get(z), "\t", jdate, "\t", end.get(z), "\t", close, "\t", z
      if day == 0 or day == '' or day == 00:
        print 'Dly IC', uid, date
        date_collected.append('IC')
        maxt.append('IC')
        mint.append('IC')
        avgT.append('IC')
        pcpn.append('IC')
        dly_id.append(z)
        break
      elif stationDates[z]['Julianstartofoperation'] < jdate and jdate < stationDates[z]['Julianendofoperation']:
        print 'Dly request', uid, date
        #Json request question
        input_dict = {'uid': z, 'sdate':date, 'edate':date, 'elems':[{'name':'maxt', 'duration':'dly'}, 
        {'name':'mint', 'duration':'dly'}, {'name':'avgt', 'duration':'dly'}, {'name':'pcpn', 'duration':'dly'}]}
        #The function to make call the ACIS
        data = callASIC(input_dict)
        if data is None:
          date_collected.append('NSF')
          maxt.append('NSF')
          mint.append('NSF')
          avgT.append('NSF')
          pcpn.append('NSF')
          dly_id.append('NSF')
          break

        #Loop over the data field and append maxt, mint, avgt, pcpn to an array
        for result in data['data']:
          date_collected.append(result[0])
          maxt.append(result[1])
          mint.append(result[2])
          avgT.append(result[3])
          pcpn.append(result[4])
          dly_id.append(z)
        break
      elif index == 9:
        date_collected.append('NSF')
        maxt.append('NSF')
        mint.append('NSF')
        avgT.append('NSF')
        pcpn.append('NSF')
        dly_id.append('NSF')
        break
      else:
        continue
  df = DataFrame([date_collected,maxt,mint, avgT, pcpn, dly_id]).T
  df = df.rename(columns={0:'dly_date', 1: 'dly_maxt', 2: 'dly_mint', 3:'dly_avgt', 4:'dly_pcpn', 5:'dly_id_use'})
  #df['dlyJulianDate'] = stringDateToJulianDate(df["dly_Date"])
  return df

#Transfer json file into flat dataframe
def callASIC(input_dict):
  '''Makes the request to ACIS data services requesting all the metadata of the nearest neighbors weather station.'''
  try:
    params = urllib.urlencode({'params':json.dumps(input_dict)})
    req = urllib2.Request('http://data.rcc-acis.org/StnData', params, {'Accept':'application/json'})
    response = urllib2.urlopen(req)
    hold = response.read()
    data = json.loads(hold)
    return data
  except:
    return None


#concatenate the daily and monthly climate data with the specimen record 
#NEED TO ADD a RETURN
def concatenateDlyAndMly(daily, monthly, nearestStations):
  '''Concatenate the daily and monthly results to the original dataset.'''
  dly_mly_specimen_df = concat([nearestStations, daily, monthly], axis=1)
  #print dly_mly_specimen_df.info()
  #dly_mly_specimen_df.to_csv('output/see.csv', index = False)
  return dly_mly_specimen_df


def cwsList(df):
  '''create a list of nearest neighbors weather stations to make it easier to iterate through the list.'''
  df = df[['1_CWSs', '2_CWSs', '3_CWSs', '4_CWSs', '5_CWSs', '6_CWSs', '7_CWSs', '8_CWSs', '9_CWSs', '10_CWSs']]
  df = map(list, df.values)
  return df