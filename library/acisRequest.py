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

#Gathering all climate stations for New England and adjacement states from ACIS Data Services. Transforms the json file into dataframe and then return it
def weatherStations():
  '''Requests all the weather stations in New England, dates of operation, and 
  much more meta data. Converts these dates to Julian dates '''
  # Creates a dictionary of all the New England and adjacement states and the metadata assoiated with station 
  input_dict = {"state":["CT","RI", "MA", "RI", "ME", "NY", "VT", "NH"],"elems":["maxt"],"meta":["name","state", "LL", "valid_daterange", "county", "uid", "climdiv", "elev", "sids"]}

  params = urllib.urlencode({'params':json.dumps(input_dict)})
  req = urllib2.Request('http://data.rcc-acis.org/StnMeta', params, {'Accept':'application/json'})
  response = urllib2.urlopen(req)
  hold = response.read()
  data = json.loads(hold)
  df= json_normalize(data['meta'])

  #Spilt of the list of the latitude and longitude into individual columns
  df['latitude'], df['longitude'] = df['ll'].str[-1], df['ll'].str[-0]

  df['sids1'] = df['sids'].str[-0]

  #Temporary holder for valid_daterange
  df['temp_date_list'] = df['valid_daterange'].str[-0]
  #Spilt of the list of valid_daterange into individual columns of startofoperation
  #df['startofoperation'], df['endofoperation'] = df['temp_date_list'].str[-0].astype('datetime64'), df['temp_date_list'].str[-1].astype('datetime64')

  df['startofoperation'], df['endofoperation'] = df['temp_date_list'].str[-0], df['temp_date_list'].str[-1]
  df['Julianstartofoperation'], df['Julianendofoperation'] = stringDateToJulianDate(df['startofoperation']), stringDateToJulianDate(df['endofoperation'])


  #Select only needed fields and rename conunty column to fips_code and and drop all rows with no latitude and longitude
  df = df[['uid', 'state', 'climdiv', 'county', 'name', 'startofoperation', 'endofoperation', 'Julianstartofoperation', 'Julianendofoperation', 'latitude', 'longitude', 'elev', 'sids1']].rename(columns={'county':'fips_code'}).dropna(subset=['latitude', 'longitude'], how='all')

  df.to_csv('output/acis_station_ID.csv', index = False)
  #return the df with all the station info
  return df

def retrieveMlyClimateData(df_dates, start, end):
  '''Within each specimen, it iterates though each of the nearest neighbor weather stations in chronological 
  order until it finds a specimen's collection date within the dates of operation of its nearest neighbor. 
  If "IC" is returned, the date on the record was incomplete. If "NFS" is returned, there were no nearest 
  neighbors found for that record. If "M" is returned, it means there is missing data from ASIC.'''
  date_collected, maxt, maxt_miss, mint, mint_miss, avgT, avgT_miss, pcpn, pcpn_miss, mly_id = [],[],[],[],[],[],[],[],[],[]
  #Loop though the closest weather stations and the date the specimen got collected
  cws_list = cwsList(df_dates)
  count = 0
  for uid, date, jdate, month, closest in zip(df_dates['1_CWSs'], df_dates['date'], df_dates['julian_date'], df_dates['month'], cws_list):
    for index, z in enumerate(closest):
      if month == 0 or month == 00:
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
      elif start.get(z) < jdate and jdate < end.get(z):
        #Json request question
        input_dict = {'uid': z, 'sdate':date[0:len(date) -2], 'edate':date[0:len(date) -2], 'elems':
        [{'name':'maxt','interval':'mly','duration':'mly','reduce':{'reduce':'max','add':'mcnt'}}, 
        {'name':'mint','interval':'mly','duration':'mly','reduce':{'reduce':'min','add':'mcnt'}}, 
        {'name':'avgt','interval':'mly','duration':'mly','reduce':{'reduce':'mean','add':'mcnt'}},
        {'name':'pcpn','interval':'mly','duration':'mly','reduce':{'reduce':'sum','add':'mcnt'}},]}
        #The function to make call the ACIS
        data = callASIC(input_dict)
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


def retrieveDlyClimateData(df_dates, start, end):
  '''Within each specimen, it iterates though each of the nearest neighbor weather stations in chronological
  order until it finds a specimen's collection date within the dates of operation of its nearest neighbor.
  If "IC" is returned, the date on the record was incomplete. If "NFS" is returned, there were no nearest 
  neighbors found for that record. If "M" is returned, it means there is missing data from ASIC.'''
  date_collected, maxt, mint, avgT, pcpn, dly_id = [], [], [], [], [], []
  #Loop though the closest weather stations and the date the specimen got collected
  cws_list = cwsList(df_dates)
  count = 0
  for uid, date, jdate, day, closest in zip(df_dates['1_CWSs'], df_dates['date'], df_dates['julian_date'], df_dates['day'], cws_list):
    for index, z in enumerate(closest):
      #print index,  "\t", start.get(z), "\t", jdate, "\t", end.get(z), "\t", close, "\t", z
      if day == 0 or day == '' or day == 00:
        date_collected.append('IC')
        maxt.append('IC')
        mint.append('IC')
        avgT.append('IC')
        pcpn.append('IC')
        dly_id.append(z)
        break
      elif start.get(z) < jdate and jdate < end.get(z):
        #Json request question
        input_dict = {'uid': z, 'sdate':date, 'edate':date, 'elems':[{'name':'maxt', 'duration':'dly'}, 
        {'name':'mint', 'duration':'dly'}, {'name':'avgt', 'duration':'dly'}, {'name':'pcpn', 'duration':'dly'}]}
        #The function to make call the ACIS
        data = callASIC(input_dict)

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
  params = urllib.urlencode({'params':json.dumps(input_dict)})
  req = urllib2.Request('http://data.rcc-acis.org/StnData', params, {'Accept':'application/json'})
  response = urllib2.urlopen(req)
  hold = response.read()
  data = json.loads(hold)
  return data


#concatenate the daily and monthly climate data with the specimen record 
#NEED TO ADD a RETURN
def concatenateDlyAndMly(dly, mly, df_dates):
  '''Concatenate the daily and monthly results to the original dataset.'''
  dly_mly_specimen_df = concat([df_dates, dly, mly], axis=1)
  #print dly_mly_specimen_df.info()
  #dly_mly_specimen_df.to_csv('output/see.csv', index = False)
  return dly_mly_specimen_df


def cwsList(df):
  '''create a list of nearest neighbors weather stations to make it easier to iterate through the list.'''
  df = df[['1_CWSs', '2_CWSs', '3_CWSs', '4_CWSs', '5_CWSs', '6_CWSs', '7_CWSs', '8_CWSs', '9_CWSs', '10_CWSs']]
  df = map(list, df.values)
  return df