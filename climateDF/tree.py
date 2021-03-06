#kd-tree for quick nearest-neighbor lookup
from scipy.spatial import cKDTree
import numpy as np
from climateDF.acisRequest import *
from climateDF.dateConverter import *
from pandas import DataFrame, concat, merge
import argparse

def nearestNeighborsSetup(filename, stateList):
  df_specimens = formatChecker(filename)
  print 'Getting the weather stations'
  with open('input/acis_station_ID.pickle') as f:
      weatherStationsMetaData = cPickle.load(f)

  # weatherStationsMetaData = weatherStations(stateList)
  # weatherStationsMetaData = read_csv('weatherStation/acis_station_ID.csv')
  df_stations = DataFrame.from_dict(weatherStationsMetaData, orient='index', dtype=None)
  '''Loads the lat/long coordinates of the specimens and weather stations into numpy arrays.
  NearestNeighborsResults() will return he number of K (nearest stations) with the index value.
  Then index will be replaced by the UID to match the ASIC data serve.'''
	#Number of points
  np1 = np.array(df_specimens['longitude']).size
  np2 = np.array(df_stations['longitude']).size

  #Search radius
  r = .25

  #Number of nearest stations returned
  k = 10

  d1 = np.empty((np1, 2))
  d2 = np.empty((np2, 2))
  d1[:, 0] = np.array(df_specimens['latitude'])
  d1[:, 1] = np.array(df_specimens['longitude'])

  d2[:, 0] = np.array(df_stations['latitude'])
  d2[:, 1] = np.array(df_stations['longitude'])
 
  result, distance = nearestNeighborsResults(d1.copy(), d2.copy(), r, k)
  columnindex = []
  closestStationList = [nearestNeighborsColumnString(x) for x in range(k)]
  for f in closestStationList: columnindex.append(f()),
  #temp variable for 0-N array
  t1 = np.arange(np2)
  #temp variable for 'uid' ID
  t2 = np.array(df_stations['uid'])
  df_results = DataFrame(result, columns=columnindex)
  #Creates a Pandas DataFrame
  uid_index = DataFrame({'0_closest_weather_station':  t1,
    'uid': t2})

  for index, column_name in enumerate(columnindex):
    temp = uid_index.rename(columns={'0_closest_weather_station': column_name, 'uid': column_name + "s"})
    df_results = df_results.reset_index().merge(temp, how='left', on= column_name, sort=False).sort('index')
    
    if index != 0:
      del df_results['level_0']

    del df_results[column_name]

  del df_results['index']
  df_results = df_results.reset_index()
  return concat([df_specimens, df_results], axis=1), distance, weatherStationsMetaData


#kd-tree for quick nearest-neighbor lookup    
def nearestNeighborsResults(d1, d2, r, k):
  '''runs a cKDTree nearest-neighbor lookup on botanical records against ACIS weather stations '''
  t = cKDTree(d2)
  distance, index = t.query(d1, k=k, eps=1, p=2, distance_upper_bound=r)
  return index, distance

def dis(d1, d2, r, k):
  t = cKDTree(d2)
  distance, index = t.query(d1, k=k, eps=0, p=2, distance_upper_bound=r)
  return distance

#Print columns name X number of closest_weather_station set by K in the function nearest_neighbors_setup
def nearestNeighborsColumnString(x):
  '''Produces a string of headers of X_CWSs... columns as many as K'''
  return lambda : str(x + 1) + '_CWS'
