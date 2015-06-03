from geopy import geocoders
from pandas import merge, DataFrame
import time

def geocoder (df):
  '''Takes a string of location information and geocodes the info into lat/long 
  coordinates and creates a string of locations of each specimen.'''
  #Library for geocoding
  #Change to match the geocoding column
  location_col_name = 'location'
  #removed all rows with *
  df = df[df[location_col_name].str.contains('\*') == 0]

  #Drops on non-unique values off the column called location. Also will drop row(s) only if NaN is in the specific column
  no_location_dup = df[location_col_name].drop_duplicates().dropna(subset=[location_col_name])

  #Returned geocoded data
  geoCodeAddress, geoLat, geoLng, rawLocation = [], [], [], []

  #Iterates through the unique values of the raw_data and appends the data to an array
  for index, location in enumerate(no_location_dup):
    if index == 2498: # change to 2498 if not testing
      print 'Maximum requests reached. Rerun again in 24 hours or change ip address\n'
      break
    else:
      try:
        time.sleep(.50)
        g = geocoders.GoogleV3()
        place, (lat, lng) = g.geocode(location)
        geoCodeAddress.append(place)
        geoLat.append(lat)
        geoLng.append(lng)
        rawLocation.append(location)
        print place #remove then not testing
      except:
        print 'passing on', location
        pass

  #del df['latitude'], df['longitude']

  #Creates a Pandas DataFrame
  geo_geocoded = DataFrame({location_col_name: rawLocation,
    'geocoded': geoCodeAddress,
    'latitude': geoLat,
    'longitude': geoLng})

  #Merges the orginal data and geocoded dataset on the column

  return merge(df, geo_geocoded, on=location_col_name, how='outer')



