import cPickle, json, urllib, urllib2, sys, time, re
import datetime
import glob
import csv as c
#Library for data structures and data analysis
from pandas import read_csv, DataFrame, concat
# from pandas.io.json import json_normalize
from fuzzywuzzy import process

from bs4 import BeautifulSoup



#Gathering all climate stations for New England and adjacement states from ACIS Data Services. Transforms the json file into dataframe and then return it
def main():
  iDigBio_New_England_Dic = dict()
  '''Requests all the weather stations in New England, dates of operation, and 
  much more meta data. Converts these dates to Julian dates '''
  # Creates a dictionary of all the New England and adjacement states and the metadata assoiated with station 
  with open('family_state_county.pickle') as f:
        familyDictionary = cPickle.load(f)
  # completeFiles = completedJobs()
  for index, (family, value) in enumerate(familyDictionary.iteritems()):
    iDigBio_New_England_Dic.clear()
    for index2, (state, value1) in enumerate(value.iteritems()):
      input_dict = {"geopoint":{"type":"exists"},"scientificname":{"type":"exists"}, "family":family,"country":"united states"}
      params = urllib.urlencode({'':json.dumps(input_dict)})
      url = 'https://beta-search.idigbio.org/v2/search/records?rq' + params.replace(" ", '') + '&limit=3000'
      try:
		req = urllib2.Request(url)
		response = urllib2.urlopen(req)
		hold = response.read()
		data = json.loads(hold)
      except:
      	print 'FAIL on 41', family
        continue
      maxCount = 3000
      if data['itemCount'] < maxCount:
        maxCount = data['itemCount']
      for index in xrange(0, maxCount):
        day = dataOrNull(data['items'][index]['data'], 'dwc:day')
        month = dataOrNull(data['items'][index]['data'], 'dwc:month')
        year = dataOrNull(data['items'][index]['data'], 'dwc:year')
        if day is None or month is None or year is None:
          continue
        if day is not None and month is not None and year is not None:
          correctedDate, julianDate = validDateToJulianDate(day + '-' + month + '-' + year)
        else:
          correctedDate, julianDate = None, None
        stateName = cleanStateName(str(state))
        if stateName == False:
          continue
        collectionCode = dataOrNull(data['items'][index]['data'], 'dwc:collectionCode')
        catalogNumber = dataOrNull(data['items'][index]['data'], 'dwc:catalogNumber')
        iDigBioUUID = str(data['items'][index]['uuid'])
        print collectionCode, catalogNumber, stateName, family
        print 'slow'
        iDigBio_New_England_Dic.setdefault(iDigBioUUID, {
            'iDigBioUUID': iDigBioUUID,
            'iPlantGUID' : iPlantGUID,
            'etag': data['items'][index]['etag'],
            'collectionCode': collectionCode,
            'hasImage': data['items'][index]['indexTerms']['hasImage'],
            'order': dataOrNull(data['items'][index]['data'], 'dwc:order'),
            'family': familyTitle(data['items'][index]['data'], 'dwc:family'),
            'scientificName': data['items'][index]['data'], 'dwc:scientificName',
            'scientificNameAuthorship': dataOrNull(data['items'][index]['data'], 'dwc:scientificNameAuthorship'),
            'genus': dataOrNull(data['items'][index]['data'], 'dwc:genus'),
            'specificEpithet': dataOrNull(data['items'][index]['data'], 'dwc:specificEpithet'),
            'higherGeography': dataOrNull(data['items'][index]['data'], 'dwc:higherGeography'),
            'county': removeCounty(data['items'][index]['data'], 'dwc:county'),
            'stateProvince': stateName,
            'country': dataOrNull(data['items'][index]['data'], 'dwc:country'),
            'municipality': removeCounty(data['items'][index]['data'], 'dwc:municipality'),
            'locality': dataOrNull(data['items'][index]['data'], 'dwc:locality'),
            'decimalLatitude': data['items'][index]['data']['dwc:decimalLatitude'],
            'decimalLongitude': data['items'][index]['data']['dwc:decimalLongitude'],
            'day': day,
            'month': month,
            'year': year,
            'location': joinState(data, index, stateName),
            'date': correctedDate,
            'julianDate': julianDate,
            'recordedBy': dataOrNull(data['items'][index]['data'], 'dwc:recordedBy'),
            'occurrenceID': dataOrNull(data['items'][index]['data'], 'dwc:occurrenceID'),
            'catalogNumber': catalogNumber,
            'modified': dataOrNull(data['items'][index]['data'], 'dcterms:modified'),
            'downloadDate': datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
            })



  # for key, value in iDigBio_New_England_Dic.iteritems():
  #   print key, value['genus'], value['specificEpithet']
  # print len(iDigBio_New_England_Dic)
    try:
	    df = DataFrame.from_dict(iDigBio_New_England_Dic, orient='index', dtype=None)
	    df = df.dropna(subset=[ 'julianDate', 'stateProvince'], how='any')
	    df = df[['iDigBioUUID','collectionCode','catalogNumber', 'family','scientificName','genus','specificEpithet','location','month','day','year', 'date', 'julianDate']]
	    fileName = 'family_iDigBio_Output/' + family + '_idgibio_.csv'
	    df.to_csv(fileName, index = False)
	    print df.info()
    except:
        pass

def joinState(data, index, stateName):
  location = ''
  country = dataOrNull(data['items'][index]['data'], 'dwc:country')
  if country.lower() == 'usa':
    country = 'United States of America'
  if country is not None:
    location += country + ', '
  if stateName is not None:
    location += stateName + ', ' 
  county = removeCounty(data['items'][index]['data'], 'dwc:county')
  if county is not None:
    location += county + ' County' + ', '
  municipality = removeCounty(data['items'][index]['data'], 'dwc:municipality')
  if municipality is not None:
    location += municipality + ', '
  if location is None:
    return None
  location = location.strip()[:-1]
  return location
def completedJobs():
	filenames = glob.glob('family_iDigBio_Output/*.csv')
	completedJobs = []
	for filename in filenames:
		temp = filename.split('/')[1]
		completedJobs.append(temp.split('_')[0])
	return completedJobs

def familyTitle(data, name):
  try:
    return str(data[name]).title()
  except:
    return None

def tryOrFail(data, name):
  try:
    return str(data[name])
  except:
    return None

def removeCounty(data, name):
  try:
    return str(data[name]).replace('County', '').strip().title()
  except:
    return None

def dataOrNull(data, name):
  try:
    return str(data[name])
  except:
    return None

def checkZeros(x):
  if x is None:
    return x
  if len(x) == 1:
    return "0" + x
  else:
    return x

def cleanStateName(state):
  choices = ['connecticut','massachusetts','maine','new hampshire','vermont','rhode island']
  if state.lower() in choices:
    return state[0].upper() + state[1:].lower()
  if state is None:
    return False
  matchCheck  = process.extractOne(state.lower(), choices)
  if matchCheck[1] >= 90:
    return matchCheck[0][0].upper() + matchCheck[0][1:]
  return False


def validDateToJulianDate(datestring):
  formatedDate = yearFormating(stringReplacement(datestring))
  try:
    match=re.match('(\d{2})[/.-](\d{2})[/.-](\d{4})$', formatedDate)
    if match is not None:
      #Return the ISO 8601 format
      iso8601Format = datetime.datetime(*(map(int, match.groups()[-1::-1])))
      date44 = str(iso8601Format).split(" ")[0]
      struct_time = iso8601Format.timetuple()
      #Return the Julian Date in the format YYYYDDD
      #return date44, int('%d%03d' % (struct_time.tm_year, struct_time.tm_yday))
      return date44, int('%03d' % struct_time.tm_yday)
  except ValueError:
      pass
  #Return all invalid dates as False

  return None, None

def stringReplacement(text):
    text = text.replace('/', r'-').replace('.', r'-').replace('|', r'-')
    return text.split('-')

def yearFormating(text):
  year = [x for x in text if len(x) > 3]
  dayAndMonthChange = monthAndDayFormating(text)
  return "-".join(dayAndMonthChange + year)


def monthAndDayFormating(text):
  try:
    value = [checkZeros(x) for x in text if len(x) < 3]
    if len(value) != 2:
      return text
    if int(value[0]) < 12 and int(value[1]) > 12:
      value[1], value[0] = value[0], value[1]
      return value[:]
    else:
      return value
  except ValueError:
    return text



# def URLimage(collCode, iPlantGUID, iDigBioUUID, barcode, huhGUIDiPlant, URLformat):
#   if collCode is None:
#     return None
#   lCollection = collCode.lower()
#   lFormat = URLformat.lower()
#   if lFormat == 'full':
#     if lCollection in ['a', 'gh', 'nebc', 'fh', 'ames', 'econ', 'yu']:
#       url = 'http://bovary.iplantcollaborative.org/image_service/image/%s?rotate=guess&resize=1250&format=jpeg,quality,90' % iPlantGUID
#       if barcode in huhGUIDiPlant:
#         return url
#       else:
#         newURL = checkURL(url)
#       if newURL is None:
#         return idigBioURL(iDigBioUUID, lFormat)
#       return newURL
#     elif lCollection in ['conn']:
#       return getCONNURLPath(iDigBioUUID, barcode, lFormat)
#     elif lCollection in ['cbs', 'conn']:
#       return idigBioURL(iDigBioUUID, lFormat)
#     else:
#       return 'http://api.idigbio.org/v1/records/%s/media' % iDigBioUUID

#   elif lFormat == 'tnail':
#     if lCollection in ['a', 'gh', 'nebc', 'fh', 'ames', 'econ', 'yu']:
#       url = 'http://bovary.iplantcollaborative.org/image_service/image/%s?rotate=guess&resize=150&format=jpeg,quality,90' % iPlantGUID
#       if barcode in huhGUIDiPlant:
#         return url
#       else:
#         newURL = checkURL(url)
#       if newURL is None:
#         return idigBioURL(iDigBioUUID, lFormat)
#       return checkURL(url)
#     elif lCollection in ['conn']:
#       return getCONNURLPath(iDigBioUUID, barcode, lFormat)
#     elif lCollection in ['cbs']:
#       return idigBioURL(iDigBioUUID, lFormat)
#     else:
#       return 'http://api.idigbio.org/v1/records/%s/media?quality=thumbnail' % iDigBioUUID
#   return None

# def checkURL(url):
#   try:
#     req = urllib2.Request(url)
#     response = urllib2.urlopen(req)
#   except:
#     print 'FAIL on 269'
#     return None

# def huhiPlantGUID():
#   '''Open the the file to get all the metadata. Return a dictionary'''
#   with open('iplantID.csv', mode='rU') as infile:
#     #mydict = {row[0]: {'origDate': str(row[dateColumn]), 'altDate': validDateToJulianDate(str(row[dateColumn]))[0], 'julDate': validDateToJulianDate(str(row[dateColumn]))[1]} for index, row in enumerate(c.reader(infile))}
#     mydict = {str(row[1].replace('barcode-', '')): {'iPlantGUID': row[0]} for index, row in enumerate(c.reader(infile)) if index != 0}
#   return mydict

# def idigBioURL(iDigBioUUID, URLformat):
#   url = 'http://api.idigbio.org/v1/records/%s' % iDigBioUUID
#   try:
# 	  req = urllib2.Request(url)
# 	  response = urllib2.urlopen(req)
# 	  hold = response.read()
# 	  data = json.loads(hold)
#   except:
#     print 'URL FAIL 282 ', iDigBioUUID
#     return None

#   try:
#     req = urllib2.Request(data['idigbio:data']['dcterms:references'])
#   except:
#     return None
#   try:
#   	response = urllib2.urlopen(req)
#   	hold = response.read()
#   except:
#   	return None
#   soup=BeautifulSoup(hold)
#   a = soup.find_all(re.compile("(^a$)"))
#   if URLformat == 'full':
#     p = re.compile(r'http://deliver.odai.yale.edu/content/repository/YPM/id/.*/format/3')
#   elif URLformat == 'tnail':
#     p = re.compile(r'http://deliver.odai.yale.edu/content/repository/YPM/id/.*/format/1')
#   try:
#     urlLarge = p.search(str(a)).group()
#   except:
#     return None
#   try:
# 	  req = urllib2.Request(urlLarge)
# 	  response = urllib2.urlopen(req)
#   except:
#   	print 'FAIL on 313', iDigBioUUID
#   	return None
#   try:
#      return response.url
#   except:
#     return None
#   return None


# def getCONNURLPath(iDigBioUUID, barcode, URLformat):
#   try:
#     if URLformat == 'full':
#       ConnURL = 'http://portal.neherbaria.org/imglib/cnh/UConn_CONN/%s/%s.jpg' % (barcode[:9], barcode)
#       try:
#         req = urllib2.Request(ConnURL)
#         response = urllib2.urlopen(req)
#         return ConnURL
#       except:
#       	pass
#     else:
#       ConnURL = 'http://portal.neherbaria.org/imglib/cnh/UConn_CONN/%s/%s_tn.jpg' % (barcode[:9], barcode)
#       try:
#       	req = urllib2.Request(ConnURL)
#       	response = urllib2.urlopen(req)
#       	return ConnURL
#       except:
#       	pass
#   except:
#     pass

#   if URLformat == 'full':
#     url = 'http://api.idigbio.org/v1/records/%s/media' % iDigBioUUID
#   else:
#     url = 'http://api.idigbio.org/v1/records/%s/media?quality=thumbnail' % iDigBioUUID
#   try:
#     req = urllib2.Request(url)
#     response = urllib2.urlopen(req)
#     if response.url is not None:
#       return response.url
#   except:
#     print 'FAIL 349', iDigBioUUID
#     return None
#   return None



def checkFor9Zeros(x):
    if x < 9:
        return None
    zeroCheck = 8 - len(x)
    return '0' * zeroCheck + x


# def collectionGUID(collCode, barcode, huhGUIDiPlant):
#   if collCode is None:
#     return None
#   if collCode.lower() in ['a', 'gh', 'nebc', 'fh', 'ames', 'econ']:
#     newbarecode = checkFor9Zeros(barcode.replace('barcode-', ''))
#     if newbarecode in huhGUIDiPlant:
#       return huhGUIDiPlant[newbarecode]['iPlantGUID']
#     return iPlantGUID(collCode + newbarecode)
#   elif collCode.lower() in ['yu']:
#      return iPlantGUID(barcode)
#   return None

# def iPlantGUID(tagQ):

#   if tagQ is None:
#     return None
#   pattern = re.compile('resource_uniq.{35}')
#   url = 'http://bovary.iplantcollaborative.org/data_service/image?tag_query=filename:%s.jpg' % tagQ
#   try:
# 	  req = urllib2.Request(url)
# 	  response = urllib2.urlopen(req)
# 	  hold = response.read()
#   except:
#   	print 'FAIL on 385', tagQ
#   	pass
#   try:
#     temp = str(pattern.findall(hold)).split('"')[1]
#     if temp is not None:
#         return temp
#     return None
#   except:
#     return None
#   return None

if __name__ == '__main__':
  main()