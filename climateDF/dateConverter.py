import csv
import sys
import re
import datetime
from pandas import read_csv, DataFrame, concat


def formatChecker(filename):
  reader = csv.DictReader(open(filename))
  result = {}
  key = -1
  for row in reader:
    key += 1
    # key = row.pop('url')
    if key in result:
      pass
    if row['longitude'] is '' or row['latitude'] is '':
      continue
    row['longitude'] = float(row['longitude'])
    row['latitude'] = float(row['latitude'])
    result[key] = row

  for k, v, in result.iteritems():
    if 'location' not in v:
      raise NameError('Missing ["location"] header')
    if 'day' not in v or 'month' not in v or 'year' not in v:
      raise NameError('Missing ["day"], ["month"], or ["year"] header')
    tempDate = validDateToJulianDate(v['month'] +'-'+ v['day'] +'-'+ v['year'])
    result[k].update({'concatDate':tempDate[0]})
    result[k].update({'julianDay':tempDate[1]})
    result[k].update({'julianDate':tempDate[2]})
  df = DataFrame.from_dict(result, orient='index', dtype=None)
  if 'latitude' not in df.columns and 'longitude' not in df.columns:
    raise NameError('Missing ["latitude"], or ["longitude"] or header')
  # df = df.convert_objects(convert_numeric=True).dtypes
  # df[['latitude', 'longitude']] = df[['latitude', 'longitude']].astype(float)

  # except:
  #   sys.exit('Date field contains non-digits.')
  return df

def changeToFloat(x):
  if x is '':
    return float('NaN')
  return x

def validDateToJulianDate(datestring):
  formatedDate = yearFormating(stringReplacement(datestring))
  try:
    match=re.match('(\d{2})[/.-](\d{2})[/.-](\d{4})$', formatedDate)
    if match is not None:
      #Return the ISO 8601 format
      iso8601Format = datetime.datetime(*(map(int, match.groups()[-1::-1])))
      date44 = str(iso8601Format).split(" ")[0]
      struct_time = iso8601Format.timetuple()
      return date44, int('%03d' % struct_time.tm_yday), int('%d%03d' % (struct_time.tm_year, struct_time.tm_yday))
  except ValueError:
      return None, None, None
  return None, None, None

def stringReplacement(text):
    text = text.replace('/', r'-').replace('.', r'-').replace('|', r'-')
    return text.split('-')

def yearFormating(text):
  year = [x for x in text if len(x) > 3]
  dayAndMonthChange = monthAndDayFormating(text)
  return "-".join(dayAndMonthChange + year)

def checkZeros(x):
  if len(x) == 1:
    return "0" + x
  else:
    return x

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

# def stringDateToJulianDate(string_dates):
#   '''Takes a string of dates and converts to Julian date.'''
#   julianDates = []
#   for index, string_date in enumerate(string_dates):
#     if string_date[len(string_date) - 2 :] == '-0':
#       string_date = string_date[0:len(string_date) -2]
#       date_fmt = '%Y-%m'
#     else:
#       date_fmt = '%Y-%m-%d'
#     try:
#       date_time = datetime.datetime.strptime(string_date, date_fmt)
#       time_tuple = date_time.timetuple()
#       julianDates.append(int('%d%03d' % (time_tuple.tm_year, time_tuple.tm_yday)))
#     except:
#       julianDates.append('')

#   return julianDates

def stringDateToJulianDate(string_date):
  string_date= str(string_date)
  '''Takes a string of dates and converts to Julian date.'''
  if string_date[len(string_date) - 2 :] == '-0':
    string_date = string_date[0:len(string_date) -2]
    date_fmt = '%Y-%m'
  else:
    date_fmt = '%Y-%m-%d'  
  date_time = datetime.datetime.strptime(string_date, date_fmt)
  time_tuple = date_time.timetuple()
  return int('%d%03d' % (time_tuple.tm_year, time_tuple.tm_yday))


if __name__ == '__main__':
  main()