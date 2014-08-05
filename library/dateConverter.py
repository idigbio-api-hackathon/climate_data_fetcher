from datetime import datetime
from errorCheck import acisDateChecker

def joinDate(df):
  '''If acisDateChecker() is true, this will change the column header of 'date' to 'original date'.
  The data from 'date' does not change, just the header name. The data is then converted to Julian Date.
  If any individuals units of year, month, or day do not exist, it will break the full date column into
  separate columns of year, month, and day. Checks again that all the values 0-9 and '-' and then labels 
  that 'date'. '''

  if acisDateChecker(df) == True:
    df = tempColumnRename(df)
    df['date'] = changeDateformat(df) 

  df['julian_date'] = stringDateToJulianDate(df['date'])
  check = 'year' in df.columns
  if check == False:
    df['year'], df['month'], df['day'] = yearMonthDay(df['date'])
  return df


def yearMonthDay(string_dates):
  '''Takes a string of dates and breaks the data into individual units: year, month, and day.'''
  date_fmt = '%Y-%m-%d'
  year, month, day = [], [], []
  for index, string_date in enumerate(string_dates):
    #print string_date, y, m, d 
    try:
      #Separates the timestamp from 2011-07-01 to datetime.datetime(2011, 7, 1, 0, 0)
      date_time = datetime.strptime(string_date, date_fmt)
      #Creates a time tuple datetime.datetime(2011, 7, 1, 0, 0) to
      #time.struct_time(tm_year=2011, tm_mon=7, tm_mday=1, tm_hour=0, tm_min=0, tm_sec=0, tm_wday=4, tm_yday=182, tm_isdst=-1
      time_tuple = date_time.timetuple()
      #Convert time.struct_time(tm_year=2011, tm_mon=7, tm_mday=1, tm_hour=0, tm_min=0, tm_sec=0, tm_wday=4, tm_yday=182, tm_isdst=-1)
      #to Julian year days (2011182)
      year.append(time_tuple[0])
      month.append(time_tuple[1])
      day.append(time_tuple[2])
    except:
      year.append('')
      month.append('')
      day.append('')

  return year, month, day


def stringDateToJulianDate(string_dates):
  '''Takes a string of dates and converts to Julian date.'''
  julianDates = []
  for index, string_date in enumerate(string_dates):
    if string_date[len(string_date) - 2 :] == '-0':
      string_date = string_date[0:len(string_date) -2]
      date_fmt = '%Y-%m'
    else:
      date_fmt = '%Y-%m-%d'
    try:
      date_time = datetime.strptime(string_date, date_fmt)
      time_tuple = date_time.timetuple()
      julianDates.append(int('%d%03d' % (time_tuple.tm_year, time_tuple.tm_yday)))
    except:
      julianDates.append('')

  return julianDates

def changeDateformat(df):
  '''Takes individual units year, month, and day and concatenates into the format of YYYY-MM-DD'''
  newDateFormat = []
  for year, month, day, in zip(df['year'], df['month'], df['day']):
    newfmt = '-'.join([str(year), str(month), str(day)])
    newDateFormat.append(newfmt)
  return newDateFormat

def tempColumnRename(df):
  '''Changes the "date" column header to "original date,"" with the original data (and its incorrect formatting) unchanged.
  The new date, generated from changeDateformat() and in the correct format of YYYY-MM-DD, will be named 'date'.'''
  df = df.rename(columns={'date': 'orginal_date'})
  return df

