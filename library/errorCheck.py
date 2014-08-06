from pandas import read_csv
import sys
import re
import warnings
import sys

def acisDateChecker(df):
  '''Check the date column for the required format that is needed to send a request to ACIS Web Services.
  The date column needs to be in the format of YYYY-MM-DD. Uses a regular expression return true if only
  "0-9" and "-" characters are found. If any characters are found outside that range, the program will
  exit, Will see this error: "Date field contains non-digits. The require format is YYYY-MM-DD and anything
  outside will produce an error [location: errorcheck.py function acisDateChecker.py]"'''
  dateCheck = False
  for i in df['date']:
    if re.match("^[0-9-]*$", i):
      dateCheck = False
    else:
      dateCheck = True
      break
  return dateCheck

def cleanUp(df):
  '''Drop all rows containing NULL on any row, checks if column names exist for "year," "month," and "day,"
  and checks hat all values are integers. If columns contain any values that are not integers, it will produce
  this error: "Date field contains non-digits. The require format is YYYY-MM-DD and anything outside will 
  produce an error [location: errorcheck.py]"'''

  df = df.dropna(how='all')
  year_check = 'year' in df.columns
  month_check = 'month' in df.columns
  day_check = 'day' in df.columns
  if year_check == True and year_check == True and day_check == True:
    try:
      df[['year', 'month', 'day']] = df[['year', 'month', 'day']].astype(int)
    except:
      sys.exit('Date field contains non-digits.\nThe require format is YYYY-MM-DD and anything outside will produce an error\n[location: errorcheck.py line 23]')
  return df

def checkLocation(df):
  '''Checks whether the column name "location" is present. If "location" is not 
  found it will produce this error : "Absence of column name [location]" and 
  the script will exit.'''
  check = 'location' in df.columns
  if check == False:
    sys.exit('Absence of column name [location]')

def suppress():
  warnings.filterwarnings("ignore")

def errorchecking():
  '''Reads the first command line arguments and calls checkLocation, cleanUp, and acisDateChecker.
  This produces an error if a file extension is found: "File did not load correctly with **[filename.csv]**. 
  Make use to the correct file extension and/or path [location: errorcheck.py]"'''
  if len(sys.argv) == 1:
    sys.exit('Structure your commandline argument in this format:\npython name.py [path to the file that will be geocoded]\n[location: errorcheck.py line 37]')
  else:
    #Open the csv file into a dataframe 
    try:
      df = read_csv(sys.argv[1])
    except:
      error = 'File did not load correctly with **[%s]**. Make use to the correct file extension and/or path' % sys.argv[1]
      sys.exit(error + '\n[location: errorcheck.py line 42]')

    suppress()
    checkLocation(df)
    a= cleanUp(df)

    try:
      dateCheck = acisDateChecker(a)
    except:
      sys.exit('Date field contains non-digits.\nThe require format is YYYY-MM-DD and anything outside will produce an error \n[location: errorcheck.py function acisDateChecker line 52]')

    return a
