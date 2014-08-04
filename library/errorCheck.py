from pandas import read_csv
import sys
import re
import warnings
import sys

def acisDateChecker(df):
  dateCheck = False
  for i in df['date']:
    if re.match("^[0-9-]*$", i):
      dateCheck = False
    else:
      dateCheck = True
      break
  return dateCheck

def cleanUp(df):
  df = df.dropna()
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
  check = 'location' in df.columns
  if check == False:
    sys.exit('Absence of column name [location]')

def suppress():
  warnings.filterwarnings("ignore")

def errorchecking():
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
