import pandas as pd
import numpy as np
import datetime
import requests
import zipfile
import io

import wrds
db = wrds.Connection()

from ERC_func import get_dferc

# LOAD DATABASES THAT REMAIN IN THE ENTIRE LOOP 
# get permno from ccm linking table
dfccm_linktable = db.raw_sql("""select gvkey, lpermno, linkdt, linkenddt , linktype, linkprim FROM crsp.ccmxpf_linktable """)

# get dsenames with ncusip
dfdsenames = db.raw_sql("""select permno, ncusip, namedt, nameendt FROM crsp.dsenames """)

# ibes tickers
dfibesident = db.raw_sql("""SELECT ticker as ibes_ticker, sdates, cusip FROM ibes.idsum  """)

# download fama french 49 indstry returns as benchmark for abnormal returns
r = requests.get("http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/49_Industry_Portfolios_daily_CSV.zip")
z = zipfile.ZipFile(io.BytesIO(r.content))
z.extractall("include") # save into include folder

# FF 49 industry & sic code link table
df_sic = pd.read_csv('./include/Siccodes49.csv')
df_sic['unitkey'] = 1 # useful for linking later

# fama french 49 industry returns table
dateparser = lambda x: pd.datetime.strptime(x, '%Y%m%d')

df_ff49_ret = pd.read_csv('include/49_Industry_Portfolios_Daily.csv', skiprows=9, engine='python', skipfooter=2, parse_dates=True)

# only want first part of csv file
idx_first_empty_row = df_ff49_ret.index[df_ff49_ret.iloc[:,1].isna()][0]
df_ff49_ret = df_ff49_ret.iloc[0:idx_first_empty_row-100,:]

date = df_ff49_ret.iloc[:,0].apply(dateparser)
df_ff49_ret = df_ff49_ret.drop('Unnamed: 0', 1)
df_ff49_ret = df_ff49_ret.replace('%','',regex=True).astype('float')
df_ff49_ret['date'] = date

# in the original file some of the columns have whitespaces
df_ff49_ret.columns = df_ff49_ret.columns.str.strip()

# replace missing
df_ff49_ret.replace(-99.99, np.nan, inplace = True)
df_ff49_ret.replace(-999, np.nan, inplace = True)

# HYPERPARAMETERS for loop
startdate = datetime.date(1990, 1, 1) # when ibes starts:
enddate = datetime.date(1998, 12, 31)

days30 = datetime.timedelta(days=30)
days90 = datetime.timedelta(days=90)

jointype = 'inner'

# full loop
i = 0
while True:
    i += 1
    mindate = startdate + i*days90
    maxdate = mindate + days90
    print('iteration {!s}: mindate is {!s}, maxdate is {!s}'.format(i, mindate, maxdate))
    dfercmore = get_dferc(db, mindate, maxdate, jointype, dfccm_linktable, dfdsenames, dfibesident, df_sic, df_ff49_ret)
    print('size of new data is {!s}'.format(dfercmore.size))
    if i == 1:
        dferc = dfercmore
    else:
        dferc = pd.concat([dferc, dfercmore])
    if maxdate >= enddate:
        break

dferc.to_csv('ERCdata.csv')