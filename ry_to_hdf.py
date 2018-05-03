# coding: utf-8

# In[1]:
import wradlib
import numpy as np
import os
import h5py
import zipfile
import datetime as dt
import pandas as pd
import sys


# In[2]:
try:
    for_year = sys.argv[1]
except:
    for_year = "2016"
    
rootdir = r"e:\data\radolan\ry"
tmpdir = r"e:\data\radolan\tmp"
h5file = 'hdf/ry_%s.hdf5' % for_year
hourlyfile = 'hdf/ry_hourly_%s.hdf5' % for_year
tstart = "%s-01-01" % for_year 
tend = "%s-12-31" % for_year
dffile = "hdf/exc_%s.csv" % for_year
nx = 900
ny = 900


# In[3]:
days = wradlib.util.from_to(dt.datetime.strptime(tstart, "%Y-%m-%d"),
                            dt.datetime.strptime(tend, "%Y-%m-%d"), tdelta=3600*24)
dtimes = wradlib.util.from_to(days[0].strftime("%Y-%m-%d 00:00:00"),
                              days[-1].strftime("%Y-%m-%d 23:55:00"), tdelta=60*5)

hrs = np.array([np.arange(0,24*12, 12), np.arange(12,25*12, 12)]).reshape((-1,2))

exc05 = np.zeros(len(dtimes)) * np.nan
exc1 = np.zeros(len(dtimes)) * np.nan
exc5 = np.zeros(len(dtimes)) * np.nan
exc10 = np.zeros(len(dtimes)) * np.nan


# In[4]:
df = pd.DataFrame({"dtime": dtimes, "exc05": exc05, "exc1": exc1, "exc5": exc5, "exc10": exc10})
df = df.set_index('dtime')


# In[5]:
with h5py.File(h5file, 'a') as f:
    for day in days:
        h5dir = day.strftime("%Y/%m/%d")
        print(h5dir)
        # Check for zip file and extract all
        zippath = os.path.join(rootdir, day.strftime("%Y/%m/%d/%Y%m%d_RY.zip"))
        if os.path.exists(zippath):
            zf = zipfile.ZipFile(zippath, "r")
            zf.extractall(tmpdir)
            zf.close()
        else:
            print("\tno archive.")
            continue
        # Create dataset if not exists
        data = np.zeros((24*12,900,900)).astype(np.float16) * np.nan
        if not h5dir in f:
            dset = f.create_dataset(h5dir, data.shape, dtype=np.float16, compression="gzip")
        else:
            dset = f[h5dir]
        for i, dtime in enumerate(wradlib.util.from_to(day.strftime("%Y-%m-%d 00:00:00"),
                                                       day.strftime("%Y-%m-%d 23:55:00"), 60*5)):
            ryfile = dtime.strftime("raa01-ry_10000-%y%m%d%H%M-dwd---bin")
            rypath = os.path.join(tmpdir, ryfile)
            try:
                data[i] = wradlib.io.read_radolan_composite(rypath, missing=np.nan)[0]
                os.remove(rypath)
            except FileNotFoundError:
                print("\tnot found: %s" % ryfile)
                continue
            except EOFError:
                print("\tEOFError: %s" % ryfile)
                continue
            except OSError:
                print("\tOSError: %s" % ryfile)
                continue
            df.loc[dtime].exc05 = len(np.where(data[i].ravel() >= 0.5)[0])
            df.loc[dtime].exc1 = len(np.where(data[i].ravel() >= 1.)[0])
            df.loc[dtime].exc5 = len(np.where(data[i].ravel() >= 5.)[0])
            df.loc[dtime].exc10 = len(np.where(data[i].ravel() >= 10.)[0])
        dset[:] = data
            # Hourly RY file
        with h5py.File(hourlyfile, 'a') as hourlyf:
            hdata = np.zeros((24,900,900)).astype(np.float16) * np.nan
            for i, hr in enumerate(hrs):
                hdata[i] = data[hr[0]:hr[1]].sum(axis=0)
            if not h5dir in hourlyf:
                dset = hourlyf.create_dataset(day.strftime("%Y/%m/%d"), data=hdata, compression="gzip")
            else:
                hourlyf[h5dir][:] = hdata

df.to_csv(dffile)

