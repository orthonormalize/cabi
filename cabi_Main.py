# -*- coding: utf-8 -*-

import cabi_Func as cf
import os
import pickle
#import pandas as pd
import numpy as np

dbName = 'cabi_201604.db'
tableTH = 'TH'
tableW  = 'Weather'
tableS  = 'Stations'
tableD = 'docksMerged'
tableB = 'bikesMerged'
weatherFile = 'KDCA___csv_mesowest_2010-2015.csv'
stationsCSV = 'stationInfo_v8.csv'

# ASSEMBLE RAW DATA:
if (not((os.path.isfile(dbName)))): # assume that TH is done, if db exists
    cf.TH_csv2db(134,154,dbName,tableTH)
    cf.weather_csv2db(weatherFile,dbName,tableW)
    cf.stations_csv2db(stationsCSV,dbName,tableS)
DF0 = cf.rek_readSQL(dbName,tableTH)
tossStations = cf.badStationLocs()
DF1 = DF0.loc[~((DF0['startLoc'].isin(tossStations)) | (DF0['endLoc'].isin(tossStations)))]
W = cf.rek_readSQL(dbName,tableW)
S = cf.rek_readSQL(dbName,tableS)
S.index = map(str,S.terminalname)
dDF = cf.mergeData(DF1,W,S)
dock_Columns = [x for x in dDF if x.startswith('D_')]
bike_Columns = [x for x in dDF if x.startswith('B_')]
bDF = dDF.copy()
bDF = bDF.drop(dock_Columns,axis=1)
dDF = dDF.drop(bike_Columns,axis=1)
cf.rek_writeSQL(dbName,tableD,dDF,'w')
cf.rek_writeSQL(dbName,tableB,bDF,'w')

pickle.dump(bDF, open( "save_bikes0912_000.p", "wb" ) )
pickle.dump(dDF, open( "save_docks0912_000.p", "wb" ) )


bDF = pickle.load('save_bikes0912_000.p','rb')
dDF = pickle.load('save_docks0912_000.p','rb')

stationStrings = map(str,S.terminalname)
CM = ['C','M']
B_List = ['B_'+cm+'_'+ss for cm in CM for ss in stationStrings]
D_List = ['D_'+cm+'_'+ss for cm in CM for ss in stationStrings]
colsS = {}
colsS['B']={}
colsS['D']={}
for s in S.terminalname:
    colsS['B'][s] = ['B_'+cm+'_'+str(s) for cm in CM]
    colsS['D'][s] = ['D_'+cm+'_'+str(s) for cm in CM]


bs45 = (bDF[((bDF.year==2014)|(bDF.year==2015)) & (bDF.DOW<=5) & (bDF.isHol==0)])
bsG = bs45.groupby(['hour'])
BSSA = bsG[B_List].mean()
ds45 = (dDF[((dDF.year==2014)|(dDF.year==2015)) & (dDF.DOW<=5) & (dDF.isHol==0)])
dsG = ds45.groupby(['hour'])
DSSA = dsG[D_List].mean()

for s in S.terminalname:
    BSSA[s] = BSSA[colsS['B'][s]].sum(axis=1)
    DSSA[s] = DSSA[colsS['D'][s]].sum(axis=1)
B_sumonly = BSSA[[x for x in BSSA.columns if type(x)==np.int64]]
D_sumonly = DSSA[[x for x in DSSA.columns if type(x)==np.int64]]



lats = [S.lat[S.terminalname==int(ss)] for ss in stationStrings]
lons = [S.long[S.terminalname==int(ss)] for ss in stationStrings]
B={}
D={}
for h in range(24):
    B[h] = [BSSA.loc[h,['B_'+cm+'_'+ss for cm in CM]].sum() for ss in stationStrings]
    D[h] = [DSSA.loc[h,['D_'+cm+'_'+ss for cm in CM]].sum() for ss in stationStrings]
