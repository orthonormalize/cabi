# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import os
import glob
import zipfile
import re
import datetime
import time
import sqlite3
#from bs4 import BeautifulSoup

def unix2str(inTime):
    return datetime.datetime.fromtimestamp(int(inTime)).strftime('%Y-%m-%d %H:%M:%S')
    
def str2unix(inTime):  
    # assumes format like '1970-01-01 00:00:00'
    return time.mktime(datetime.datetime.strptime(inTime,'%Y-%m-%d %H:%M:%S').timetuple())    
    
def rek_readSQL(dbName,tableName):
    myConnection = sqlite3.connect(dbName)
    try:
        tableDF = pd.read_sql("SELECT * FROM "+tableName,myConnection)
    except:
        tableDF = []
    myConnection.close()
    return tableDF
    
def badStationLocs():
    return [00000,40000,40099,40098]    # throw out these non-existent stations
    
def rek_writeSQL(dbName,tableName,DF,wa):
    myConnection = sqlite3.connect(dbName)
    try:
        if (wa=='w'):
            DF.to_sql(tableName,myConnection,if_exists='replace')    
        elif (wa=='a'):
            DF.to_sql(tableName,myConnection,if_exists='append')
    except:
        print('error with rek_writeSQL')
    myConnection.close()

def get_cabiFieldMatcher():
    f = open('cabi_fieldMatcher.txt')
    lines = [LLINE.strip() for LLINE in f.readlines()]
    f.close()
    cfm_F = {}
    cfm_B = {}
    for ii in lines:
        [officialName,possibleNames] = ii.split(':')
        pNam = possibleNames.split(',')
        cfm_F.update({officialName:pNam})
        for nn in pNam:
            cfm_B.update({nn:officialName})
    return [cfm_F,cfm_B]
       
def csv2dic(csvfile,vInt=0):
    f = open(csvfile)
    lines = [LLINE.strip() for LLINE in f.readlines()]
    f.close()
    term_name = [re.findall('[^,]+',x) for x in lines]
    dd={}
    if vInt:
        for TN in term_name:
            dd.update({TN[1]:int(TN[0])})
    else:
        for TN in term_name:
            dd.update({TN[1]:TN[0]})
    return dd
    
def atoz():
    return 'abcdefghijklmnopqrstuvwxyz'    
    
def ismember(A,B):
    return [True if a in B else False for a in A]        
    
def listSelect(L,filterTF):
    return [el for (el, TF) in zip(L, filterTF) if TF]    
    
def dupeValues(A):
    return list(set([a for a in A if A.count(a) >= 2]))    

def fN_TH():
    return ['duration','startTime','endTime','startLoc','endLoc','member']
    
def fN_weatherW():
    return ['timestamp_KDCA','temp_F','RH','SKNT mph',\
                    'P01I in','P06I in','SNOW in','dewpointF']

def fN_weatherR():
    return ['timeW','tempF','RH','windSpeed',\
                    'precip01h','precip06h','snowDepth','dewpointF']


def reformatCabiField(idata,FN):
    if (FN=='duration'):
        try:
            odata = idata / 1000.0      # some files use numeric milliseconds
        except:
            # otherwise strings of 
            tuplist_HMS_str=[re.findall('(\d+)\D+(\d+)\D+(\d+)',x)[0] for x in idata]
            tuplist_HMS_int=[[int(x) for x in tup] for tup in tuplist_HMS_str]
            odata = [(3600*h + 60*m + s) for (h,m,s) in tuplist_HMS_int]
    elif (FN in ['startLoc','endLoc']):
        name2terminal = csv2dic('LUT___stationTerminalNames_revised.txt',1)
        idata = [(str(x)).strip() for x in idata]
        odata = [name2terminal[x] for x in idata]
    elif (FN in ['startTime','endTime']):
        if ('-' in idata.iloc[0]):
            tStr = '%Y-%m-%d %H:%M'
            #odata = [time.mktime(datetime.datetime.strptime(x,tStr).timetuple()) for x in idata]
            #tups_YMDhm_str = [re.findall('(\d+)-(\d+)-(\d+) (\d+):(\d+)',x)[0] for x in idata]
            #tups_YMDhm_int = [[int(x) for x in tup] for tup in tups_YMDhm_str]
            #odata = [(3600*h+60*mi+time.mktime(datetime.date(y,mo,d).timetuple())) \
            #                            for (y,mo,d,h,mi) in tups_YMDhm_int]
        else:
            tStr = '%m/%d/%Y %H:%M'
            #tups_MDYhm_str = [re.findall('(\d+)/(\d+)/(\d+) (\d+):(\d+)',x)[0] for x in idata]
            #tups_MDYhm_int = [[int(x) for x in tup] for tup in tups_MDYhm_str]
            #odata = [(3600*h+60*mi+time.mktime(datetime.date(y,mo,d).timetuple())) \
            #                            for (mo,d,y,h,mi) in tups_MDYhm_int]        
        odata = [time.mktime(datetime.datetime.strptime(x,tStr).timetuple()) for x in idata]
    elif (FN in ['member']):
        odata = ['C' if (r=='Casual') else 'M' for r in idata]
    return odata    

def read_TH_zipLogFile(zipsDir,dbName):
    # reads the log file, outputs which years and individual months have been covered previously
    fileDB = os.path.join(zipsDir,dbName+'.db')
    (yearsCovered,monthsCovered)=([],[])
    fileLOG = re.sub(r'\.zip\Z','.log',fileDB)
    if (os.path.isfile(fileLOG)):
        with open(fileLOG,'r') as f:
            for line in f:
                if (len(line)==6):
                    monthsCovered.append(line)
                elif (len(line)==4):
                    yearsCovered.append(line)
    return (yearsCovered,monthsCovered)

def TH_zips2db(zipsDir,dbName,tableName):
    # check directory zipsDir for zip files
    # write new files to database dbName,tableName
    # a log file will be generated, in zipsDir that will track previous DB writes
        # skip any zip file that contains a timeblock which has already been added to the DB
    (yearsCovered,monthsCovered) = read_TH_zipLogFile(zipsDir,dbName)
    cabiZipFiles = filter(lambda x: x.endswith('-capitalbikeshare-tripdata.zip'),os.listdir(zipsDir))
    for ff in cabiZipFiles:
        y = ff[:4]
        if (y not in yearsCovered):
            tempRFA = re.findall('\A[^\-]+(?=\-)',ff)
            if (len(tempRFA) != 1):
                print('Unable to parse file!  %s' % ff)
            else:
                y_or_m = tempRFA[0]
                if (y_or_m not in monthsCovered):
                    # this TH file has not been read, either as a full year or as an individual month
                    zf=zipfile.ZipFile(os.path.join(zipsDir,ff))
                    zipContents = filter(lambda x: x.endswith('.csv'),zf.namelist())
                    for csvfilename in zipContents:
                        th = pd.read_csv(zf.open(csvfilename))
                        
                    
                    
        
        


def TH_csv2db(yqStart,yqEnd,dbName,tableName):
    #origDIR = os.getcwd()
    #os.chdir(cabiDIR)
    y = yqStart / 10
    q = yqStart % 10
    yE = yqEnd / 10
    qE = yqEnd % 10
    [cfm_F,cfm_B] = get_cabiFieldMatcher()
    numRides=0
    while ((10*y+q) <= (10*yE+qE)):
        csvFilename = '20'+str(y)+'-Q'+str(q)+'-cabi-trip-history-data.csv'
        print('reading file: %s' % csvFilename)
        th = pd.read_csv(csvFilename)
        th.index = np.arange(numRides,numRides+len(th))
        cols=th.columns
        for col0 in cols:
            col = ''.join(listSelect(col0.lower(),ismember(col0.lower(),atoz())))
            if (col in cfm_B):
                FN = cfm_B[col]
                if FN in fN_TH():
                    print('    Processing field: %s' % FN)
                    th = th.rename(columns = {col0:FN})
                    th[FN] = reformatCabiField(th[FN],FN)
        th = th[fN_TH()]
        th.loc[:,'startHour'] = np.floor(th['startTime']/3600.0).astype('int64')
        th.loc[:,'endHour'] = np.floor(th['endTime']/3600.0).astype('int64')
        rek_writeSQL(dbName,tableName,th,'a')
        if (q < 4):
            q+=1
        else:
            y+=1
            q=1
        numRides += len(th)
    #os.chdir(origDIR)
    
    
def dicTimeOffsetsWeatherFields():
    return {'precip01h':3600,'precip06h':21600,'snowDepth':21600}
    
def weather_csv2db(weatherFile,dbName,tableW):
    dfW0 = pd.read_csv(weatherFile)
    dfW1 = dfW0[fN_weatherW()]
    dfW1 = dfW1.rename(columns = {(fN_weatherW()[j]):(fN_weatherR()[j]) \
                        for j in range(len(fN_weatherW()))})   
    for col in dfW1.columns:
        print('Weather: reading column: %s' % col)
        if (col=='timeW'):
            dfW1.timeW = dfW1.timeW.apply(lambda x: (re.findall('.*(?= E)',x)[0]))   # ignores EST/EDT... error @FallBack each November
            dfW1.timeW = dfW1.timeW.apply(lambda x: \
                    time.mktime(datetime.datetime.strptime(x,'%m-%d-%Y %H:%M').timetuple()))
        elif (col in ['precip01h','precip06h','snowDepth']):  # only allow backfill to replace NaNs up to a time difference given by tOffset
            pSeries=dfW1[col]
            tOffset = dicTimeOffsetsWeatherFields()[col]
            dexValid=len(dfW1)-1
            for dexCur in range((len(dfW1)-1),-1,-1):   # step backwards through data to determine if there is a valid replacement for each NAN
                if (np.isnan(pSeries.iloc[dexCur])):
                    if ((dfW1.timeW.iloc[dexValid]-dfW1.timeW.iloc[dexCur])<tOffset):
                        pSeries.iloc[dexCur]=pSeries.iloc[dexValid]
                else:
                    dexValid=dexCur
            dfW1[col]=dfW1[col].fillna(value=0)
        else:
            # just fill over the remaining missing data with later-measured values. Not many of them:
            dfW1[col] = (dfW1[col]).fillna(method='bfill')
    rek_writeSQL(dbName,tableW,dfW1,'w')


def getDTypes_StationFields():
    return {'terminalname':int,'name':str,'lat':float,'long':float,'el':float}    

"""    
def stations_xml2db(stationsXML,dbName,tableS):
    fXML = open(stationsXML)
    xmlBody = fXML.readlines()[-1]
    cabiSoup = BeautifulSoup(xmlBody,"lxml")
    CC = cabiSoup.findChildren()
    #fnSoup = [x.name for x in CC]
    sta = cabiSoup.findAll('station')
    allContents = [x.contents for x in sta]
    fieldsHere = [[re.search('(?<=\<)\w+(?=>)',str(entry)).group(0) \
                for entry in x] for x in allContents]
    valuesHere = [[re.sub('&amp;','&',re.search('(?<=>)[^\<]*(?=\<)',str(entry)).group(0)) \
                             for entry in x] for x in allContents]              
    dNew = {}
    for ff in range(len(fieldsHere[0])):    # assumes they're all identical!
        thisField = fieldsHere[0][ff]
        if thisField in getDTypes_StationFields():
            thisType = getDTypes_StationFields()[thisField]
            dNew.update({thisField:[thisType(x[ff]) for x in valuesHere]})
    #dfS = pd.DataFrame(dNew,index=dNew['terminalname'])
    #return dfS
"""

def stations_csv2db(stationsCSV,dbName,tableS):
    dfS = pd.read_csv(stationsCSV)
    rek_writeSQL(dbName,tableS,dfS,'w')
    
def getMatchedRowDexes_origWeather(ts_Index,W):
    # inputs: (list of N timestamps pre-arranged in an increasing arithmetic sequence, originalWeatherData)
    # output: (list of N indices, identifying which rows of the originalWeatherData correspond to each timestamp)
    print('computing merged weather timestamps...')
    dexW=0
    numT=(len(ts_Index))
    MRD=[0]*numT
    for dexT in range(numT):
        while(W.timeW[dexW]<ts_Index[dexT]):
            dexW+=1
        MRD[dexT] = dexW if ((2*ts_Index[dexT])>=(W.timeW[dexW-1]+W.timeW[dexW])) else (dexW-1)
    return MRD            
        
    
def getMergedWeatherDF(W,tStart,tEnd):
    # output pandas DF of all weather fields, indexed by on-hour timestamps
        # pick weather data closest to xx:30
    print('merging weather data...')
    h0 = int(np.floor(tStart/3600.0))
    hE = int(np.ceil(tEnd/3600.0))
    wIndex = range(h0,1+hE)                     # h in hours since unix0
    ts_Index = [1800+3600*x for x in wIndex]    # xx:30 in seconds
    w0Dexes = getMatchedRowDexes_origWeather(ts_Index,W)
    wMerged =  W.loc[w0Dexes]
    wMerged.index = wIndex
    wMerged = wMerged.drop('index',axis=1)
    return wMerged

def holidayList():
    holDF = pd.read_csv('holidays2010on.csv')
    temp1 = [(x.split('/')) for x in holDF.date]
    temp2 = [(int(x[2]),int(x[0]),int(x[1])) for x in temp1]
    return [datetime.date(*x) for x in temp2]    

def daysPerMonth():
    return [31,28,31,30,31,30,31,31,30,31,30,31]

def getTimeDF(tStart,tEnd):
    # creates DF containing the fields that are inherent functions only of time: (DOW,DOY,hour of day,isHol,year)
    holidays = holidayList()
    DPM = daysPerMonth()
    h0 = int(np.floor(tStart/3600.0))
    hE = int(np.ceil(tEnd/3600.0))
    wIndex = range(h0,1+hE)
    ts_Index = [3600*x for x in wIndex]    # xx:30 in seconds
    dtDate = [datetime.date.fromtimestamp(x) for x in ts_Index]
    dtDateTime = [datetime.datetime.fromtimestamp(x) for x in ts_Index]
    dTime = {}
    dTime['hour'] = [y.hour for y in dtDateTime]
    dTime['year'] = [x.year for x in dtDate]
    dTime['DOW'] = [x.isoweekday() for x in dtDate]
    dTime['DOY'] = [(x.day + sum(DPM[:(x.month-1)])) for x in dtDate]
    dTime['isHol'] = [1 if (x in holidays) else 0 for x in dtDate]
    dfTime = pd.DataFrame(dTime)
    dfTime.index = wIndex
    return dfTime
    
def strBikesDocks():
    return {'B':'start','D':'end'}    

def mergeData_old(DF1,W,S):
    # very slow !!
    # outputs all data into a single DF: uses compound names for all the rideCount columns
        # B_C_31000, D_M_31096, etc.
    W_h = getMergedWeatherDF(W,min(DF1.startTime),max(DF1.endTime))
    time_h = getTimeDF(min(DF1.startTime),max(DF1.endTime))
    strBD = strBikesDocks()
    responses = ['B','D']
    members = ['C','M']
    uSta = S.terminalname.unique()
    dd = {}
    for r in responses:
        fThLoc  = strBD[r]+'Loc'
        fThHour = strBD[r]+'Hour'
        for s in uSta:
            print('r = %s, s = %s' % (r,s))
            matchesS = (DF1[DF1[fThLoc]==s])
            for m in members:
                matchesMS = (matchesS[matchesS['member']==m])
                tableName = ('%s_%s_%s' % (r,m,s))
                dd[tableName] = pd.Series(W_h.index,index=W_h.index)
                dd[tableName] = dd[tableName].apply(lambda x: np.count_nonzero(matchesMS[fThHour]==x))
    PDF = pd.DataFrame(dd)
    return (pd.concat([time_h,W_h,PDF],axis=1))


def mergeData(DF1,W,S):
    # this outputs a different data structure (predictors, separate B and D each with double-indexed columns) than mergeData_old (one DS, compound names for columns)
    print(time.clock())
    print('Merging Trip History, Weather, and Station data frames: ')
    ssc_1B=DF1.groupby(['startHour','startLoc','member']).duration.count()   # Executes in two seconds for 2014-2015 data set
    ssc_1D=DF1.groupby(['endHour','endLoc','member']).duration.count()   # Executes in two seconds for 2014-2015 data set
    time_h = getTimeDF(min(DF1.startTime),max(DF1.endTime))
    W_h = getMergedWeatherDF(W,min(DF1.startTime),max(DF1.endTime))  # ~ 2 sec
    uSta = S.terminalname.unique()
    exhaustiveTups = [(Hour,Loc,cm) for Hour in W_h.index for Loc in uSta for cm in ('C','M')]  # 3 sec
    print(time.clock())
    print('Re-indexing after groupby().count() ... ')
    ssc_2B = ssc_1B.reindex(pd.MultiIndex.from_tuples(exhaustiveTups,names=['startHour','startLoc','member']),fill_value=0) # 25sec !
    ssc_2D = ssc_1D.reindex(pd.MultiIndex.from_tuples(exhaustiveTups,names=['endHour','endLoc','member']),fill_value=0) # 25 sec !
    print(time.clock())
    print('Unstacking nested pSeries ...')
    dfB3 = ssc_2B.unstack().unstack()
    dfD3 = ssc_2D.unstack().unstack()
    print(time.clock())
    predictors = pd.concat([time_h,W_h],axis=1)
    return ((predictors, dfB3, dfD3))