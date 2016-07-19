# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 11:29:50 2016
@author: rek_Standard
"""
import time
import sqlite3
import os
from bs4 import BeautifulSoup
import requests
import re
import numpy as np
import pandas as pd
paramsFile = 'parameters00_cabiScrape.csv'
sec_2_msec = 1000 
       
def defaultParams():
    return {'time_readData':60,\
                    'time_eStatus':87000,'time_eDump':86320,\
                    'size_eDump':38.0}
                    
def defaultNames():
    return {'eAddr':'scoot3129@gmail.com','ePass':'insecure123!@#',\
        'scrapeURL':'https://www.capitalbikeshare.com/data/stations/bikeStations.xml',\
        'dbBase':'dockHist_','staticTable':'static','dynamicTable':'dynamic',
        'eFailLogFile':'log_eFail.txt'}
        
def dbName(N,dumpNumber):
    return (N['dbBase']+('_'+str(os.getpid())+'_')+('%05d' % dumpNumber)+'.db')
    
def cabiFields():
    return ['id', 'installdate', 'installed', 'lastcommwithserver', 'lat',\
       'latestupdatetime', 'locked', 'long', 'name', 'nbbikes',\
       'nbemptydocks', 'public', 'removaldate', 'temporary',\
       'terminalname']
       
def idFields():
    return ['id']
    
def timestampFields():
    return ['lastcommwithserver','latestupdatetime']
    
def dynamicFields():
    return ['nbbikes','nbemptydocks']
    
def staticFields():
    return [F for F in cabiFields() if not F in (idFields()+dynamicFields()+timestampFields())]    
    
def getDtype(fieldname):
    if fieldname in timestampFields():
        return np.int64
    elif fieldname in idFields():
        return np.uint16
    elif fieldname in dynamicFields():
        return np.uint8
    else:
        return str        
       
def rek_writeSQL(DF,dbName,tableName):
    myConnection = sqlite3.connect(dbName)
    DF.to_sql(tableName,myConnection,if_exists='append')
    myConnection.close()    
    
def readParams(): # readParams(pFile)
    # read params Eventually want to read from file, or interactive?
    # For v2, don't allow params to change during execution
    P = defaultParams()
    N = defaultNames()
    return [P,N]
        
def buildEmailAlert(subject,bodyStart,P,dbFilename=''):
    bodyParts = [bodyStart,'','Python Process ID: '+str(os.getpid())]
    if (os.path.isfile(dbFilename)):
        bodyParts += ['Database Filename: '+dbFilename,\
                'Current File Size: '+str(os.path.getsize(dbFilename))+' bytes']
    bodyParts += ['','Parameters:']
    bodyParts += [(key + ': ' + str(P[key])) for key in P]
    body = '\n'.join(bodyParts)
    subject = (str(os.getpid())) + ': ' + subject
    return [subject,body]    
    
def sendGmail(errLog,sender,recipient,password,subject,body,fileAttach=''):
    try:
        import smtplib
        import os
        import zipfile
        from email import mime
        from email import encoders
        msg = mime.multipart.MIMEMultipart()
        msg['From'] = sender
        msg['To'] = recipient
        msg['Subject'] = subject
        msg.attach(mime.text.MIMEText(body, 'plain'))
        if ((os.path.isfile(fileAttach))):
            try:
                if (os.path.getsize(fileAttach)>1e5):
                    zipfilename = re.sub('\.\w+','.zip',fileAttach)
                    zFID = zipfile.ZipFile(zipfilename,'w',zipfile.ZIP_DEFLATED)
                    zFID.write(fileAttach)
                    zFID.close()
                    fileAttach = zipfilename
            except:
                pass
            attachment = open(fileAttach,"rb")
            part = mime.base.MIMEBase('application', 'octet-stream')
            part.set_payload((attachment).read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', "attachment; filename= %s" % fileAttach)
            msg.attach(part)
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender,password)
        text = msg.as_string()
        server.sendmail(sender, recipient, text)
        server.quit()
        Q=1
    except Exception as e:
        Q=0
        try:
            oFID = open(errLog,'a')
            oFID.write(time.ctime()+': '+str(e)+'\n')
            oFID.close()
        except Exception as eWrite:
            print(eWrite)
        # if e is file attachment, send an alert email. Else just print(e)
    return Q
    
def getNewData(thisURL):
    # reads XML file, converts to pandas dataFrame. Each row is one station.
    cabiBase = requests.get(thisURL)
    cabiSoup = BeautifulSoup(cabiBase.content,"lxml")
    CC = cabiSoup.findChildren()
    fnSoup = [x.name for x in CC]
    sta = cabiSoup.findAll('station')
    allContents = [x.contents for x in sta]
    fieldsHere = [[re.search('(?<=\<)\w+(?=>)',str(entry)).group(0) \
                for entry in x] for x in allContents]
    valuesHere = [[re.sub('&amp;','&',re.search('(?<=>)[^\<]*(?=\<)',str(entry)).group(0)) \
                             for entry in x] for x in allContents]              
    dNew = {}
    for ff in range(len(fieldsHere[0])):    # assumes they're all identical!
        thisField = fieldsHere[0][ff]
        thisType = getDtype(thisField)
        try:
            dNew.update({thisField:[thisType(x[ff]) for x in valuesHere]})
        except:
            temptemp = [x[ff] for x in valuesHere]
            temp2 = [thisType(x) if (len(x)) else -999 for x in temptemp]
            dNew.update({thisField:temp2})            
    overall_LastUpdate_sec = [int(CC[fnSoup.index('stations')].attrs['lastupdate'])/sec_2_msec]*(len(sta))
    zipIt = zip([1000000*OLU for OLU in overall_LastUpdate_sec],dNew['id'])
    DF = pd.DataFrame(dNew,index=[sum(zz) for zz in zipIt])
    return DF
    
def qChanged_staticFields(DF6,dicStatics):
    # input:
        # DF6: most recent data read
        # dicStatics: existing {id: DF[staticFields()]}
    # return:
        # boolean pd.Series indicating if the row in newDF changed SFs at all
        # updated dicStatics
    qChanged = pd.Series([False]*len(DF6),index=DF6.index)
    for row in (DF6.index):
        thisEntry = DF6.loc[row,staticFields()]
        if ((row not in dicStatics) or (any(thisEntry != dicStatics[row]))):
            qChanged[row]=True
            dicStatics[row] = thisEntry
    return [qChanged,dicStatics]
    
def reset_DF_dtypes(DF7):
    for col in DF7.columns:
        thisType = getDtype(col)
        DF7.__setattr__(col,DF7[col].astype(thisType)) 
    return DF7
    

def writeData2DB(DF,dfS,N,dumpCount):
    try:	
        dfD = reset_DF_dtypes((DF.copy())[idFields()+timestampFields()+dynamicFields()])
        rek_writeSQL(dfD,dbName(N,dumpCount),N['dynamicTable'])
        rek_writeSQL(dfS,dbName(N,dumpCount),N['staticTable'])
        # send email alert if static fields change:
        try:
            if (len(dfS)):
                strUpdatedStations = str(len(dfS)) + ' Updated Stations: \n\n'+\
                                ','.join(list([str(x) for x in dfS.index]))
                subject = 'static updates for ' + str(len(dfS)) + ' stations'
                [subject,body] = buildEmailAlert(subject,strUpdatedStations,P,dbName(N,dumpCount))
                sendGmail(N['eFailLogFile'],N['eAddr'],N['eAddr'],N['ePass'],subject,body)        
        except:
            pass
        DF = DF[DF['latestupdatetime']<999] # empty DF, but keep cols        
        dfS = DF[idFields()+timestampFields()+staticFields()] # empty dfS
    except Exception as e:
        # send alert email if write failed
        [subject,body] = buildEmailAlert('failed to write DB',str(e),P,dbName(N,dumpCount))
        sendGmail(N['eFailLogFile'],N['eAddr'],N['eAddr'],N['ePass'],subject,body)
    return [DF,dfS]

def gmailDump_DB(LT,N,dumpCount):
	Q=0
	try:
		[subject,body] = buildEmailAlert(\
			'database dump: '+dbName(N,dumpCount),'',P,dbName(N,dumpCount))
		Q=sendGmail(N['eFailLogFile'],N['eAddr'],N['eAddr'],N['ePass'],subject,body,dbName(N,dumpCount))
		if Q:
			LT['time_eDump'] = time.clock()
			LT['time_eStatus'] = time.clock()
			dumpCount += 1
			# perhaps also delete file?
	except:
		sendGmail(N['eFailLogFile'],N['eAddr'],N['eAddr'],N['ePass'],subject,body)
	return [Q,LT,dumpCount]



# MAIN:
processStartTime = time.clock()
[P,N] = readParams()
LT = dict.fromkeys(P, processStartTime)
LT['time_eStatus'] = LT['time_eStatus'] - 2*P['time_eStatus']
        # always send beacon first iter
DF = pd.DataFrame(index=range(0),columns=cabiFields())
dfS = DF[idFields()+timestampFields()+staticFields()]
dStatics = {}
dumpCount=0
try:
    while True:
        # read new data:
        startTime = time.clock()
        try:
            DF_new = getNewData(N['scrapeURL'])
            DF = DF.append(DF_new)
        except Exception as e:
            #send alert email if read failed
            [subject,body] = buildEmailAlert('failed to read CaBi data',str(e),P,dbName(N,dumpCount))
            Q=sendGmail(N['eFailLogFile'],N['eAddr'],N['eAddr'],N['ePass'],subject,body)
        # check static fields, update if needed:
        try:
            DF_newITS = (DF_new.copy())[idFields()+timestampFields()+staticFields()]
            DF_newITS = DF_newITS.set_index(DF_newITS.id)
            [whichStaticsChanged,dStatics] = qChanged_staticFields(DF_newITS,dStatics)
            dfS = dfS.append(DF_newITS.loc[whichStaticsChanged])
        except Exception as e:
            [subject,body] = buildEmailAlert('error processing static fields',str(e),P,dbName(N,dumpCount))
            Q=sendGmail(N['eFailLogFile'],N['eAddr'],N['eAddr'],N['ePass'],subject,body)
        # write new data:
        [DF,dfS] = writeData2DB(DF,dfS,N,dumpCount)
        nowTime = time.clock()
        # email dump:
        if ((os.path.isfile(dbName(N,dumpCount)))  and  \
            ((os.path.getsize(dbName(N,dumpCount))>1e6*P['size_eDump'])\
                            or((nowTime>(LT['time_eDump']+P['time_eDump']))))):
            # send an email dump
			[Q,LT,dumpCount]=gmailDump_DB(LT,N,dumpCount)
        elif ((nowTime>(LT['time_eStatus']+P['time_eStatus']))):
			# no dump, but send a beacon:
            [subject,body] = buildEmailAlert('beacon','',P,dbName(N,dumpCount))
            Q=sendGmail(N['eFailLogFile'],N['eAddr'],N['eAddr'],N['ePass'],subject,body)
            if Q:
                LT['time_eStatus'] = time.clock()
        nowTime = time.clock()
        napTime = P['time_readData'] - 0.3 - (nowTime-startTime)
        time.sleep(max([0.1,napTime]))
except Exception as eF:
    [subject,body] = buildEmailAlert('Aborted Process '+str(os.getpid()),str(eF),P,dbName(N,dumpCount))
    sendGmail(N['eFailLogFile'],N['eAddr'],N['eAddr'],N['ePass'],subject,body)
    [DF,dfS] = writeData2DB(DF,dfS,N,dumpCount)
    [Q,LT,dumpCount]=gmailDump_DB(LT,N,dumpCount)
