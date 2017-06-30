import gauth
import numpy as np
import pandas as pd
import time
import sqlite3
import os
from bs4 import BeautifulSoup
import requests
import httplib2
from apiclient import discovery
import re
paramsFile = 'parameters00_cabiScrape.csv'
eu_eligChars = ''.join([chr(x) for x in ([45,46,95] + range(48,58) + range(64,91) + range(97,123))])
sec_2_msec = 1000
radiusEarth = 3959*5280 # feet
deg2rad = (np.pi/180.)
serviceAreaBounds = {'lat':[38,40],'lon':[-78,-76]}
qLook = 2 					# autoresponse output: how many B/D to require when deciding how many rows to display
qUniqSta = 3 				# autoresponse output: how many unique stations must be displayed, for B and for D
defaultNumRows2Display = 3 	# autoresponse output: how many rows are displayed at minimum



def defaultParams():
    return {'time_readData':60,'time_checkPingbox':5,'bufferTime_readData':0.7,\
                    'time_retryPingboxAutoresponse':15,'time_maxRetrySendingPingboxResponse':130,\
                    'time_eStatus':44000,'time_eDump':43170,\
                    'size_eDump':25.0} 
                    
def defaultNames():
    return {'eAddr':'','ePass':'','customerList':[],\
        'customerListFile':'customerList.csv',\
        'scrapeURL':'https://feeds.capitalbikeshare.com/stations/stations.xml',\
        'dbBase':'dockHist_','staticTable':'static','dynamicTable':'dynamic',
        'eFailLogFile':'log_eFail.txt',\
        'gService':'','gServoDatascraper':'','gServoPingbox':'','outgoingMail':[]}
        
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

def getCustomers(N):
    eligChars_addr = ''.join([chr(x) for x in ([45,46,95] + range(48,58) + range(64,91) + range(97,123))])
    tempL = list(pd.read_csv(N['customerListFile'],header=None)[0])
    N['customerList'] = [cust for cust in tempL if all([ch in eu_eligChars for ch in cust])]
    return N	
       
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
    return [DF,(cabiBase.content)]

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

def writeData2DB(DF,dfS,N,dumpCount,rawXML_String):
    try: 
		try:
			dfD = reset_DF_dtypes((DF.copy())[idFields()+timestampFields()+dynamicFields()])
			rek_writeSQL(dfD,dbName(N,dumpCount),N['dynamicTable'])
			rek_writeSQL(dfS,dbName(N,dumpCount),N['staticTable'])
			# if static fields have changed, send email alert:
			try:
				if (len(dfS)):
					strUpdatedStations = str(len(dfS)) + ' Updated Stations: \n\n'+\
									','.join(list([str(x) for x in dfS.index]))
					subject = 'static updates for ' + str(len(dfS)) + ' stations'
					[subject,body] = buildEmailAlert(subject,strUpdatedStations,P,dbName(N,dumpCount))
					sendGmail(N['eFailLogFile'],N['eAddr'],N['eAddr'],N['ePass'],subject,body)				
					try:
						[subject2,body2] = buildEmailAlert('raw XML',rawXML_String,P,dbName(N,dumpCount))
						sendGmail(N['eFailLogFile'],N['eAddr'],N['eAddr'],N['ePass'],subject2,body2)
					except:
						pass
			except: # but don't worry if the notification email fails:
				pass
			DF = DF[DF['latestupdatetime']<-999] # empty out DF, but keep cols        
			dfS = DF[idFields()+timestampFields()+staticFields()] # empty dfS
		except Exception as e:
			# send alert email if local database write failed
			[subject,body] = buildEmailAlert('failed to write DB',str(e),P,dbName(N,dumpCount))
			sendGmail(N['eFailLogFile'],N['eAddr'],N['eAddr'],N['ePass'],subject,body)
    except: # failed to write data into DB; hold on until next attempt
        pass
    return [DF,dfS]

def gmailDump_DB(LT,N,dumpCount):
	Q=0
	try:
		[subject,body] = buildEmailAlert(\
			'database dump: '+dbName(N,dumpCount),'',P,dbName(N,dumpCount))
		Q=sendGmail(N['eFailLogFile'],N['eAddr'],N['eAddr'],N['ePass'],subject,body,dbName(N,dumpCount))
		if Q:
			LT['time_eDump'] = time.clock()
			LT['time_eStatus'] = time.clock() # DB dump can subst for a beacon, so reset beacon timer too
			dumpCount += 1
			# perhaps also delete file?
	except:
		sendGmail(N['eFailLogFile'],N['eAddr'],N['eAddr'],N['ePass'],subject,body)
	return [Q,LT,dumpCount]

def getMessages_Label(label,service):
	msgList = service.users().messages().list(userId='me').execute()
	messageIDs = [dd['id'] for dd in msgList['messages']]
	matches=[]
	for mID in messageIDs[::-1]:
		msg = service.users().messages().get(userId='me',id=mID).execute()
		if (label in msg['labelIds']):
			matches.append(msg)	
	return matches

'''
def clearOutbox(N,service):
	msgList = service.users().messages().list(userId='me').execute()
	messageIDs = [dd['id'] for dd in msgList['messages']]
	fullArray_messageLabels = []
	for mID in messageIDs:
		fullArray_messageLabels.append(service.users().messages().get(userId='me',id=mID).execute()['labelIds'])
	sent = [1 if 'SENT' in msgLabels else 0 for msgLabels in fullArray_messageLabels]
'''	

def getMsgReturnPath(msg):
	rp = ''
	if ('payload' in msg.keys()):
		if ('headers' in msg['payload'].keys()):
			h=0
			while (h<len(msg['payload']['headers'])):
				mH = msg['payload']['headers'][h]
				if (('name' in mH) and ('value' in mH)):
					if (mH['name']=='Return-Path'):
						rp=re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+",mH['value'])[0]
						h=len(msg['payload']['headers'])
				h+=1
	return rp


def address2LatLong(addr,dfRecent):
		# return [latitude,longitude,numMatchingAddresses,textString]
				# where textString contains either a full address or an error message
	# if multiple addresses return: choose the one nearest the median of all station locations
	# addr = '+'.join(addr.split(' ')) # unnecessary
	urlBase_ReverseGeocode = 'https://maps.googleapis.com/maps/api/geocode/json'
	ddd = {'address':addr}
	sSB = {k:[str(b) for b in v] for (k,v) in serviceAreaBounds.items()}
	ddd['bounds'] = sSB['lat'][0]+','+sSB['lon'][0]+'|'+sSB['lat'][1]+','+sSB['lon'][1]
	gReq = requests.get(urlBase_ReverseGeocode, params=ddd)
	try:
		gResult = gReq.json()
		numResults = len(gResult['results'])
		if (numResults):
			dLL = gResult['results'][0]['geometry']['location']		# choose first location only
			return [dLL['lat'],dLL['lng'],numResults,gResult['results'][0]['formatted_address']]
		else:
			return [0.0,0.0,0,'OK but numResults=0']
	except ValueError:
		return [0.0,0.0,0,'JSON Decode Error']


def interpretPingboxRequest(inText,dfMostRecent):
	# return [successQ,mark4Trash,bike-or-dock,lat,lon]
	pattern = '\d+\s+[\w\s]+$'
	match = re.findall(pattern,inText)
	if (match):
		addr = match[0]
		try:
			[aLat,aLon,numAddr,gText] = address2LatLong(addr,dfMostRecent)
			if (numAddr):
				return [1,1,aLat,aLon,gText]
		except:
			return [0,1,aLat,aLon,'Error: cannot find address: "'+inText+'"'] # m4T=0 ???
	else:
		return [0,1,0.0,0.0,'Error: no pattern match found for input string']

def directionText(myAngle):
	myAngle /= deg2rad
	NS = ('N') if (myAngle>0) else ('S')
	EW = ('W') if ((abs(myAngle))>90) else ('E')
	posAngle = myAngle if (myAngle>0) else (myAngle+360)
	return (NS+EW+str(int(posAngle)).zfill(3))

def roundN(x,n):
	return int(round(x / float(n))) * n

def dfRow2autoresponseOutputString(ro):
		# input = one row of df (i.e. one station)
	outStr = '('+str(ro.nbbikes) + ',' + str(ro.nbemptydocks) + '), '
	outStr += directionText(ro.angle) + ' '
	outStr += str(roundN(ro.distance,10)) + 'ft, '
	outStr += str(ro['name']) # tricky: dot vs. [''] for subfield
	return outStr

def createAutoresponseBody(lat,lon,textAddrOrError,dfR):
	# return: list of strings, each is one separate line of the body
	if (textAddrOrError.startswith('Error:')):
		body = [textAddrOrError]
	else: # ping is good
		dfR['lat'] = pd.to_numeric(dfR['lat'])
		dfR['long'] = pd.to_numeric(dfR['long'])
		dfR['distNorth'] = deg2rad*(dfR['lat']-lat)*radiusEarth
		dfR['distEast']  = deg2rad*(dfR['long']-lon)*(np.cos(deg2rad*lat))*radiusEarth
		dfR['distance'] = np.sqrt(dfR['distNorth']**2 + dfR['distEast']**2)
		dfR['angle'] = np.arctan2(dfR['distNorth'],dfR['distEast'])  # radians CCW from east
		dfR = dfR.sort_values('distance')
		body=[]
		print('Placeholder: send response re: (%s,%s): "%s"' % (str(lat),str(lon),textAddrOrError))
		bSpots=0
		dSpots=0
		for j in range(min(defaultNumRows2Display,len(dfR))):
			psRow = dfR.iloc[j]
			bSpots += 1 if (psRow.nbbikes>=qLook) else 0
			dSpots += 1 if (psRow.nbemptydocks>=qLook) else 0
			body += [str(1+j) + ': '+dfRow2autoresponseOutputString(psRow)]
		jBase=j+1
		if (bSpots<qUniqSta):
			body += ['+B']
			while ((bSpots<qUniqSta) and (j<len(dfR)-1)):
				psRow = dfR.iloc[j]
				if (psRow.nbbikes>=qLook):
					bSpots += 1
					body += [str(1+j) + ': '+dfRow2autoresponseOutputString(psRow)]
				j+=1
		j=jBase
		if (dSpots<qUniqSta):
			body += ['+D']
			while ((dSpots<qUniqSta) and (j<len(dfR)-1)):
				psRow = dfR.iloc[j]
				if (psRow.nbemptydocks>=qLook):
					dSpots += 1
					body += [str(1+j) + ': '+dfRow2autoresponseOutputString(psRow)]
				j+=1
		#fullBody = '\n'.join(body)
		#print(fullBody)
		#print('B %s, D %s' % (bSpots,dSpots))
	return body # list of strings
	

def writeAutoresponsesAndCleanMailbox(outMail,N,dfRecent):
	# get messages. Append autoresponse for valid pings onto outMail. Trash almost everything.
	servo = N['gService']
	msgList = servo.users().messages().list(userId='me').execute() # actually returns dict
	if ('messages' in msgList):
		messageIDs = [dd['id'] for dd in msgList['messages']]
		messageLabels = []
		for mID in messageIDs[::-1]:
			mark4Trash=1 # only hold if it looks like a good ping, but we then had trouble creating response
			msg = servo.users().messages().get(userId='me',id=mID).execute()
			mLabs = msg['labelIds']
			if ('INBOX' in mLabs):
				customer = getMsgReturnPath(msg)
				print('   Received message from %s' % customer)
				if (customer in N['customerList']):
					print('     Customer %s found' % customer)
					[goodPing,mark4Trash,cLat,cLong,textAddrOrError] = interpretPingboxRequest(msg['snippet'],dfRecent)
					outgoingBody = createAutoresponseBody(cLat,cLong,textAddrOrError,dfRecent)
					# if outgoingBody is more than the default number of rows: split it into two text messages
										# and reverse the order, because phone will display the most recent first...:
					[oBod1,oBod2] = [outgoingBody[:defaultNumRows2Display],outgoingBody[defaultNumRows2Display:]]
					if (oBod2):
						outgoingMessage2 = gauth.create_message('me',customer,'','\n'.join(oBod2))
						outMail.append((time.clock(),outgoingMessage2))
					outgoingMessage1 = gauth.create_message('me',customer,'','\n'.join(oBod1))
					outMail.append((time.clock(),outgoingMessage1))
			if (mark4Trash):
				Q=gauth.trashMessage(servo,'me',mID)
				if (Q):
					print('                  trashing message %s' % mID)
				else:
					print('                  failed to trash message %s' % mID)
	return outMail

			
def sendAutoresponses(outMail,N,P):
	keepTheseMessages = [0]*len(outMail)
	msgsRetainedOutbox = []
	for j in range(len(outMail)):
		(creationTime,messageObject) = outMail[j]
		nowTime=time.clock()
		if (nowTime <= creationTime + P['time_maxRetrySendingPingboxResponse']):
			[Q,sentMsg]=gauth.send_message(N['gService'],'me',messageObject)
			keepTheseMessages[j]=1-Q # if successfully sent, remove from outgoing mail
		else:
			pass # could maybe send an alert that an outgoing msg failed to send?
		if (keepTheseMessages[j]):
			msgsRetainedOutbox.append((creationTime,messageObject))
	outMail = msgsRetainedOutbox
	return outMail


def processPingbox(outgoingMail,N,P,LT,dfR):
	outgoingMail = writeAutoresponsesAndCleanMailbox(outgoingMail,N,dfR)
	nowTime = time.clock()
	LT['time_checkPingbox'] = nowTime
	print('Now ready to send outgoing mail: %s, len(oM)=%d' % (str(nowTime),len(outgoingMail)))
	if (outgoingMail):
		if (nowTime >= LT['time_retryPingboxAutoresponse']+P['time_retryPingboxAutoresponse']):
			LT['time_retryPingboxAutoresponse']=nowTime
			print('Right now, I will send this response:')
			print(outgoingMail)
			outgoingMail = sendAutoresponses(outgoingMail,N,P)
			#outgoingMail = []
	return [outgoingMail,LT]

def setupCredentials(N):
	for fN in N:
		if fN.startswith('gServo'):
			subApplication = fN[6:]
			myCredentials = gauth.get_credentials(subApplication.lower())
			http = myCredentials.authorize(httplib2.Http())
			N[fN] = discovery.build('gmail','v1',http=http)
	return N






# MAIN:
processStartTime = time.clock()
[P,N] = readParams()
P = {k:1*v for (k,v) in P.iteritems()} 		# MULTIPLICATION FACTOR for DEBUGGING !
N = getCustomers(N)
LT = dict.fromkeys(P, processStartTime)
LT['time_eStatus'] = processStartTime - 2*P['time_eStatus'] # always beacon first iter
LT['time_readData'] = processStartTime - 3*P['time_readData']
DF = pd.DataFrame(index=range(0),columns=cabiFields())
dfS = DF[idFields()+timestampFields()+staticFields()]
dStatics = {}
dumpCount=0
try:
	#N = setupCredentials(N)
	credentials = gauth.get_credentials()
	http = credentials.authorize(httplib2.Http())
	N['gService'] = discovery.build('gmail', 'v1', http=http)
	outgoingMail = []
	# main loop
	while True:
		iterStartTime = time.clock()
		if ((LT['time_readData']+P['time_readData']) < (LT['time_checkPingbox']+P['time_checkPingbox']+P['bufferTime_readData'])):
			print(time.ctime() + ': ' + str(iterStartTime) + ': reading new Data')
			# scrape new data:
			try:
				[DF_new,rawXML] = getNewData(N['scrapeURL'])
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
			[DF,dfS] = writeData2DB(DF,dfS,N,dumpCount,rawXML)
			LT['time_readData'] = iterStartTime
			# if time, email dump or send a beacon:
			if ((os.path.isfile(dbName(N,dumpCount)))  and  \
				((os.path.getsize(dbName(N,dumpCount))>1e6*P['size_eDump'])\
								or((iterStartTime>(LT['time_eDump']+P['time_eDump']))))):
				# send an email dump
				[Q,LT,dumpCount]=gmailDump_DB(LT,N,dumpCount)
			elif ((iterStartTime>(LT['time_eStatus']+P['time_eStatus']))):
				# no dump, but send a beacon:
				[subject,body] = buildEmailAlert('beacon','',P,dbName(N,dumpCount))
				Q=sendGmail(N['eFailLogFile'],N['eAddr'],N['eAddr'],N['ePass'],subject,body)
				if Q:
					LT['time_eStatus'] = time.clock()
		else:
			# process Pingbox:
			try:
				print('                      '+str(iterStartTime) + ': checking Pingbox')
				[outgoingMail,LT]=processPingbox(outgoingMail,N,P,LT,DF_new)
				LT['time_checkPingbox'] = iterStartTime
			except:
				pass
		# sleep before next task:
		nowTime = time.clock()
		napTime = min(LT['time_readData']+P['time_readData'],LT['time_checkPingbox']+P['time_checkPingbox']) - nowTime
		time.sleep(max([0.1,napTime])) # safety: negSleep crashes
except Exception as e: # full process abort:
	print(str(e))
	[subject,body] = buildEmailAlert('Aborted Process '+str(os.getpid()),str(e),P,dbName(N,dumpCount))
	sendGmail(N['eFailLogFile'],N['eAddr'],N['eAddr'],N['ePass'],subject,body)
	[DF,dfS] = writeData2DB(DF,dfS,N,dumpCount,'no XML on cabiScrape abort')
	[Q,LT,dumpCount]=gmailDump_DB(LT,N,dumpCount)
