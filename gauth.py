# Receive and send secure email Python Gmail API
# http://stackoverflow.com/questions/25944883/
# https://developers.google.com/gmail/api/guides/sending

from __future__ import print_function
import os
import re

# from apiclient import errors
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

from email import mime
#from email.mime.text import MIMEText
#from email.mime.multipart import MIMEMultipart
#from email.mime.base import MIMEbase
import base64

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/gmail-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/gmail.modify'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Gmail API Python Quickstart'
dClientSecrets = {'pingbox':'client_secret_68.json',\
					'datascraper':'client_secret_67.json'}


flags = None # don't use command line args / argparse
def get_credentials(json_subApp=''):
	"""Gets valid user credentials from storage.

	If nothing has been stored, or if the stored credentials are invalid,
	the OAuth2 flow is completed to obtain the new credentials.

	Returns:
		Credentials, the obtained credential.
	"""
	if (not(json_subApp)):
		json_subApp = ''
	home_dir = os.path.expanduser('~')
	credential_dir = os.path.join(home_dir, '.credentials')
	if not os.path.exists(credential_dir):
		os.makedirs(credential_dir)
	credential_path = os.path.join(credential_dir,
								   'gmail-rw.json')

	store = Storage(credential_path)
	credentials = store.get()
	if not credentials or credentials.invalid:
		flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
		flow.user_agent = APPLICATION_NAME
		if flags:
			credentials = tools.run_flow(flow, store, flags)
		else: # Needed only for compatibility with Python 2.6
			credentials = tools.run(flow, store)
		print('Storing credentials to ' + credential_path)
	return credentials

def create_message(sender, to, subject, message_text, fileAttach=''):
	"""Create a message object for an email.
	Args:
	  sender: Email address of the sender.
	  to: Email address of the receiver.
	  subject: The subject of the email message.
	  message_text: The text of the email message.
	Returns:
	  An object containing a base64url encoded email object.
	"""
	if ((os.path.isfile(fileAttach))):
		msg = mime.multipart.MIMEMultipart()
		msg['From'] = sender
		msg['To'] = to
		msg['Subject'] = subject
		msg.attach(mime.text.MIMEText(message_text, 'plain'))
		try: # if it's a big file, zip it up first:
			if (os.path.getsize(fileAttach)>1e5): 
				zipfilename = re.sub('\.\w+','.zip',fileAttach)
				zFID = zipfile.ZipFile(zipfilename,'w',zipfile.ZIP_DEFLATED)
				zFID.write(fileAttach)
				zFID.close()
				fileAttach = zipfilename
		except:
			pass
		fp_attachment = open(fileAttach,"rb")
		part = mime.base.MIMEBase('application', 'octet-stream')
		part.set_payload((fp_attachment).read())
		fp_attachment.close()
		part.add_header('Content-Disposition', 'attachment', filename=fileAttach)
		msg.attach(part)
	else:
		#msg = mime.text.MIMEText(message_text)
		msg = mime.multipart.MIMEMultipart()
		msg['from'] = sender
		msg['to'] = to
		msg['subject'] = subject
		msg.attach(mime.text.MIMEText(message_text, 'plain'))
	return {'raw': base64.urlsafe_b64encode(msg.as_string())}



def send_message(service, user_id, message):
    """Send an email message.

    Args:
      service: Authorized Gmail API service instance.
      user_id: User's email address. The special value "me"
      can be used to indicate the authenticated user.
      message: Message to be sent.

    Returns:
      [successQ,Sent Message].
     """
    try:
        message = (service.users().messages().send(userId=user_id, body=message).execute())
        Q=1
        #print('Message Id: %s' % message['id'])
    except:
        Q=0
        #print('An error occurred: %s')
    return [Q,message]

def trashMessage(service, user_id, msg_id):
	"""Trash a Message.

	Args:
		service: Authorized Gmail API service instance.
		user_id: User's email address. The special value "me"
		can be used to indicate the authenticated user.
		msg_id: ID of Message that will be moved to Trash.
	"""
	try:
		service.users().messages().trash(userId=user_id, id=msg_id).execute()
		Q=1
		#print 'Message with id: %s trashed successfully.' % msg_id
	except:
		Q=0
		#print 'An error occurred: %s' % error
	return Q