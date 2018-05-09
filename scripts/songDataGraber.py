# use the data in the google sheet (raw data by Seth and co) to create a json representation of that data
# to facilitate working with the data

import configparser
import httplib2
import json
import os
# import sqlite3

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

config = configparser.ConfigParser()
config.read('instances/config.ini')

# Code is based on this https://developers.google.com/sheets/api/quickstart/python

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/sheets.googleapis.com-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'songhelix'

sqlliteTable = '''song(id INTEGER PRIMARY KEY, "Song Title" TEXT, Composer TEXT, "Poet or lyricist" TEXT, Features TEXT, Keywords TEXT,
                      "Larger Work (original publication)" TEXT, "Poet's associated movements or -isms or Groups" TEXT, "Musical form" TEXT,
                      "Year of Composition" TEXT, "Catalog designation" TEXT, "Complete edition Reference" TEXT, "Composer's Place of Birth" TEXT,
                      "Original Language" TEXT, "Voice part suggested by the composer (where possible taken from the indication or publication)" TEXT,
                      "Berton Coffin suggested voice type" TEXT, "Berton Coffin suggested song type" TEXT, "Piano and voice only (Yes or leave blank)" TEXT,
                      "Orchestra and voice (check box)" TEXT, "Other instrumentation and voice (check box)" TEXT, "More than one voice" TEXT,
                      "Original Key" TEXT, "Range in original key" TEXT, "Dedicated to" TEXT, "Premiered by" TEXT, "Commissioned by" TEXT,
                      "Average Duration" TEXT, "Degrees (location, relationship, occupation)" TEXT, "Recommended Printed Source" TEXT,
                      "Recommended Translation Source" TEXT, "Score source" TEXT, "Audio source" TEXT,
                      "Other (musical allusions, accompaniment figures)" TEXT, "Contributor" TEXT, "order" TEXT)'''


def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'sheets.songhelix.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else:  # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


def createDB(theDB):
    cursor = theDB.cursor()
    cursor.execute(''' CREATE TABLE ''' + sqlliteTable)
    theDB.commit()


def scrapingGoogleSheetToJSON(jsonFilePath):

    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
    service = discovery.build('sheets', 'v4', http=http,
                              discoveryServiceUrl=discoveryUrl)

    spreadsheetId = config['GOOGLE_SHEET_RAW_DATA']['SPREADSHEET_ID']
    rangeName = 'Sheet1!A1:AI'
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheetId, range=rangeName).execute()
    values = result.get('values', [])

    # print(values
    iterRows = iter(values)
    header = next(iterRows)

    rows = []
    for i, row in enumerate(iterRows):
        rowDict = dict(zip(header, row))

        rows.append(rowDict)

    with open(jsonFilePath, 'w') as jsonfile:
        json.dump(rows, jsonfile, indent=4)

        # cursor = theDB.cursor()
        # print(i)
        # # print([i] + list(row))
        # # print('''INSERT INTO ''' + sqlliteTable + ''' VALUES(''' + ('i,' + ''.join(list(row))))
        # cursor.execute('''INSERT INTO ''' + sqlliteTable + ''' VALUES(''' + str(i) + ',' + ''.join(list(row)) + ''')''')
        # db.commit()

# TODO go on from here:
#
# scraping the google sheet and creating a csv file or something that could easily be added to a db

    #
    # if not values:
    #     print('No data found.')
    # else:
    #     print('Name, Major:')
    #     for row in values:
    #         # Print columns A and E, which correspond to indices 0 and 4.
    #         print('%s' % (row))


if __name__ == '__main__':
    # db = sqlite3.connect('songData.db')
    # createDB(db)
    # _v0314 = version 03=march and 14=day
    file_name_json = 'songHelixData_v0410.json'
    output_directory = './data'
    file_path_json = os.path.join(output_directory, file_name_json)
    try:
        os.remove(file_path_json)
    except OSError:
        pass

    scrapingGoogleSheetToJSON(file_path_json)
