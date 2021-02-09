import urllib.request, urllib.parse, urllib.error
import json
import ssl
import sqlite3
import requests
import time

fh = open('api_key.txt')
api_key = fh.read()
fh.close()

# Cloud of Darkness 5.4 (73)
enc = 69
partition = 7  # 5.4 (1)

jobCodes = {'AST': 1, 'BRD': 2, 'BLM': 3, 'DRK': 4, 'DRG': 5, 'MCH': 6, 'MNK': 7, 'NIN': 8, 'PLD': 9, 'SCH': 10,
            'SMN': 11, 'WAR': 12, 'WHM': 13, 'RDM': 14, 'SAM': 15, 'DNC': 16, 'GNB': 17}
jobCodes2 = {'AST': 1, 'BRD': 2, 'BLM': 3, 'DRK': 4, 'DRG': 5, 'MCH': 6, 'MNK': 7, 'NIN': 8, 'PLD': 9, 'SCH': 10,
            'SMN': 11, 'WAR': 12, 'WHM': 13, 'RDM': 14, 'SAM': 15, 'DNC': 16, 'GNB': 17}


for currentJob, currentVal in jobCodes2.items():
    # Initiate table
    conn = sqlite3.connect('ramuh_53.sqlite')
    cur = conn.cursor()

    cur.execute('''
    ALTER TABLE  "{}" 
    ADD server TEXT'''.format(currentJob.replace("'", "''")))

    # Loop through all jobs, and update server name

    js = {}

    # Other times through ###########################################################################

    # Pull all rankings with Job
    js['hasMorePages'] = True
    pageNum = 0

    while js['hasMorePages']:
        pageNum += 1
        serviceurl = 'https://www.fflogs.com:443/v1/rankings/encounter/{}?' \
                     'metric=rdps&partition={}&spec={}&page={}&api_key='.format(enc, partition,
                                                                                    currentVal, pageNum)
        url = serviceurl + api_key
        invalidReturn = True  # invalid until proven otherwise
        while invalidReturn:
            try:
                response = requests.get(url)
                js = response.json()
                invalidReturn = False  # move on
            except:
                invalidReturn = True
                print('ouch')
                time.sleep(15)  # if invalid json return, wait 15 sec then try again

        for rank in js['rankings']:
            cur.execute('''UPDATE "{}" SET server == ? WHERE (rdps == ? AND name == ?)'''
                        .format(currentJob.replace("'", "''")),
                        (rank['serverName'], rank['total'], rank['name']))

        # print(response.headers['X-RateLimit-Remaining'])
        if int(response.headers['X-RateLimit-Remaining']) < 20:
            print('rate limit hold')
            time.sleep(1 * 60)  # let the limit reset

    conn.commit()

    print(currentJob)

    print('Sleeping')
    # Wait one minute
    time.sleep(0.5 * 60)
    print('Waking up')






