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
enc = 73
partition = 1  # 5.4 (1)

jobCodes = {'AST': 1, 'BRD': 2, 'BLM': 3, 'DRK': 4, 'DRG': 5, 'MCH': 6, 'MNK': 7, 'NIN': 8, 'PLD': 9, 'SCH': 10,
            'SMN': 11, 'WAR': 12, 'WHM': 13, 'RDM': 14, 'SAM': 15, 'DNC': 16, 'GNB': 17}
jobCodes2 = {'AST': 1, 'BRD': 2, 'BLM': 3, 'DRK': 4, 'DRG': 5, 'MCH': 6, 'MNK': 7, 'NIN': 8, 'PLD': 9, 'SCH': 10,
            'SMN': 11, 'WAR': 12, 'WHM': 13, 'RDM': 14, 'SAM': 15, 'DNC': 16, 'GNB': 17}


for currentJob, currentVal in jobCodes2.items():
    # Initiate table
    conn = sqlite3.connect('cod_54_2-8-21.sqlite')
    cur = conn.cursor()

    cur.execute('''
    CREATE TABLE IF NOT EXISTS "{}" (name TEXT, rdps NUMERIC, adps NUMERIC, dps NUMERIC, duration NUMERIC,
     region TEXT, server TEXT, report TEXT,
     AST TEXT, BRD TEXT, BLM TEXT, DRK TEXT, DRG TEXT, MCH TEXT, MNK TEXT, NIN TEXT, PLD TEXT, SCH TEXT, SMN TEXT,
      WAR TEXT, WHM TEXT, RDM TEXT, SAM TEXT, DNC TEXT, GNB TEXT)'''.format(currentJob.replace("'", "''")))

    # Remove all anon's, b/c we can't easily check/update them against fresh pulls
    cur.execute('''DELETE FROM {} WHERE name == "Anonymous"'''.format(currentJob.replace("'", "''")))

    # Loop through all jobs, and first time through (val = 1 with AST) fill in the whole table as well

    js = {}
    for job, val in jobCodes.items():

        if val == 1:
            # First time through ############################################################################

            # Pull all rankings with Job
            js['hasMorePages'] = True
            pageNum = 0

            while js['hasMorePages']:
                pageNum += 1
                serviceurl = 'https://www.fflogs.com:443/v1/rankings/encounter/{}?' \
                             'metric=rdps&partition={}&spec={}&filter=1.{}.1&page={}&api_key='.format(enc, partition,
                                                                                            currentVal, val, pageNum)
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

                    cur.execute('''SELECT rdps FROM "{}" WHERE (name == ? AND server == ?)'''
                                .format(currentJob.replace("'", "''")), (rank['name'], rank['serverName']))
                    rdps = cur.fetchone()
                    if (rdps is None) or (rank['name'] == 'Anonymous'):
                        cur.execute(
                            '''INSERT INTO "{}" (name, rdps, adps, dps, duration, region, server, report, "{}")
                             VALUES (?,?,?,?,?,?,?,?,?)'''
                                .format(currentJob.replace("'", "''"), job.replace("'", "''")),
                            (rank['name'], rank['total'], rank['otherAmount'], rank['rawDPS'],
                             rank['duration'], rank['regionName'], rank['serverName'], rank['reportID'], 1))
                    elif rdps != rank['total']:
                        cur.execute('''UPDATE "{}" SET rdps = ?, adps = ?, dps = ?, duration = ?, report = ?, {} = ?
                         WHERE (name == ? AND server == ?)'''
                                    .format(currentJob.replace("'", "''"), job.replace("'", "''")),
                                    (rank['total'], rank['otherAmount'], rank['rawDPS'], rank['duration'],
                                     rank['reportID'], 1, rank['name'], rank['serverName']))

                print(response.headers['X-RateLimit-Remaining'])
                if int(response.headers['X-RateLimit-Remaining']) < 20:
                    print('rate limit hold')
                    time.sleep(1 * 60)  # let the limit reset

            # Pull all rankings without Job ##################
            # reset page start
            js['hasMorePages'] = True
            pageNum = 0

            while js['hasMorePages']:
                pageNum += 1
                serviceurl = 'https://www.fflogs.com:443/v1/rankings/encounter/{}?' \
                             'metric=rdps&partition={}&spec={}&filter=1.{}.0&page={}&api_key='.format(enc, partition,
                                                                                            currentVal, val, pageNum)
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

                    rdps = cur.execute('''SELECT rdps FROM "{}" WHERE (name == ? AND server == ?)'''
                                       .format(currentJob.replace("'", "''")), (rank['name'], rank['serverName']))
                    rdps = cur.fetchone()
                    if (rdps is None) or (rank['name'] == 'Anonymous'):
                        cur.execute(
                            '''INSERT INTO "{}" (name, rdps, adps, dps, duration, region, server, report, "{}")
                             VALUES (?,?,?,?,?,?,?,?,?)'''
                                .format(currentJob.replace("'", "''"), job.replace("'", "''")),
                            (rank['name'], rank['total'], rank['otherAmount'], rank['rawDPS'],
                             rank['duration'], rank['regionName'], rank['serverName'], rank['reportID'], 0))
                    elif rdps != rank['total']:
                        cur.execute('''UPDATE "{}" SET rdps = ?, adps = ?, dps = ?, duration = ?, report = ?, {} = ?
                         WHERE (name == ? AND server == ?)'''
                                    .format(currentJob.replace("'", "''"), job.replace("'", "''")),
                                    (rank['total'], rank['otherAmount'], rank['rawDPS'], rank['duration'],
                                     rank['reportID'], 0, rank['name'], rank['serverName']))

                print(response.headers['X-RateLimit-Remaining'])
                if int(response.headers['X-RateLimit-Remaining']) < 20:
                    print('rate limit hold')
                    time.sleep(1 * 60)  # let the limit reset

            conn.commit()
        else:
            # Other times through ###########################################################################

            # Pull all rankings with Job
            js['hasMorePages'] = True
            pageNum = 0

            while js['hasMorePages']:
                pageNum += 1
                serviceurl = 'https://www.fflogs.com:443/v1/rankings/encounter/{}?' \
                             'metric=rdps&partition={}&spec={}&filter=1.{}.1&page={}&api_key='.format(enc, partition,
                                                                                            currentVal, val, pageNum)
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
                    cur.execute('''UPDATE "{}" SET "{}" == ? WHERE (rdps == ? AND name == ?)'''
                                .format(currentJob.replace("'", "''"), job.replace("'", "''")),
                                (1, rank['total'], rank['name']))

                # print(response.headers['X-RateLimit-Remaining'])
                if int(response.headers['X-RateLimit-Remaining']) < 20:
                    print('rate limit hold')
                    time.sleep(1 * 60)  # let the limit reset

            # Pull all rankings without Job #######################
            # reset page start
            js['hasMorePages'] = True
            pageNum = 0

            while js['hasMorePages']:
                pageNum += 1
                serviceurl = 'https://www.fflogs.com:443/v1/rankings/encounter/{}?' \
                             'metric=rdps&partition={}&spec={}&filter=1.{}.0&page={}&api_key='.format(enc, partition,
                                                                                            currentVal, val, pageNum)
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
                    cur.execute('''UPDATE "{}" SET "{}" == ? WHERE (rdps == ? AND name == ?)'''
                                .format(currentJob.replace("'", "''"), job.replace("'", "''")),
                                (0, rank['total'], rank['name']))

                # print(response.headers['X-RateLimit-Remaining'])
                if int(response.headers['X-RateLimit-Remaining']) < 20:
                    print('rate limit hold')
                    time.sleep(1 * 60)  # let the limit reset

            conn.commit()
        print(currentJob + ": " + job)
        if job == 'GNB':
            break
        print('Sleeping')
        # Wait one minute
        time.sleep(0.5 * 60)
        print('Waking up')






