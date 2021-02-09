import sqlite3
import requests
import time

fh = open('api_key.txt')
api_key = fh.read()
fh.close()

# Cloud of Darkness 5.4 (73)
enc = 73
partition = 1  # 5.4 (1) 5.3 (7)



# Open DB, create main table (speed rankings), include necessary data to find the fight data
conn = sqlite3.connect('test.sqlite')
cur = conn.cursor()
cur.execute('''CREATE TABLE IF NOT EXISTS SpeedRank
 (rankKey INTEGER PRIMARY KEY, duration NUMERIC, guild TEXT, 
 report TEXT, fightID INT, reportStart NUMERIC, fightStart NUMERIC, region TEXT, server TEXT)''')


# Pull all speed rankings
js = {'hasMorePages': True}
pageNum = 0

while js['hasMorePages']:
    pageNum += 1
    serviceurl = 'https://www.fflogs.com:443/v1/rankings/encounter/{}' \
                 '?metric=speed&page={}&api_key='.format(enc, pageNum)
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

        cur.execute('''SELECT duration FROM SpeedRank WHERE (guild == ? AND server == ?)'''
                    , (rank['guildName'], rank['serverName']))
        duration = cur.fetchone()
        if (duration is None):
            cur.execute(
                '''INSERT INTO SpeedRank 
                (duration, guild, report, fightID, reportStart, fightStart, region, server)
                 VALUES (?,?,?,?,?,?,?,?)''',
                (rank['duration'], rank['guildName'], rank['reportID'], rank['fightID'], rank['reportStart'],
                 rank['startTime'], rank['regionName'], rank['serverName']))
        elif duration != rank['duration']:
            print('guild: {} server: {} duration: {}'
                  .format(rank['guildName'], rank['serverName'], rank['duration']))
            cur.execute('''UPDATE SpeedRank 
            SET duration = ?, report = ?, fightID = ?, reportStart = ?, fightStart = ?
             WHERE (guild == ? AND server == ?)''',
                        (rank['duration'], rank['reportID'], rank['fightID'],
                         rank['reportStart'], rank['startTime'],
                         rank['guildName'], rank['serverName']))

    print(response.headers['X-RateLimit-Remaining'])
    if int(response.headers['X-RateLimit-Remaining']) < 20:
        print('rate limit hold')
        time.sleep(1 * 60)  # let the limit reset

    print('page Num: {}'.format(pageNum))


conn.commit()
conn.close()


quit()

jobCodes = {'AST': 1, 'BRD': 2, 'BLM': 3, 'DRK': 4, 'DRG': 5, 'MCH': 6, 'MNK': 7, 'NIN': 8, 'PLD': 9, 'SCH': 10,
            'SMN': 11, 'WAR': 12, 'WHM': 13, 'RDM': 14, 'SAM': 15, 'DNC': 16, 'GNB': 17}
jobCodes2 = {'AST': 1, 'BRD': 2, 'BLM': 3, 'DRK': 4, 'DRG': 5, 'MCH': 6, 'MNK': 7, 'NIN': 8, 'PLD': 9, 'SCH': 10,
            'SMN': 11, 'WAR': 12, 'WHM': 13, 'RDM': 14, 'SAM': 15, 'DNC': 16, 'GNB': 17}

serviceurl = 'https://www.fflogs.com:443/v1/report/tables/damage-done/' \
             'WLatPJV1C3hH9Tq8?start=621660&end=1039090&translate=true&api_key='
url = serviceurl + api_key
response = requests.get(url)
js = response.json()

print(js['totalTime'])
for entry in js['entries']:
    print(entry['totalRDPS'])
    print(entry['totalRDPS']/js['totalTime'])

quit()




