# python import to postgres script
import gzip
import json
import os
import psycopg2 as db
import redis
import re
# nlp fun!
from nltk.corpus import stopwords

# connect to redis
red = redis.StrictRedis(host='localhost', port=6379, db=0)

conn = db.connect("dbname='postgres' user='data' host='localhost' password='moardata'")
insertQuery = 'INSERT INTO logdata(repo, hour, event, stars, payload) VALUES(%s, %s, %s, %s, %s)'

dataQuery = 'select distinct on (repo) repo, stars, payload from logdata order by repo'
transferQuery = 'insert into repo_desc(repo, "desc", short_desc, stars, issues, forks, lang) VALUES(%s, %s, %s, %s, %s, %s, %s)'

nlpQuery = 'select repo, short_desc, stars, lang from repo_desc'

cur = conn.cursor()


def storeInPostgres(file, fName, num):
    f = gzip.open(file, 'rb')
    fc = f.read()
    obj = json.loads(fc)

    # remove bad repos
    if obj.get('undefined', False):
        del obj['undefined']

    reposToInsert = []
    for repoId, repoObj in obj.iteritems():
        # scan for stars (and it is always stars)
        didBreak = False
        for eventType, eventObj in repoObj.iteritems():
            for event in eventObj:
                if event['repo'].get('watchers', -1) >= num:
                    reposToInsert.append(repoId)
                    didBreak = True
                    break
            if didBreak:
                break

    # figure out the hour from the file name
    base = fName.replace('index-', '').replace('.json.gz', '')
    arr = base.split('-')
    timeStamp = arr[0] + '-' + arr[1] + '-' + arr[2] + ' ' + arr[3] + ':00'

    for repoId in reposToInsert:
        repo = obj[repoId]
        for eventType, eventArr in repo.iteritems():
            for event in eventArr:
                cur.execute(insertQuery, (repoId, timeStamp, eventType, event['repo']['watchers'], json.dumps(event)))
    # commit the bitch
    conn.commit()


def getFiles(dir, num):
    print dir
    for files in os.listdir(dir):
        print files
        storeInPostgres(os.path.join(dir, files), files, num)


def transferFromLog():
    print 'query data'
    cur.execute(dataQuery)
    print 'fetch data'
    rows = cur.fetchall()
    print 'process data'
    for row in rows:
        print row[0]

        repoObj = row[2]['repo']
        desc = repoObj.get('description', '')

        word_list = desc.lower().split(' ')
        # regex out punctuation and numbers
        punctuation = re.compile(r'[-.?!,":;()|0-9]')
        word_list = [punctuation.sub("", word) for word in word_list]
        # thanks to Daren Thomas
        # http://stackoverflow.com/questions/5486337/how-to-remove-stop-words-using-nltk-or-python
        filtered_words = [w for w in word_list if not w in stopwords.words('english')]

        short_desc = ' '.join(filtered_words)
        stars = row[1]
        issues = repoObj.get('open_issues', 0)
        forks = repoObj.get('forks', 0)
        lang = repoObj.get('language', '')
        cur.execute(transferQuery, (row[0], desc, short_desc, stars, issues, forks, lang))
        # commit the bitch
        conn.commit()


def redisPunishment():
    print 'time to punish redis'
    cur.execute(nlpQuery)
    rows = cur.fetchall()
    for row in rows:
        print row[3]
        desc = row[1]
        # inc by one, avoid multiply by zero errors
        incVal = row[2] + 1
        words = desc.split(' ')
        for w in words:
            # under score the work by the lang for seperation
            red.hincrby('weight', w, incVal)
            red.hincrby('count', w, 1)
            if True != (not row[3]):
                red.hincrby(row[3], w, incVal)
                red.hincrby(row[3] + '_count', w, 1)


def createWordDist():
    print 'creating word dist'
    # get all the different keys
    keys = red.keys('*')
    for k in keys:
        print k
        postfix = ''
        # per lang count
        if ('_count' in k):
            postfix = k
        # per lang weight
        else:
            postfix = k

        # total weight
        if ('weight' == k):
            postfix = 'all'
        # total count
        if ('count' == k):
            postfix = 'count'

        obj = red.hgetall(k)
        key = 'words_' + postfix
        for word, weight in obj.iteritems():
            if (word != ''):
                red.hincrby(key, word, weight)

#getFiles('../reformat', 0)
#transferFromLog()
#redisPunishment()
createWordDist()
