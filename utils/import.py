# python import to postgres script
import gzip
import json
import os
import psycopg2 as db
import re
# nlp fun!
from nltk.corpus import stopwords

conn = db.connect("dbname='postgres' user='data' host='localhost' password='moardata'")
insertQuery = 'INSERT INTO logdata(repo, hour, event, stars, payload) VALUES(%s, %s, %s, %s, %s)'

dataQuery = 'select distinct on (repo) repo, stars, payload from logdata order by repo'
transferQuery = 'insert into repo_desc(repo, "desc", short_desc, stars, issues, forks, lang) VALUES(%s, %s, %s, %s, %s, %s, %s)'

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
        word_list = row[2]['repo']['description'].lower().split(' ')
        # regex out punctuation and numbers
        punctuation = re.compile(r'[-.?!,":;()|0-9]')
        word_list = [punctuation.sub("", word) for word in word_list]
        # thanks to Daren Thomas
        # http://stackoverflow.com/questions/5486337/how-to-remove-stop-words-using-nltk-or-python
        filtered_words = [w for w in word_list if not w in stopwords.words('english')]
        desc = row[2]['repo']['description']
        short_desc = ' '.join(filtered_words)
        stars = row[1]
        issues = row[2]['repo']['open_issues']
        forks = row[2]['repo']['forks']
        lang = row[2]['repo']['language']
        cur.execute(transferQuery, (row[0], desc, short_desc, stars, issues, forks, lang))
        # commit the bitch
        conn.commit()


#getFiles('../reformat', 0)
transferFromLog()
