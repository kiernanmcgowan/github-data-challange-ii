# python import to postgres script
import gzip
import json
import os
import psycopg2 as db

conn = db.connect("dbname='postgres' user='data' host='localhost' password='moardata'")
insertQuery ='INSERT INTO logdata(repo, hour, event, stars, payload) VALUES(%s, %s, %s, %s, %s)'

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
                conn.commit()



def getFiles(dir, num):
    print dir
    for files in os.listdir(dir):
        print files
        storeInPostgres(os.path.join(dir, files), files, num)

getFiles('../reformat', 5)
