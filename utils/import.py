# python import script
import gzip
import json
import psycopg2 as db

insertQuery = db.prepare('INSERT INTO logdata(repo, hour, event, stars, payload) VALUES($1, $2, $3, $4, $5)')


def storeInPostgres(file, fName, num):
    f = gzip.open(file, 'rb')
    fc = f.read()
    obj = json.loads(fc)
    del obj['undefined']

    reposToInsert = []
    for repoId, repoObj in obj:
        # scan for stars (and it is always stars)
        didBreak = False
        for eventType, eventObj in repoObj:
            for event in eventObj:
                if event.repo.watchers >= num:
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
        for eventType, eventArr in repo:
            for event in eventArr:
                insertQuery(repoId, timeStamp, eventType, event['repo']['watchers'], json.dumps(event))
