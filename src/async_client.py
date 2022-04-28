import asyncio
import json

import psycopg
from aiohttp import ClientSession
import time

from database import Database


missing_fedora = []
missing_solr = []
missing_both = []


async def fetch_solr(uuid, session):
    url = 'http://localhost/solr/kramerius/select?q=PID:\"{}\"&wt=json'
    async with session.get(url.format(uuid)) as resp:
        res = await resp.read()
        json_resp = json.loads(res.decode('utf8'))
        return True if json_resp['response']['numFound'] != 0 else False


async def fetch_fedora(uuid, session):
    url = 'http://localhost/fedora/get/{}'
    async with session.get(url.format(uuid)) as resp:
        solr = await fetch_solr(uuid, session)
        if resp.status != 200:
            return False, solr
        else:
            return True, solr


async def bound_fetch(sem, uuid, session):
    async with sem:
        fedora, solr = await fetch_fedora(uuid, session)

        if not fedora and not solr:
            print('missing')
            missing_both.append(uuid)
        elif not fedora and solr:
            print('missing')
            missing_fedora.append(uuid)
        elif fedora and not solr:
            print('missing')
            missing_solr.append(uuid)


async def run(uuids, limit):
    tasks = []
    sem = asyncio.Semaphore(limit)

    async with ClientSession() as session:
        for uuid in uuids:
            task = asyncio.ensure_future(bound_fetch(sem, uuid['token'], session))
            tasks.append(task)
            await asyncio.sleep(0.003)
        response = asyncio.gather(*tasks)
        await response


def write_file(filename, data):
    with open(filename, 'a') as writer:
        for i in data:
            writer.write(str(i) + '\n')


if __name__ == '__main__':
    count = 0
    limit = 50000
    database = Database()

    try:

        while True:
            missing_solr = []
            missing_both = []
            missing_fedora = []
            start = time.time()

            uuids = database.fetch_by_limit(limit)

            print(f'Start: {count}')

            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(run(uuids, 50))
            loop.run_until_complete(future)
            stop = time.time()
            print(f'{limit}: {stop-start} s')
            count += len(uuids)
            write_file('missing_both', missing_both)
            write_file('missing_fedora', missing_fedora)
            write_file('missing_solr', missing_solr)
            print('Fedora: ', missing_fedora)
            print('Solr: ', missing_solr)
            print('Both: ', missing_both)

            if uuids is None:
                database.close()
                break
    except KeyboardInterrupt as e:
        print(e)
    except psycopg.errors.Error as e:
        print(e)
        database.close()
    except IOError as e:
        print(e)
        database.close()
    finally:
        database.close()
        print('Bye-bye')

