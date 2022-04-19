import asyncio
import random

from aiohttp import ClientSession
import time


missing_fedora = []
missing_solr = []
missing_both = []


async def fetch_solr(uuid, session):
    url = 'http://localhost:8983/solr/kramerius/select?q=PID:\"{}\"&wt=json'
    async with session.get(url.format(uuid)) as resp:
        await asyncio.sleep(random.randint(0, 10))
        return uuid


async def fetch_fedora(uuid, session):
    url = 'http://localhost:8080/fedora/get/{}'
    async with session.get(url.format(uuid)) as resp:
        solr = await fetch_solr(uuid, session)
        if resp.status != 200:
            return uuid, solr
        else:
            return uuid, solr


async def bound_fetch(sem, uuid, session):
    async with sem:
        fedora, solr = await fetch_fedora(uuid, session)
        print(f'\n{fedora}\n{solr}\n\n')
        if not fedora and not solr:
            missing_both.append(uuid)
        elif not fedora and solr:
            missing_fedora.append(uuid)
        elif fedora and not solr:
            missing_solr.append(uuid)


async def run(uuids, limit):
    tasks = []
    sem = asyncio.Semaphore(limit)

    async with ClientSession() as session:
        for uuid in uuids:
            task = asyncio.ensure_future(bound_fetch(sem, uuid, session))
            tasks.append(task)
        response = asyncio.gather(*tasks)
        await response


def read_file(filename='urls.txt'):
    data = []
    with open(filename, 'r') as reader:
        for i in reader.readlines():
            data.append(i.strip('\n'))
    return data


uuids = read_file()
loop = asyncio.get_event_loop()

start = time.time()
future = asyncio.ensure_future(run(uuids, 20))
loop.run_until_complete(future)
stop = time.time()

total = stop - start
print(total)
print('BOTH: ', missing_both)
