import asyncio
import aiohttp
import csv
from time import sleep, time


LIMIT = 6150


def write_csv(data):
    with open('async_liveinternet_ajax_data.csv', 'a', newline='', encoding='utf-8') as f:
        order = ['Name', 'URL', 'Description', 'Traffic', 'Percent']
        writer = csv.DictWriter(f, fieldnames=order)
        writer.writerow(data)


async def parse(session, pageNo):
    url = f'https://www.liveinternet.ru/rating/ru//today.tsv?page={pageNo}'
    print(f'Parsing {url}')
    async with session.get(url) as r:
        try:
            response = await r.read()
            data = response.decode('utf-8').strip().split('\n')[1:]

            for row in data:
                columns = row.strip().split('\t')
                name = columns[0]
                url = columns[1].replace('/', '')
                description = columns[2]
                traffic = columns[3]
                percent = columns[4]

                dic = {'Name': name,
                       'URL': url,
                       'Description': description,
                       'Traffic': traffic,
                       'Percent': percent}

                write_csv(dic)

        except UnicodeDecodeError:
            print('UnocodeDecodeError occured')


async def main():
    tasks = []

    async with aiohttp.ClientSession() as session:
        for i in range(1, LIMIT):
            task = asyncio.create_task(parse(session, i))
            tasks.append(task)

        await asyncio.gather(*tasks)

    sleep(1.5)

if __name__ == "__main__":
    start = time()
    asyncio.run(main())
    finish = time()
    speed = LIMIT/(finish - start)
    print(finish - start)
    print(f'Speed: {speed} URLs/second')


# Total amount taken for 6150 URLs = 81.663 seconds
# 75.309 URLs per second
