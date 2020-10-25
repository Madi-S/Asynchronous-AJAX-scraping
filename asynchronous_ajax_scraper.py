import asyncio
import aiohttp
import csv
from time import sleep, time


LIMIT = 6150 # Pagination up to 6159, 6150 is taken only for experiments
ORDER = ['Name', 'URL', 'Description', 'Traffic', 'Percent'] # CSV Headers


# Function to write data (dictionary) - not asynchronous method to write csv files (since no big difference and it is better to write csv files synchronously)
def write_csv(data):
    with open('async_liveinternet_ajax_data.csv', 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=ORDER, delimiter='\t') # Dict writer is more preferable, since dictionary and headers is used
        writer.writerow(data)

# Fucntion to parse the page, which takes session and page number as parameters
async def parse(session, pageNo):
    url = f'https://www.liveinternet.ru/rating/ru//today.tsv?page={pageNo}' # format the URL
    print(f'Parsing {url}')
    async with session.get(url) as r:
        try:
            # Get raw (binary), then formatted content
            response = await r.read()
            data = response.decode('utf-8').strip().split('\n')[1:]

            # Parsing data
            for row in data:
                columns = row.strip().split('\t')
                name = columns[0]
                url = columns[1].replace('/', '')
                description = columns[2]
                traffic = columns[3]
                percent = columns[4]

                data = {'Name': name,
                        'URL': url,  
                        'Description': description,
                        'Traffic': traffic,
                        'Percent': percent}

                # Writing data
                write_csv(data)

        # If any decoding errors occured
        except UnicodeDecodeError:
            print('UnocodeDecodeError occured')


# Main asynchronous function, which is called and where tasks are created
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
# 75.309, 77.893  URLs per second
