import requests
import csv
from time import time


scraped_count = 0
LIMIT = 6150  # Pagination up to 6159, 6150 is taken only for experiments
ORDER = ['Name', 'URL', 'Description', 'Traffic', 'Percent']


# Function to return text content of the page
def get_lines(url):
    r = requests.get(url)
    return r.text


# Function to write data to a csv file
def write_csv(data):
    with open('sync_liveinternet_ajax_data.csv', 'a', newline='', encoding='utf-8') as f:
        try:
            writer = csv.DictWriter(f, fieldnames=ORDER, delimiter='\t')
            writer.writerow(data)

        except UnicodeDecodeError:
            pass


# Main function, which is called and where page are looped through
def main():
    for i in range(1, LIMIT):
        url = f'https://www.liveinternet.ru/rating/ru//today.tsv?page={i}' # Formatting the URL
        print(f'Parsing {url}')
        response = get_lines(url)
        data = response.strip().split('\n')[1:]

        # Parsing the data
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
            write_csv(data)


if __name__ == "__main__":
    start = time()
    main()
    finish = time()
    speed = LIMIT/(finish - start)
    print(finish - start)
    print(f'Speed: {speed} URLs/second')


# 2.8342. 2.7780 URLs per second
