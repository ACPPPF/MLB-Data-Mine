import urllib2
import lxml
import pandas as pd
import time
import httplib
import requests
import sys
import concurrent.futures

from retrying import retry
from bs4 import BeautifulSoup
from mlbparse import BRefParser

def load_url(url):
    r = requests.get(url, headers={'Connection': 'close'}, timeout=10)
    return r.content

def get_player_data(player):
    print player.filename
    page = url_connect(player.url)

    soup = BeautifulSoup(page, "lxml")
    print player.filename + " acquired"
    # # Get table and headers
    # table = soup.find('table')
    # head = [th.get_text(strip=True) for th in table.find('thead').find_all('th')]
    # table.tfoot.extract()
    # [row.extract() for row in table.find_all('tr', class_="minors_table hidden")]
    # pnd = pd.read_html(str(table))


def get_data(URLS):
    i = 0
    start = time.time()
    # Using 7 workers was the sweet spot on the low-RAM host I used, but you should tweak this
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    # Start the load operations and mark each future with its URL
        future_to_url = {executor.submit(load_url, link): link for link in URLS}
        for future in concurrent.futures.as_completed(future_to_url):
            this_id = future_to_url[future][0]
            try:
                data = future.result()
                soup = BeautifulSoup(data, "lxml")
                if soup.title:
                    # Due to some problems with foreign titles, I encoded in ascii
                    title = soup.title.string.encode('ascii', 'ignore')
                else:
                    title = ''
                # This removes the styles and scripts from the soup object so I can just grab text on page
                [s.extract() for s in soup(['style', 'script', '[document]', 'head', 'title'])]
                content = soup.getText().encode('ascii', 'ignore')
            except Exception:
                title = ''
                content = ''
            i = i + 1
            # Print out things so you know what's going on behind the scenes
            print("Completed link {} with title {}\r".format(i, title))
    runtime = time.time() - start
    print("took {} seconds or {} links per second".format(runtime, 100/runtime))

if __name__ == '__main__':
    parser = BRefParser()
    players = parser.get_url_queue()
    get_data([p.url for p in players])
