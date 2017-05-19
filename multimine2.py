import urllib2
import re
import lxml
import pandas as pd
import time
import gevent
import signal
import httplib

from multiprocessing import Queue, Process, JoinableQueue, cpu_count
from bs4 import BeautifulSoup
from mlbparse import BRefParser
from gevent import monkey

def get_player_data(player):
    print player.filename   
    try:
        page = urllib2.urlopen(player.url).read()
    except httplib.IncompleteRead, e:
        page = e.partial
    
    soup = BeautifulSoup(page, "lxml")
    print player.filename + " acquired"

    # # Get table and headers
    # table = soup.find('table')
    # head = [th.get_text(strip=True) for th in table.find('thead').find_all('th')]        
    # table.tfoot.extract()   
    # [row.extract() for row in table.find_all('tr', class_="minors_table hidden")]
    
    # pnd = pd.read_html(str(table))

if __name__ == '__main__':
    # patches stdlib (including socket and ssl modules) to cooperate with other greenlets
    monkey.patch_all()

    parser = BRefParser()
    players = parser.get_url_queue()

    jobs = [gevent.spawn(get_player_data, player) for player in players[:10]]
    gevent.joinall(jobs)
