import urllib2
import re
import lxml
import pandas as pd
import time

from multiprocessing import Queue, Process, JoinableQueue, cpu_count
from bs4 import BeautifulSoup
from mlbparse import BRefParser

class Consumer(Process):
    def __init__(self, task_queue, result_queue):
        Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue

    def run(self):
        proc_name = self.name
        while True:
            next_task = self.task_queue.get()
            if next_task is None:
                # Poison pill means shutdown
                print '%s: Exiting' % proc_name
                self.task_queue.task_done()
                break
            print '%s: %s' % (proc_name, next_task)
            answer = next_task()
            self.task_queue.task_done()
            self.result_queue.put(answer)
        return


class Task(object):
    def __init__(self, player):
        self.player = player
    def __call__(self):
        time.sleep(0.1) # pretend to take some time to do the work
        print self.player.filename
        
        page = urllib2.urlopen(self.player.url).read()
        soup = BeautifulSoup(page, "lxml")

        # Get table and headers
        table = soup.find('table')
        head = [th.get_text(strip=True) for th in table.find('thead').find_all('th')]        
        table.tfoot.extract()   
        [row.extract() for row in table.find_all('tr', class_="minors_table hidden")]
        
        pnd = pd.read_html(str(table))

if __name__ == '__main__':
    parser = BRefParser()
    players = parser.get_url_queue()

    # Establish communication queues
    tasks = JoinableQueue()
    results = Queue()
    
    # Start consumers
    num_consumers = cpu_count()
    print 'Creating %d consumers' % num_consumers
    consumers = [ Consumer(tasks, results)
                  for i in xrange(num_consumers) ]
    for w in consumers:
        w.start()
    
    [tasks.put(Task(player)) for player in players]
    
    # Add a poison pill for each consumer
    for i in xrange(num_consumers):
        tasks.put(None)

    # Wait for all of the tasks to finish
    tasks.join()