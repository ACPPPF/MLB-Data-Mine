import urllib2
import re
import lxml
import pandas as pd
import time

from multiprocessing import Process, Queue
from bs4 import BeautifulSoup
from string import ascii_lowercase
from player import Player

class BRefParser:
	def get_soup(self, webpage):
		return BeautifulSoup(webpage, "lxml")
	
	def get_pairs(self, letter):
		print "Getting all players with names starting with " + letter

		# Baseball reference page for letter
		response = urllib2.urlopen("http://www.baseball-reference.com/players/" + letter)
		soup = self.get_soup(response.read())

		# Each player page will have this pattern link
		pattern = re.compile("/players/" + letter + "/")

		# Find all players in div
		players = soup.find("div", class_="section_content")
		
		# Player page urls
		urls = ["http://www.baseball-reference.com" + a['href'] for a in players.find_all('a')]
		
		# File names to be stored under
		fnames = [re.sub('.shtml','',re.sub(pattern,'',a['href'])) for a in players.find_all('a')] 
		
		response.close()

		return [Player(key, value) for key, value in zip(fnames, urls)] # Zip it all up

	def get_url_queue(self):
		all_players = [] # Empty list of players
		for c in ascii_lowercase:
			all_players.extend(self.get_pairs(c)) # Extend, not append, to all_players
		return all_players