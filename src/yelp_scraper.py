import requests
from bs4 import BeautifulSoup
import time
import string
from tqdm import tqdm
import sqlite3
import random
import re

class Yelp():
	def __init__(self):
		#Try 10 times before accepting link is bad
		self.BAD_REQUEST_TIMEOUT = 10
		self.CURRENT_BAD_REQUESTS = 0
		self.BAD_REQUEST_FILE = 'bad_request_urls.txt'
		self.proxies = {}
		self.initialize()

	def initialize(self):
		self.start_db()
		self.init_tables()
		self.get_inputs()

	def get_inputs(self):
		#Get scraping details
		self.search_term = input('Enter search term: ')
		self.search_city = input('Enter search city(\"Chicago\"): ')
		self.search_state = input('Enter search state(\"IL\"): ')

	def start_db(self):
		#Databse start
		check = re.compile('^.*.db$')
		db_name = input('Enter Database Name: ')
		while not check.match(db_name):
			print('Wrong format, \"*.db\" is required format')
			db_name = input('Enter Database Name: ')
		output_name = db_name.split('.')[0]
		self.conn = sqlite3.connect(db_name)
		self.conn.execute('pragma foreign_keys=ON')
		self.conn.commit()

	def init_tables(self):
		self.c = self.conn.cursor()
		try:
			self.c.execute('''CREATE TABLE RESTAURANT(id INTEGER PRIMARY KEY,Restaurant TEXT,Address TEXT,Keywords TEXT);''')
			self.c.execute('''CREATE TABLE REVIEW(RestaurantID INTEGER,Date TEXT,Rating REAL,Review TEXT PRIMARY KEY,CONSTRAINT FK_RESTAURANT FOREIGN KEY (RestaurantID) REFERENCES RESTAURANT(id));''')
		except Exception as e:
			print ('ERROR CREATING TABLES ' + e.args[0])

		try:
			self.c.execute('SELECT * FROM RESTAURANT ORDER BY id DESC LIMIT 1')
			result = self.c.fetchone()
			self.restaurantID = 0 if result is None else result[0] + 1
		except Exception as e:
			print ('ERROR SETTING RESTAURANT ID ' + e.args[0])
		
		
	def initialize_scraper(self):
		#Open first page to get number of places so can loop correctly
		url = 'https://www.yelp.com/search?find_desc={0}&find_loc={1},+{2}&start=0&attrs=RestaurantsPriceRange2.1'.format(self.search_term, self.search_city, self.search_state)
		html = self.get_html(url)
		soup = BeautifulSoup(html.content, 'html.parser')
		neighborhoods = soup.find('h4', string='Neighborhoods').parent.find_all('span')
		neighborhood_set = set()
		for n in neighborhoods:
			if n.string is not None and ', ' not in n.string and n.string != 'Cities':
				neighborhood_set.add(n.string.replace(' ', '_'))
		return neighborhood_set

	def get_html(self, url):
		#try to get url, write to file if bad data is returned
		while True:
			try:
				return requests.get(url, proxies=self.proxies)
			except Exception as e:
				self.CURRENT_BAD_REQUESTS += 1
				if self.CURRENT_BAD_REQUESTS > self.BAD_REQUEST_TIMEOUT:
					self.CURRENT_BAD_REQUESTS = 0
					print('BAD REQUEST TIMEOUT... SKIPPING... PRINTED TO {0}'.format(self.BAD_REQUEST_FILE))
					with open(self.BAD_REQUEST_FILE, 'a') as file:
						file.write(url + '\n')
					break
				print('BAD REQUEST... TRYING AGAIN')
				time.sleep(1)

	def scrape_reviews(self, soup):
		#scrape the reviews
		review_blocks = soup.find_all(itemprop='review')
		for block in review_blocks:
			rating = block.find(itemprop='ratingValue')['content']
			date = block.find(itemprop='datePublished')['content']
			review = block.find(itemprop='description').text.replace('\n', '')
			try:
				self.c.execute('INSERT INTO REVIEW VALUES(?, ?, ?, ?);', (self.restaurantID, date, rating, review))
				self.conn.commit()
			except Exception as e:
				print('ERROR INSERTING REVIEW ' + e.args[0])
			

	#returns a list of restaurants on the page
	def get_restaurants(self, index, neighborhood):
		page_url = 'https://www.yelp.com/search?find_desc={0}&find_loc={1},+{2}&start={3}&attrs=RestaurantsPriceRange2.1&l=p:[{4}:[{5}::[{6}]]]'.format(self.search_term, self.search_city, self.search_state, index, self.search_state, self.search_city, neighborhood)
		page_html = self.get_html(page_url)
		page_soup = BeautifulSoup(page_html.content, 'html.parser')
		page_restaurants = [r.contents[1]['href'].split('?')[0] for r in page_soup.find_all(class_='indexed-biz-name')] #Magical ads appear that make this necessary, split is to take search term off end of url(creates problems for going through reviews)
		return ['https://www.yelp.com%s' % s for s in page_restaurants]

	#Finds number of reviews of restaurant
	def get_review_count(self, soup):
		review_count = soup.find(itemprop='reviewCount')
		return 0 if review_count is None else int(review_count.string)

	#Finds name of restaurant
	def get_name(self, soup):
		#multiple tags with itemprop='name' so find tag above name to find name
		name = soup.find(itemprop='priceRange')
		if name is None:
			name = 'No Name Found'
		elif name.next_sibling is None:
			name = 'No Name Found'
		else:
			name = name.next_sibling.next_sibling['content']
		return name

	#Finds street address of restaurant
	def get_address(self, soup):
		street_address = soup.find(itemprop='streetAddress')
		#For restaurants with no address....Food trucks...
		if street_address is None:
			street_address = 'No address'
		#For restaurants with additional address lines...
		elif street_address.string is None:
			street_address = street_address.getText(separator=' ')
		else:
			street_address = street_address.string
		return street_address

	#Finds city of restaurant
	def get_city(self, soup):
		city = soup.find(itemprop='addressLocality')
		return '' if city is None else city.string

	#Finds state of restaurant
	def get_state(self, soup):
		state = soup.find(itemprop='addressRegion')
		return '' if state is None else state.string

	#Finds zipcode of restaurant
	def get_zipcode(self, soup):
		zipcode = soup.find(itemprop='postalCode')
		return '' if zipcode is None else zipcode.string

	#Finds the "tags" associated with each restaurant
	def get_tags(self, soup):
		keywords_list = []
		for tag in soup.find_all(itemprop='title'):
			if tag is not None:
				if tag.string != 'Food' and tag.string !='Restaurants':
					keywords_list.append(tag.string)
			else:
				keywords_list.append('No tag')
		keywords = ', '.join(keywords_list)
		return keywords

	#returns a dictionary with all of the restaurant's basic info
	def get_restaurant_info(self, url):
		restaurant_html = self.get_html(url)
		restaurant_soup = BeautifulSoup(restaurant_html.content, 'html.parser')
		review_count = self.get_review_count(restaurant_soup)
		print(review_count)
		name = self.get_name(restaurant_soup)
		print(name)
		street_address = self.get_address(restaurant_soup)
		city = self.get_city(restaurant_soup)
		state = self.get_state(restaurant_soup)
		zipcode = self.get_zipcode(restaurant_soup)
		full_address = street_address + ' ' + city + ', ' + state + ' ' + zipcode
		print(full_address)
		keywords = self.get_tags(restaurant_soup)
		print(keywords)
		try:
			self.c.execute('INSERT INTO RESTAURANT VALUES(?, ?, ?, ?);', (self.restaurantID, name, full_address, keywords))
			self.conn.commit()
		except Exception as e:
			print('ERROR INSERTING RESTAURANT ' + e.args[0])
		return review_count

	#main scraper
	def scrape(self):
		NEIGHBORHOOD_SET = self.initialize_scraper()
		for n in tqdm(NEIGHBORHOOD_SET):
			print('\n' + n)
			restaurants_present = True
			page = 0
			while(restaurants_present):
				restaurant_urls = self.get_restaurants(page, n)
				if not restaurant_urls: #No more restaurants in neighborhood
					restaurants_present = False
				for restaurant_url in restaurant_urls:
					#Review scraping
					review_count = self.get_restaurant_info(restaurant_url)
					review_page = range(0, review_count+1, 20)
					for r in review_page:
						#keep yelp happy?
						time.sleep(random.randint(1,2))
						url_new = restaurant_url + ("?start=%s" % r)
						html_new = self.get_html(url_new)
						soup_new = BeautifulSoup(html_new.content, 'html.parser')
						self.scrape_reviews(soup_new)
					self.restaurantID += 1
				page += 10 #Next Page		
				

if __name__ == '__main__':
	y = Yelp()
	y.scrape()
	self.conn.close()