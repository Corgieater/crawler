from bs4 import BeautifulSoup
import threading
from concurrent.futures import ThreadPoolExecutor
import requests
from fake_useragent import UserAgent
import random
import time
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import Select
import csv
from datetime import datetime
from time import sleep
from dateutil.relativedelta import relativedelta
import os
from dotenv import load_dotenv
import pandas as pd

thread_local = threading.local()

load_dotenv()
CHROME_LOCATION = os.getenv('CHROME_LOCATION')
WATCHER_FOLDER = os.getenv('WATCHER_FOLDER')
FILE_LOCATION = os.getenv('FILE_LOCATION')
ENVIRONMENT = os.getenv('ENVIRONMENT')


user_agent = UserAgent(use_cache_server=False)
headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/"
              "webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.8",
    "Host": "www.imdb.com",  #目標網站
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": user_agent.random,
    "Referer": "https://www.google.com/"
}


today = datetime.today()
date = today.strftime("%Y-%m-%d")
file_name = f'movie_{date}'


from_date = None
to_date = None
genre_count = 0

# get year
with open(f'{FILE_LOCATION}year_list.csv', 'r', encoding="utf-8", errors="ignore") as f:
    reader = csv.reader(f)
    for row in reader:
        from_date = row[0].replace('/', '-')
        to_date = row[1].replace('/', '-')
        print('from year to year\n', from_date, to_date)


check_month = datetime.strptime(to_date, "%Y-%m-%d").date().month
# 3 months per scrape
# 2020-01-01 2020-03-31
# 2020-04-01 2020-06-30
# 2020-07-01 2020-09-30
# 2020-10-01 2020-12-31

# it's means it supposed to change year next time
if check_month == 12:
    next_from_date = datetime.strptime(from_date, "%Y-%m-%d").date() + relativedelta(months=3)
    next_to_date = datetime.strptime(to_date, "%Y-%m-%d").date() + relativedelta(months=3)
    next_from_date = next_from_date - relativedelta(years=2)
    next_to_date = next_to_date - relativedelta(years=2)
else:
    next_from_date = datetime.strptime(from_date, "%Y-%m-%d").date() + relativedelta(months=3)
    next_to_date = datetime.strptime(to_date, "%Y-%m-%d").date() + relativedelta(months=3)


def make_url_list(main_url):
    urls = []
    html = requests.get(main_url, headers=headers)
    random_delay([8, 5, 10, 6, 20, 11])
    soup = BeautifulSoup(html.text, 'html.parser')
    link_items = soup.findAll('h3', attrs={'class': 'lister-item-header'})
    # in case there is nothing
    if len(link_items) == 0:
        return 0
    for item in link_items:
        page_url = item.findNext('a')['href']
        urls.append('https://www.imdb.com'+page_url)
    return [urls, len(link_items)]


# new no mysql task write to csv
def get_data_clean_it_and_input_data(page_url):
    global genre_count
    movie_data_list = []

    page = requests.get(page_url, headers=headers)
    random_delay([1, 5, 10, 16, 8])

    soup = BeautifulSoup(page.text, 'html.parser')

    scraped_movie = []
    unclean_title = None
    unclean_year = None

    # get title
    try:
        unclean_title = soup.title.string
    except Exception as e:
        print(e)

    #     get year
    try:
        unclean_year = soup.findAll \
            ('a', class_="ipc-link ipc-link--baseAlt ipc-link--inherit-color sc-8c396aa2-1 WIUyh")
    except Exception as e:
        print(e)

    find_director = soup.find(text='Director')

    m_title = cut_string(unclean_title, ' (')
    scraped_movie.append(m_title)
    year = unclean_year[0].string
    scraped_movie.append(year)

    try:
        story_line = soup.find('span', class_='sc-16ede01-2 gXUyNh').getText()
        if story_line == '':
            print('no story line skip')
            return
        scraped_movie.append(story_line)
    except Exception as e:
        print(e)
    try:
        find_tag_line = soup.find(text="Taglines")
        tag_line = find_tag_line.findNext('div').findNext('ul') \
            .findNext('li').findNext('span').getText()
        scraped_movie.append(tag_line)
    except Exception as e:
        scraped_movie.append('')
        print(e)
    genres = []
    directors = []
    actors = []

    # THis is different from main merelyasite crwaler
    genres_div = soup.find('div', attrs={'data-testid': 'genres'})
    if genres_div is None:
        print('\n genres is none return')
        return
    else:
        genres_children = genres_div.findChildren('a')

        for g in genres_children:
            if genre_count < 3:
                genre = g.findNext('ul').findNext('li').getText()
                genres.append(genre.title())
                genre_count += 1

        genre_count = 0

        try:
            actors = soup.find(text='Stars')
            actors = clean_by_input(soup, 'Stars')
        except Exception as e:
            print(e)

        # dealing with directors
        if find_director is None:
            directors = clean_by_input(soup, 'Directors')
        else:
            directors.append(soup.find(text='Director').findNext('div').findNext('ul').
                             find('li').find('a').getText())

        # get poster url if none, skip movie
        try:
            imdb_poster_url = 'https://www.imdb.com' + \
                              soup.find('a', attrs={'class': 'ipc-lockup-overlay ipc-focusable'})['href']
            poster_place = requests.get(imdb_poster_url, headers=headers)
            random_delay([8, 5, 10, 6, 20, 11])
            soup = BeautifulSoup(poster_place.text, 'html.parser')
            poster_url = soup.find('img', attrs={'class': 'sc-7c0a9e7c-0 hXPlvk'})['src']

        except Exception as e:
            print(e, 'skip')
            return

        scraped_movie.append(genres)
        scraped_movie.append(directors)
        scraped_movie.append(actors)
        scraped_movie.append(poster_url)
        movie_data_list.append(scraped_movie)
        with open(f'{WATCHER_FOLDER}{file_name}.csv', 'a', newline='', encoding='utf8') as file:
            movie_writer = csv.writer(file)
            for data in movie_data_list:
                movie_writer.writerow(data)


def random_delay(time_list):
    delay_choices = time_list
    delay = random.choice(delay_choices)
    time.sleep(delay)


# cut movie title
def cut_string(string, target):
    target_index = string.index(target)
    result = string[:target_index]
    return result


# for multiple things to list
def clean_by_input(soup, input_text):
    item_list = []
    for item in soup.find(text=input_text).findNext('div').findNext('ul').findAll('li'):
        result = item.find('a').getText()
        item_list.append(result)
    return item_list


def get_driver():
    driver = getattr(thread_local, 'driver', None)
    if driver is None:
        chrome_options = webdriver.ChromeOptions()
        # for efficiency
        chrome_options.add_argument("--headless")
        # for not been block by pop windows
        chrome_options.add_argument("--disable-notifications")
        # driver location
        chrome_options.add_argument(CHROME_LOCATION)
        if ENVIRONMENT != 'local':
            # below seems will get some problems if not in ec2 try and see
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-setuid-sandbox")
            chrome_options.add_argument("--remote-debugging-port=9222")
            chrome_options.add_argument('--window-size=1420,1080')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')

        driver = webdriver.Chrome(options=chrome_options, service=Service(ChromeDriverManager().install()))
        setattr(thread_local, 'driver', driver)
    return driver


def check_if_next_page(url):
    chrome = get_driver()
    chrome.get(url)
    try:
        next_page = chrome.find_element(By.XPATH, '//*[@id="main"]/div/div[4]/a')
    except Exception as e:
        print(e)
        return False
    else:
        next_page.click()
        url = chrome.current_url
        return url


# this will get page url at first and later return next page
def search_movies_per_page(find_next=False):
    # 設置路徑讓Selenium找到chrome的driver
    chrome = get_driver()
    if find_next:
        next_page = None
        try:
            # IMDB previous and next link are the same if only one remain
            # Means their xpath will all be div[4][a] if at the very first page and the last page
            a_link_text = chrome.find_element(By.XPATH, '//*[@id="main"]/div/div[4]/a').text
            if a_link_text == 'Next »':
                next_page = chrome.find_element(By.XPATH, '//*[@id="main"]/div/div[4]/a')
            elif a_link_text == '« Previous':
                try:
                    next_page = chrome.find_element(By.XPATH, '//*[@id="main"]/div/div[4]/a[2]')
                except Exception as e:
                    print('only previous and last has a[2], paul')
                    print(e)
                    chrome.close()
                    # 我多加這行 晚點去ec2上看看 if anything wrong
                    return False
        except Exception as e:
            print(e)
            return False

        else:
            next_page.click()
    # first time, go to imdb
    else:
        chrome.get("https://www.imdb.com/")
        # find the drop down menu and click it
        imdb_drop_down = chrome.find_element(By.XPATH, '//*[@id="nav-search-form"]/div[1]/div/label/div')
        imdb_drop_down.click()
        advance_search = chrome.find_element(By.XPATH, '//*[@id="navbar-search-category-select-contents"]/ul/a')
        advance_search.click()
        advance_title_search = chrome.find_element(By.XPATH, '//*[@id="main"]/div[2]/div[1]/a')
        advance_title_search.click()
        sleep(3)
        # advance search
        featured_film_checkbox = chrome.find_element(By.XPATH, '//*[@id="title_type-1"]')
        featured_film_checkbox.click()
        # insert movies date where to  start from
        from_when_input = chrome.find_element(By.XPATH, '//*[@id="main"]/div[3]/div[2]/input[1]')
        from_when_input.send_keys(from_date)
        sleep(6)
        # insert movies date where to end
        to_when_input = chrome.find_element(By.XPATH, '//*[@id="main"]/div[3]/div[2]/input[2]')
        to_when_input.send_keys(to_date)
        # choose how many movies in a page
        sleep(6)
        movies_per_page_selection = chrome.find_element(By.XPATH, '// *[ @ id = "search-count"]')

        Select(movies_per_page_selection).select_by_value("50")
        sleep(1)
        submit = chrome.find_element(By.XPATH, '//*[@id="main"]/p[3]/button')
        submit.click()

    current_url = chrome.current_url
    return current_url


def scrape(urls):
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(get_data_clean_it_and_input_data, urls, chunksize=5)
        return True


if __name__ == '__main__':
    try:
        # get page url
        current_page = search_movies_per_page()
        urls_list_ready = make_url_list(current_page)
        while urls_list_ready != 0:
            list_len = urls_list_ready[1]
            if list_len != 0:
                scrape_done = scrape(urls_list_ready[0])
                if scrape_done:
                    continue_next = search_movies_per_page(find_next=True)
                    if continue_next:
                        urls_list_ready = make_url_list(continue_next)
                    else:
                        urls_list_ready = False
                        break

        with open(f'{FILE_LOCATION}year_list.csv', 'a', newline='', encoding='utf8') as f:
            writer = csv.writer(f)
            writer.writerow([next_from_date, next_to_date])

        # drop duplicates
        df = pd.read_csv(f'{WATCHER_FOLDER}{file_name}.csv')
        df.drop_duplicates()

        # write an ok file to tell watcher move
        with open(f'{WATCHER_FOLDER}{file_name}_ok.csv', 'w', newline='', encoding='utf8') as f:
            writer = csv.writer(f)
            writer.writerow('ok')
    except Exception as e:
        with open(f'{FILE_LOCATION}{file_name}_error.txt', 'w', newline='', encoding='utf8') as f:
            f.write(str(e))
