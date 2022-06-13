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


from_year = None
to_year = None

# get year
with open(f'{FILE_LOCATION}year_list.csv', 'r', encoding="utf-8", errors="ignore") as f:
    reader = csv.reader(f)
    for row in reader:
        from_year = row[0]
        to_year = row[1]


next_from_year = datetime.strptime(from_year, "%Y-%m-%d").date() - relativedelta(years=1)
next_to_year = datetime.strptime(to_year, "%Y-%m-%d").date() - relativedelta(years=1)

genre_count = 0
# movie_counts = 0
# global urls_list_ready
# urls_list_ready = None


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
    # global movie_counts
    # movie_counts += 1
    movie_data_list = []
    # print('movie_counts\n', movie_counts)

    page = requests.get(page_url, headers=headers)
    random_delay([1, 5, 10, 16, 8])

    soup = BeautifulSoup(page.text, 'html.parser')

    scraped_movie = []
    # scraped_poster = []
    unclean_title = None
    unclean_year = None
    tag_line = None
    poster_url = None

    # get title
    try:
        unclean_title = soup.title.string
        print(unclean_title)
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
    # scraped_poster.append(m_title)
    year = unclean_year[0].string
    scraped_movie.append(year)
    # scraped_poster.append(year)
    try:
        story_line = soup.find('span', class_='sc-16ede01-2 gXUyNh').getText()
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

        # get poster url if none, don't need to do anything
        try:
            imdb_poster_url = 'https://www.imdb.com' + \
                              soup.find('a', attrs={'class': 'ipc-lockup-overlay ipc-focusable'})['href']
            poster_place = requests.get(imdb_poster_url, headers=headers)
            random_delay([8, 5, 10, 6, 20, 11])
            soup = BeautifulSoup(poster_place.text, 'html.parser')
            poster_url = soup.find('img', attrs={'class': 'sc-7c0a9e7c-0 hXPlvk'})['src']
            # scraped_poster.append(poster_url)

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
        # chrome_options.add_argument("--no-sandbox")
        # chrome_options.add_argument("--disable-setuid-sandbox")
        # below seems it has some problems if not in ec2
        # chrome_options.add_argument("--remote-debugging-port=9222")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument(CHROME_LOCATION)

        driver = webdriver.Chrome(options=chrome_options, service=Service(ChromeDriverManager().install()))
        setattr(thread_local, 'driver', driver)
    return driver


def check_if_next_page(url):
    print('in next func')
    chrome = get_driver()
    chrome.get(url)
    try:
        next_page = chrome.find_element(By.XPATH, '//*[@id="main"]/div/div[4]/a')
        print('next_page', next_page)
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
        try:
            # # test only since i didn't open a chrome during this test!!!!
            # chrome.get('https://www.imdb.com/search/title/?'
            #            'title_type=feature&release_date=2021-01-01,2021-01-15&sort=moviemeter,desc&count=50')
            next_page = chrome.find_element(By.XPATH, '//*[@id="main"]/div/div[4]/a')
        except Exception as e:
            print(e)
            return False
        else:
            print('i am clicking next page baby!!!!!!!!!\n')
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
        from_when_input.send_keys(from_year)
        # insert movies date where to end
        to_when_input = chrome.find_element(By.XPATH, '//*[@id="main"]/div[3]/div[2]/input[2]')
        to_when_input.send_keys(to_year)
        # choose how many movies in a page
        movies_per_page_selection = chrome.find_element(By.XPATH, '// *[ @ id = "search-count"]')

        Select(movies_per_page_selection).select_by_value("250")
        sleep(1)
        submit = chrome.find_element(By.XPATH, '//*[@id="main"]/p[3]/button')
        submit.click()

    current_url = chrome.current_url
    # chrome.close()
    return current_url

# # TEST this will get first page
# def search_movies_per_year():
#     # 設置路徑讓Selenium找到chrome的driver
#     chrome = get_driver()
#     # go to imdb
#     chrome.get("https://www.imdb.com/")
#     # find the drop down menu and click it
#     imdb_drop_down = chrome.find_element(By.XPATH, '//*[@id="nav-search-form"]/div[1]/div/label/div')
#     imdb_drop_down.click()
#     advance_search = chrome.find_element(By.XPATH, '//*[@id="navbar-search-category-select-contents"]/ul/a')
#     advance_search.click()
#     advance_title_search = chrome.find_element(By.XPATH, '//*[@id="main"]/div[2]/div[1]/a')
#     advance_title_search.click()
#     sleep(3)
#     # advance search
#     featured_film_checkbox = chrome.find_element(By.XPATH, '//*[@id="title_type-1"]')
#     featured_film_checkbox.click()
#     from_when_input = chrome.find_element(By.XPATH, '//*[@id="main"]/div[3]/div[2]/input[1]')
#     from_when_input.send_keys(from_year)
#     to_when_input = chrome.find_element(By.XPATH, '//*[@id="main"]/div[3]/div[2]/input[2]')
#     to_when_input.send_keys(to_year)
#     movies_per_page_selection = chrome.find_element(By.XPATH, '// *[ @ id = "search-count"]')
#     Select(movies_per_page_selection).select_by_value("250")
#     sleep(1)
#     submit = chrome.find_element(By.XPATH, '//*[@id="main"]/p[3]/button')
#     submit.click()
#
#     current_url = chrome.current_url
#     return current_url


current_page = search_movies_per_page()
urls_list_ready = make_url_list(current_page)
# urls_list_ready = make_url_list('https://www.imdb.com/search/title/?title_type=feature&release_date'
#               '=2022-01-01,2022-01-15&count=250&start=1&ref_=adv_nxt')

# urls_list_ready = make_url_list('https://www.imdb.com/search/title/?'
#                                 'title_type=feature&release_date=2021-01-01,2021-01-15&sort=moviemeter,desc&count=50')
print('urls_list_ready\n', urls_list_ready)


def scrape(urls):
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(get_data_clean_it_and_input_data, urls, chunksize=5)
        return True
        # futures = []
        # for url in urls:
        #     futures.append(executor.submit(get_data_clean_it_and_input_data, url))
        #     time.sleep(0.2)
        # for future in concurrent.futures.as_completed(futures):
        #     print(future.result())
        # done, not_done = wait(futures, return_when=concurrent.futures.ALL_COMPLETED)
        # if movie_counts >= total_movies:
        #     print('STOPPPPPPPPPPPPPPPPP for next\n')
        #     return True


# while urls_list_ready != 0:
#     list_len = urls_list_ready[1]
#     scrape_done = scrape(urls_list_ready[0], list_len)
#
#     if scrape_done:
#         movie_counts = 0
#         continue_next = check_if_next_page()
#         if continue_next:
#             urls_list_ready = make_url_list(continue_next)
#         else:
#             print('global now\n', urls_list_ready)
#             urls_list_ready = False
#             print('global after\n', urls_list_ready)
#             break

    # if page_job_done:
    #     next_page = check_if_next_page()
    #     if not next_page:
    #         print('no next')
    #     else:
    #         new_url_list = make_url_list(next_page)




# while this_is_end is not True:
#     print('open nnew thread')
#     with ThreadPoolExecutor(max_workers=10) as executor:
#         executor.map(get_data_clean_it_and_input_data, all_urls)
#         total_movies = len(all_urls)
#         if total_movies <= 250 and movie_counts >= total_movies:
#             next_page = check_if_next_page()
#             print('next page \n', next_page)
#             if not next_page:
#                 this_is_end = True
#                 break
#             else:
#                 movie_counts = 0
#                 make_url_list(next_page)


if __name__ == '__main__':
    while urls_list_ready != 0:
        list_len = urls_list_ready[1]
        if list_len != 0:
            scrape_done = scrape(urls_list_ready[0])
            if scrape_done:
                # movie_counts = 0
                continue_next = search_movies_per_page(find_next=True)
                if continue_next:
                    urls_list_ready = make_url_list(continue_next)
                else:
                    urls_list_ready = False
                    break

    with open(f'{FILE_LOCATION}year_list.csv', 'a', newline='', encoding='utf8') as f:
        writer = csv.writer(f)
        writer.writerow([next_from_year, next_to_year])

    # drop duplicates
    df = pd.read_csv(f'{WATCHER_FOLDER}{file_name}.csv')
    df.drop_duplicates()

    # write an ok file to tell watcher move
    with open(f'{WATCHER_FOLDER}{file_name}_ok.csv', 'w', newline='', encoding='utf8') as f:
        writer = csv.writer(f)
        writer.writerow('ok')
