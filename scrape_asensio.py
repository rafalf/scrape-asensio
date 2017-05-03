#!/usr/bin/env python


# win: pip install lxml==3.6.0  (other pip install lxml)
# pip install requests
# pip install beautifulsoup4

import requests
from requests import ConnectionError
from bs4 import BeautifulSoup
import os
import sys
import getopt
import logging
import time
import csv

scrape_url = 'http://www.asensio.com/research-reports-by-company/'
logger = logging.getLogger(os.path.basename(__file__))

ARTICLE = 'article_id-{}.htm'
METADATA = 'metadata_id-{}.csv'


def scrape(fld, from_date, to_date):

    total_counter = 0

    logger.info('Scraping: {} for all articles'.format(scrape_url.format("1")))
    r = requests.get(scrape_url)

    # create download folder
    if not fld:
        downloads_folder = os.path.join(os.path.dirname(__file__), 'download')
    else:
        downloads_folder = os.path.join(os.path.dirname(__file__), fld)
    if not os.path.isdir(downloads_folder):
        os.mkdir(downloads_folder)

    soup = BeautifulSoup(r.text, 'lxml')
    table = soup.find("table")

    strong_reserach = table.select("tr td:nth-of-type(1)")
    other = table.select("tr td:nth-of-type(2)")

    strong_companies = []
    for a in strong_reserach[0].find_all('a'):
        href = a.get('href')
        strong_companies.append(href)
        logger.info('Add strong sell research href: {}'.format(href))

    other_companies = []
    for a in other[0].find_all('a'):
        href = a.get('href')
        other_companies.append(href)
        logger.info('Add other companies href: {}'.format(href))

    all_companies = [strong_companies, other_companies]

    for x, companies in enumerate(all_companies):

        for url in companies:

            metadata_share = []
            metadata_share.append(time.strftime('%B %d, %Y', time.localtime()))

            logger.info('--------------------------------------------')

            if x == 0:
                logger.info('Category: Strong Sell Research')
                metadata_share.append('Strong Sell Research')
            else:
                logger.info('Category: Other Coverage')
                metadata_share.append('Other Coverage')

            logger.info('Scraping company url: {}'.format(url))
            r = requests.get(url)

            soup = BeautifulSoup(r.text, 'lxml')

            # heading
            # company name
            company_heading = soup.find('h1', class_='main_title')
            comp_name = company_heading.text.strip()
            metadata_share.append(comp_name)
            logger.info('Company name: {}'.format(comp_name))

            table_comp = soup.find('table', class_="tablesorter")
            table_comp = table_comp.find('tbody')

            # rows
            for counter, row in enumerate(table_comp.find_all('tr')):

                metadata = list(metadata_share)

                logger.info('Article {} out of {}.'.format(counter, len(table_comp.find_all('tr'))))

                # columns
                cols = row.find_all('td')

                article_id = cols[0].text.strip()
                logger.info('Article id: {}'.format(article_id))

                date_ = cols[1].text.strip()
                logger.info('Publish date found: {}'.format(date_))
                metadata.append(date_)

                post_date = time.strptime(date_, '%B %d, %Y')
                post_date_secs = time.mktime(post_date)
                logger.info('Date in secs: %s' % post_date_secs)

                link_ = cols[2].find('a')
                href_ = link_.get('href')
                logger.info('Report url: {}'.format(href_))
                metadata.append(href_)

                logger.info('Report title: {}'.format(link_.text.strip().encode("utf-8")))
                metadata.append(link_.text.strip().encode("utf-8"))

                if from_date < post_date_secs < to_date:

                    logger.info('Between start and end date -> Process')

                    # folder
                    split = date_.split(' ')
                    year_numeric = split[2]
                    month_alphabetic = time.strptime(split[0], "%B")
                    month_numeric = time.strftime("%m", month_alphabetic)
                    day_without_leading = time.strptime(split[1].strip(','), "%d")
                    day_numeric = time.strftime("%d", day_without_leading)
                    folder_struc = os.path.join(downloads_folder, comp_name, year_numeric, month_numeric, day_numeric)
                    if not os.path.isdir(folder_struc):
                        os.makedirs(folder_struc)
                        logger.info('Folders created: %s' % folder_struc)

                    row = ['Processed Time', 'Category', 'Company Name', 'Publish Date', 'Report URL', 'Report Title']
                    _write_row(row, os.path.join(folder_struc, METADATA.format(article_id)))
                    _write_row(metadata, os.path.join(folder_struc, METADATA.format(article_id)))

                    for _ in range(3):

                        try:
                            # get requests
                            request = requests.get(href_, timeout=30, stream=True)
                            file_ = os.path.join(folder_struc, ARTICLE.format(article_id))
                            with open(file_, 'wb') as fh:
                                for chunk in request.iter_content(chunk_size=1024):
                                    fh.write(chunk)
                            logger.info('Downloaded as: {}'.format(file_))
                            break
                        except ConnectionError:
                            logger.info('ConnectionError --> retry up to 3 times')
                    else:
                        logger.error('ERROR: Failed to download')

                    total_counter += 1

                else:
                    logger.info('Not between start and end date -> Skip')
                    logger.info('! {} < {} < {}'.format(from_date, post_date_secs, to_date))

        logger.info('Total articles saved: {}'.format(total_counter))


def _write_row(row, full_path):
    with open(full_path, 'ab') as hlr:
        wrt = csv.writer(hlr, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        wrt.writerow(row)
        logger.debug('Added to %s file: %s' % (full_path, row))


if __name__ == '__main__':
    download_folder = None
    verbose = None
    from_date = '01/01/1900'
    to_date = '01/01/2100'

    log_file = os.path.join(os.path.dirname(__file__), 'logs',
                                time.strftime('%d%m%y', time.localtime()) + "_scraper.log")
    file_hndlr = logging.FileHandler(log_file)
    logger.addHandler(file_hndlr)
    console = logging.StreamHandler(stream=sys.stdout)
    logger.addHandler(console)
    ch = logging.Formatter('[%(levelname)s] %(message)s')
    console.setFormatter(ch)
    file_hndlr.setFormatter(ch)

    argv = sys.argv[1:]
    opts, args = getopt.getopt(argv, "o:vf:t", ["output=", "verbose", "from=", "to="])
    for opt, arg in opts:
        if opt in ("-o", "--output"):
            download_folder = arg
        elif opt in ("-f", "--from"):
            from_date = arg
        elif opt in ("-t", "--to"):
            to_date = arg
        elif opt in ("-v", "--verbose"):
            verbose = True

    str_time = time.strptime(from_date, '%m/%d/%Y')
    from_secs = time.mktime(str_time)

    str_time = time.strptime(to_date, '%m/%d/%Y')
    to_secs = time.mktime(str_time)

    if verbose:
        logger.setLevel(logging.getLevelName('DEBUG'))
    else:
        logger.setLevel(logging.getLevelName('INFO'))

    logger.info('CLI args: {}'.format(opts))
    logger.info('from: {}'.format(from_date))
    logger.info('to: {}'.format(to_date))
    logger.debug('from_in_secs: {}'.format(from_secs))
    logger.debug('to_in_secs: {}'.format(to_secs))

    scrape(download_folder, from_secs, to_secs)