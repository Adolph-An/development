#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Mini spider

Description:
    This file implement mini spider by multi thread
"""

import os
import sys
import getopt
import ConfigParser
import re
import logging
import threading
import urllib2
import urllib
from sgmllib import SGMLParser
import urlparse
import chardet
import time


# Global variables
logger = None
output_directory = None
max_depth = 1
crawl_interval = None
crawl_timeout = None
target_url = None
thread_count = 1
time_clock = time.time()

class URLList(SGMLParser):
    def __init__(self):
        SGMLParser.__init__(self)
        self.list = []

    def start_a(self, attrs):
        href = [v for k, v in attrs if k == 'href']
        if href:
            self.list.extend(href)

    def end_a(self):
        pass

def get_config_file():
    """Get config file from opts

    Returns:
        A confilg file path.

    """

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hvc:", ["help"])
    except getopt.GetoptError as err:
        # print help information and exit
        print str(err)
        usage()
        sys.exit(2)

    if len(opts) == 0:
        usage()
        sys.exit()

    config_file = None
    for o, a in opts:
        if o == "-v":
            print ("mini_spider version: 0.1")
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o == "-c":
            config_file = a
        else:
            print ("Unhandled option(s)")
            sys.exit()

    return config_file

def usage():
    # Print help information
    print ("")
    print ("    -h       help information")
    print ("    -v       version information")
    print ("    -c[file] config file path")
    print ("")
    return

def get_formatted_url(url, link):
    """To get formatted url

    Check whether the `link` is a relative path or an absolute path.
    And return a formatted absolute hyperlink

    Args:
        url: web page's url
        link: hyperlink in web page

    Returns:
        formatted_url: formatted absolute hyperlink
    """

    global logger
    formatted_url = None
    logger.debug("get_formatted_url() url: %r link: %r" %(url, link))

    # Unquote `url` and `link`
    url = urllib2.unquote(url)
    link = urllib2.unquote(link)

    # If look like "//www.baidu.com/index.html"
    if re.match(r"^//\w+.*.html?$", link):
        formatted_url = "http:" + link
    # If looks like "http://www.baidu.com/a.html", not change
    elif re.match(r"^http://w+.*.html?$", link):
        formatted_url = link
    # If looks like "a.html", "/b.html", "./c.html" or "../d.html"
    elif re.match(r"^(\.{0,2}/)?\w+.*.html?$", link):
        formatted_url = urlparse.urljoin(url, link)
    else:
        # Illegal link
        logger.error("Illeagal link")

    return formatted_url

def spider_internal(url, depth):
    """Spider internal

    Args:
        url: this page's url
        depth: this page's depth

    Return:
        None
    """

    global thread_count
    global output_directory
    global logger
    global crawl_timeout

    logger.info("spider_internal(): url: %r depth: %d" %(url, depth))

    # Get page by urllib2
    try:
        page_file = urllib2.urlopen(url, timeout=crawl_timeout).read()
        page_coding = chardet.detect(page_file)['encoding']
    except urllib2.URLError, e:
        logger.error("Get page %r error %s" %(url, e.reason))
        return
    except:
        logger.error("url:%r timeout:%d" %(url, crawl_timeout))
        return

    # Check and convert the encoding
    if page_coding == 'utf-8' or page_coding == 'UTF-8':
        pass
    else:
        logger.debug("Convert %r from %s to utf-8" %(url, page_coding))
        try:
            page_file = page_file.decode(page_coding, 'ignore').encode('utf-8')
        except LookupError, e:
            logger.error("Decode %r error, %s" %(url, e[0]))
            return

    # Set target url regex pattern
    try:
        target_url_pattern = re.compile("%r" %target_url)
    except re.error, e:
        logger.error("Invalid target_url %r, %s" %(target_url, e[0]))
        return

    # Build file path
    page_file_path = output_directory + "/"
    page_file_path += urllib.quote(url).replace("/", "_")
    if target_url_pattern.match(url):
        pass
    else:
        page_file_path += ".html"

    # Get all url links and save page
    page_link_list = URLList()
    try:
        f = open(page_file_path, "w")
        page_link_list.feed(page_file)
        f.write(page_file)
        f.close()
    except IOError, e:
        logger.error("Save page file error: %s" %e[1])
        return
    logger.debug("Save page file %s succeed!" %page_file_path)

    # Check all url links
    # If match(target_url):
    #    format the link
    #    spider_start(formatted link) by new thread
    for link in page_link_list.list:
        if target_url_pattern.match(link):
            formatted_url = get_formatted_url(url, link)
            thread_spider = threading.Thread( \
                            target=spider_start, \
                            args=(formatted_url, depth+1,))
            thread_spider.start()

def spider_start(url, depth):
    """Spider start with `url`

    Args:
        url
        depth

    Return:
        None
    """

    global max_depth
    global thread_count
    global crawl_interval
    global time_clock
    global logger

    logger.info("spider_start(): url: %r depth: %d" %(url, depth))

    if depth > max_depth:
        logger.info("%s over depth! max_depth: %d" %(url, max_depth))
        return

    # Thread pool
    if thread_count.acquire():
        # Check interval and crawl_interval
        interval = time.time() - time_clock
        if interval < crawl_interval:
            logger.debug("Waitting for crawl_interval...")
            time.sleep(crawl_interval - interval)
        time_clock = time.time()
        spider_internal(url, depth)
        thread_count.release()

def main(config_file):
    """Main function of mini spider

    Args:
        config_file: config file path.

    Return:
        None
    """

    global output_directory
    global max_depth
    global crawl_interval
    global crawl_timeout
    global target_url
    global thread_count
    global logger

    # Config logging module
    try:
        fmt = "%(asctime)s %(filename)s[%(lineno)-3s]: %(levelname)-8s %(message)s"
        logging.basicConfig(level=logging.DEBUG,
                            format=fmt,
                            datefmt='%H:%M:%S',
                            filename='mini_spider.log',
                            filemode='w')
        logger = logging.getLogger('')
        formatter = logging.Formatter(fmt)
        #log_file_handler.setFormatter(formatter)
        log_stream_handler = logging.StreamHandler()
        log_stream_handler.setFormatter(formatter)
        log_stream_handler.setLevel(logging.INFO)
        #logger.addHandler(log_file_handler)
        logger.addHandler(log_stream_handler)
    except logging.ERROR, e:
        print ("Logging Error")
        return

    # Read config file
    logger.info("Reading config file %s ..." %config_file)
    if not os.path.isfile(config_file):
        logger.error("Invalid config file path: %s" %config_file)
        return
    cf = ConfigParser.ConfigParser()
    cf.read(config_file)

    try:
        url_list_file = cf.get("spider", "url_list_file")
        output_directory = cf.get("spider", "output_directory")
        max_depth = cf.getint("spider", "max_depth")
        crawl_interval = cf.getint("spider", "crawl_interval")
        crawl_timeout = cf.getint("spider", "crawl_timeout")
        target_url = cf.get("spider", "target_url")
        thread_count = threading.Semaphore(cf.getint("spider", "thread_count"))
    except ConfigParser.Error, e:
        logger.error("ConfigParser error %s" %e)
        return

    # Check url_list_file and output_directory
    if not os.path.isfile(url_list_file):
        logger.error("Invalid url list file path: %s" %url_list_file)
        return
    if not os.path.exists(output_directory.rstrip("/")):
        logger.info("Create output directory: %s" %output_directory)
        try:
            os.makedirs(output_directory.rstrip("/"))
        except OSError as e:
            logger.error("Create output directory error")
            sys.exit()

    # Read urls file line by line
    logger.info("Reading url file %s ..."  %url_list_file)
    try:
        url_file = open(url_list_file)
    except IOError, e:
        logger.error(e[1])
        return

    for url_line in url_file:
        thread_spider = threading.Thread( \
                        target=spider_start, \
                        args=(url_line, 0,))
        thread_spider.start()

if __name__ == "__main__":
    config_file = get_config_file()
    if config_file:
        main(config_file)
