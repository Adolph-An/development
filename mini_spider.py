#!/usr/bin/python
#-*-coding: gbk-*-
"""
Mini spider

Description:
    This file implement mini spider.
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


# Global variables
logger = None
output_directory = None
max_depth = 1
crawl_interval = None
crawl_timeout = None
target_url = None
thread_count = 1

class URLList(SGMLParser):
    def __inti__(self):
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
    print ("-h       help information")
    print ("-v       version information")
    print ("-c[file] config file path")
    return

def spider_internal(url):
    """Spider internal

    Args:
        url

    Return:
        None
    """

    global thread_count
    global output_directory
    global logger
    global crawl_timeout

    logger.debug("spider_internal start(): url: %s" % url)

    try:
        page_file = urllib2.urlopen(url, timeout = crawl_timeout).readlines()
    except urllib2.URLError, e:
        logger.error(e.reason)
        thread_count += 1
        return

    page_file_path = output_directory + "/"
    page_file_path += urllib.quote(url).replace("/", "_") + ".html"

    page_link_list = URLList()
    try:
        f = open(page_file_path, "w")
        for page_file_line in page_file:
            page_link_list.feed(page_file_line)
            f.write(page_file_line)
        f.close()
    except IOError, e:
        logger.error(e[1])
        thread_count +=  1
        return

    link_pattern = re.compile(r target_url)
    for link in page_link_list.list:
        if re.match()

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

    if depth > max_depth:
        return
    while True:
        # Thread pool
        if thread_count:
            thread_count -= 1
            th = threading.Thread(target = spider_internal, args = (url,))
            th.start()
            break

def main(config_file):
    """Main function of mini spider

    Args:
        config_file: config file path.

    Return:
        None
    """

    # Read config file
    cf = ConfigParser.ConfigParser()
    cf.read(config_file)

    url_list_file = cf.get("spider", "url_list_file")
    global output_directory
    output_directory = cf.get("spider", "output_directory")
    global max_depth
    max_depth = cf.getint("spider", "max_depth")
    global crawl_interval
    crawl_interval = cf.getint("spider", "crawl_interval")
    global crawl_timeout
    crawl_timeout = cf.getint("spider", "crawl_timeout")
    global target_url
    target_url = cf.get("spider", "target_url")
    global thread_count
    thread_count = cf.getint("spider", "thread_count")

    # Config logging module
    global logger
    logger = logging.getLogger('')
    fmt = "%(asctime)s %(filename)s[%(lineno)s]: %(levelname)s %(message)s"
    formatter = logging.Formatter(fmt)
    log_file_handler = logging.FileHandler("mini_spider.log")
    log_file_handler.setFormatter(formatter)
    log_stream_handler = logging.StreamHandler(sys.stderr)
    logger.addHandler(log_file_handler)
    logger.addHandler(log_stream_handler)

    # Read urls file line by line
    try:
        url_file = open(url_list_file)
    except IOError, e:
        logger.error(e[1])
        sys.exit()

    for url_line in url_file:
        spider_start(url_line, 0)

if __name__ == "__main__":
    config_file = get_config_file()
    main(config_file)
