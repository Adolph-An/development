import os,sys
from Queue import Queue
import threading
import time
import urllib2
import urllib
import cookielib
import socket
from sgmllib import SGMLParser

class Lister(SGMLParser):
    def __init__(self):
        self.urls = []
    def start_a(self, attrs):
        href = [v for k, v in attrs if k=='href']
        if href:
            self.urls.extend(href)
    def reset(self):
        SGMLParser.reset(self)

thread_queue = None
thread_count = 10

def main():
    lister = Lister()
    data = urllib.urlopen("http://www.baidu.com").read()
    lister.feed(data)
    print lister.urls

if __name__ == "__main__":
    main()
