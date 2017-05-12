#!@PYTHON@
#!/bin/bash
#coding: utf-8
import os
import sys
import json
import getopt
import pymysql as db
import time
from OpenSSL import crypto
import pdb

# sys.setrecursionlimit(10000)
path = "/usr/local/var/cache/rpstir"
count = int(sys.argv[1])
count1 = 0
count2 = 0
count3 = 0
count4 = 0

def build_str(components):
    str=''
    for i in components:
        str+='/'+i[0]+'='+i[1]
    return str
def byMySQL():
    start = time.time()
    config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'passwd': '0000',
        'charset': 'utf8mb4',
        'cursorclass': db.cursors.DictCursor
        }
    conn = db.connect(**config)
    conn.select_db('test')
    conn.autocommit(1)
    cursor = conn.cursor()
    tmp = 0
    global count
    
    for root, dirs, files in os.walk(path):
        for f in files:
            if tmp == count:
                break
            if os.path.splitext(f)[1][1:] == "cer":
                tmp += 1
                cert = crypto.load_certificate(crypto.FILETYPE_ASN1, open(os.path.join(root, f), 'rt').read())
                sub = cert.get_subject()
                iss = cert.get_issuer()
                subject = build_str(sub.get_components())
                issuer = build_str(iss.get_components())
                filename = os.path.join(root,f)
                if in_database(cursor, subject, issuer):
                    continue
                build_path_up_mysql(cursor, subject, issuer)
                build_path_down_mysql(cursor, subject, issuer)
                cursor.execute("INSERT INTO test values('%s','%s','%s','','')" %(filename,subject,issuer))
        if tmp == count:
            break

    cursor.close()
    conn.close()
    end = time.time() - start

    print "By MySQL spend time: " + str(end)

def in_database(cursor, subject, issuer):
    cnt = cursor.execute("select filename from test where subject='%s' and issuer='%s'" %(subject, issuer))
    if cnt != 0:
        return True
    return False

def build_path_up_mysql(cursor, subject, issuer):
    cursor.execute("SELECT filename,subject,issuer from test where subject='%s'" %issuer)
    global count1
    for res in cursor.fetchall():    
        if res["subject"] == res["issuer"]:
            count1 += 1
            return
        build_path_up_mysql(cursor, res["subject"], res["issuer"])

def build_path_down_mysql(cursor, subject, issuer):
    cursor.execute("SELECT filename,subject,issuer from test where issuer='%s' and subject!=issuer" %subject)
    global count2
    for res in cursor.fetchall():
        # pdb.set_trace()
        count2 += 1
        if res["subject"] == subject:
            return
        build_path_down_mysql(cursor, res["subject"], res["issuer"])

# execute!
byMySQL()

json_path_up = "/home/adolphanchn/github/development/map_on_cert_chain/up.json"
json_path_down = "/home/adolphanchn/github/development/map_on_cert_chain/down.json"

def byMap():
    start = time.time()
    if os.path.exists(json_path_up):
        with open(json_path_up, 'r') as f:
            map_json_up = json.load(f)
    else:
        map_json_up = {}

    if os.path.exists(json_path_down):
        with open(json_path_down, 'r') as f:
            map_json_down = json.load(f)
    else:
        map_json_down = {}
    tmp = 0
    global count

    for root, dirs, files in os.walk(path):
        for f in files:
            if tmp == count:
                break
            if os.path.splitext(f)[1][1:] == "cer":
                tmp += 1
                cert = crypto.load_certificate(crypto.FILETYPE_ASN1, open(os.path.join(root, f), 'r').read())
                sub = cert.get_subject()
                iss = cert.get_issuer()
                subject = build_str(sub.get_components())
                issuer = build_str(iss.get_components())
                if in_json(map_json_up, subject):
                    continue
                build_path_up_map(map_json_up, subject, issuer)
                build_path_down_map(map_json_down, subject, issuer)
                map_json_up[subject] = issuer
                try:
                    map_json_down[issuer].append(subject)
                except KeyError:
                    map_json_down[issuer] = [subject]
        if tmp == count:
            break
    with open(json_path_up, 'w') as f:
        json.dump(map_json_up, f)
    with open(json_path_down, 'w') as f:
        json.dump(map_json_down, f)
    end = time.time() - start
    print "By Hash_Map spend time: " + str(end)
    print "len(json_up):%d" %len(map_json_up)
    print "len(json_down):%d" %len(map_json_down)

def in_json(map_json_up, subject):
    try:
        issuer = map_json_up[subject]
    except KeyError:
        return False
    return True

def build_path_up_map(map_json_up, subject, issuer):
    while True:
        global count3
        try:
            if subject == issuer:
                count3 += 1
                break
            subject = issuer
            issuer = map_json_up[issuer]
        except KeyError:
            break
def build_path_down_map(map_json_down, subject, issuer):
    global count4
    try:
        for i in map_json_down[subject]:
            count4 += 1
            if i == subject:
                return
            build_path_down_map(map_json_down, i, subject)
        return
        # issuer = subject
        # subject = map_json[subject]
    except KeyError:
        return

# execute!
byMap()

print "up by mysql recursion count:%d" %count1
print "down by mysql recursion count:%d" %count2
print "up by json recursion count:%d" %count3
print "down by json recursion count:%d" %count4
