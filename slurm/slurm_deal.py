#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import json
import getopt
import socket
import MySQLdb
import ConfigParser
import pycurl
import logging
import time

cf = ConfigParser.ConfigParser()
# read config
cf.read("slurm.conf")
table_list = []

def log_init():
    # init log
    sh = logging.StreamHandler()
    fmt = "%(asctime)s %(filename)s[%(lineno)s]: %(levelname)s: %(message)s"
    try:
        log_path = cf.get("config", "logdir")+"slurm.log"
    except ConfigParser.NoOptionError, e:
        print "log_init() Error: %s" % e.args[1]
        log_path = "/usr/local/var/log/rpstir/slurm.log"
        print "Use default log path: %s" % log_path
    logging.basicConfig(level=logging.DEBUG, format=fmt, filename=log_path, filemode='w')
    sh.setLevel(logging.INFO)
    formatter = logging.Formatter(fmt)
    sh.setFormatter(formatter)
    logging.getLogger('').addHandler(sh)
    return logging

logger = log_init()

def db_init():
    global cf
    global logger
    logger.debug("db_init() started!")
    try:
        user = cf.get("db", "user")
        password = cf.get("db", "password")
        database = cf.get("db", "database")
        # connect database
        db = MySQLdb.connect("localhost", user, password, database)
        cursor = db.cursor()
    except MySQLdb.Error, e:
        logger.error("MySQL ERROR %d:%s" %(e.args[0], e.args[1]))
        sys.exit()
    except ConfigParser.Error, e:
        logger.error("ConfigParser ERROR %d: %s" %(e.args[0], e.args[1]))
        sys.exit()
    logger.debug("db_init() successed!")
    return db, cursor

def table_init(conp, cursor, asn):
    global logger
    global table_list
    max_num = -1
    min_num = -1
    flag = 0
    try:
        query = "select max(serial_num),min(serial_num) from slurm_target_%d;" % asn
        cursor.execute(query)
        max_min = cursor.fetchall()[0]
        max_num = max_min[0]
        min_num = max_min[1]
    except MySQLdb.Error, e:
        if e.args[0] == 1146:
            # 不存在slurm_target_%asn表，需要create
            flag = 1
        else:
            logger.error("table_init() Error!")
            return

    try:
        query = "select serial_num from rtr_update order by create_time desc limit 1;"
        cursor.execute(query)
        latest_num = cursor.fetchall()[0][0]
    except:
        logger.error("table_init() Error: Can't get latest serial number!")
        return

    if (max_num == latest_num and min_num != 0) or (min_num == latest_num and min_num == 0):
        # 存在有效的slurm_target_%asn表，不需要处理表
        flag = 0
    elif flag == 1:
        pass
    else:
        # 存在旧的slurm_target_%asn表，需要drop table
        flag = -1

    if flag == -1:
        try:
            cursor.execute("drop table slurm_target_%d" %asn)
            conp.commit()
        except:
            logger.error("table_init() Error: Drop table error")
            conp.rollback()
            return
    elif flag == 0:
        table_list.append("slurm_target_%d" %asn)
        return

    query = "create table slurm_target_%d select * from rtr_full;" % asn
    try:
        curosr.execute(query)
        table_list.append("slurm_target_%d" %asn)
    except MySQLdb.Error, e:
        logger.error("MySQL ERROR %d:%s" %(e.args[0], e.args[1]))
        sys.exit()

def form_ip(ip):
    iparray4 = ip.split('.')
    iparray6 = ip.split(':')
    str = ""
    if len(iparray4) > 1:
        # ipv4 address
        for i in iparray4:
            str += "%02x" % int(i)
    elif len(iparray6) > 1:
        for i in range(0, len(iparray6) - 1):
            if iparray6[i] != "":
                str += "{:0>4}".format(iparray6[i])
            else:
                for j in range(0, 9 - len(iparray6)):
                    str += "{:0>4}".format(iparray6[i])
        str += "{:0>4}".format(iparray6[len(iparray6)-1])
    return str.upper()

def check_target(conp, cursor, t):
    # 声明全局变量
    global cf
    global table_list
    global logger
    # 初始化需要修改的数据库表的列表
    table_list = []
    logger.debug("check_target()......")
    ASn = json.loads(cf.get("target", "asn"))
    HostName = cf.get("target", "hostname")
    res = (len(t) == 0)

    for i in t:
        try:
            if i["asn"] != "":
                # check asn
                try:
                    ASn.index(i["asn"])
                    res = True
                    table_init(conp, cursor, i["asn"])
                except ValueError:
                    continue
        except KeyError:
            pass
        try:
            if i["hostname"] != "":
                # check RP's hostname
                if not i["hostname"] == HostName:
                    table_list = []
                    res = False
                    break
        except KeyError:
            pass
    logger.debug("check_target() returned: %r" % res)
    return res

def slurm_deal_file(file_path):
    global logger
    global table_list
    global conp
    global cursor

    logger.info("slurm_deal_file() start: file: %s" % file_path)
    if (os.path.splitext(file_path)[1] != '.json'):
        logger.error("Slurm file extension SHOULD be .json")
        return

    try:
        with open(file_path, 'r') as f:
            slurm = json.load(f)
    except IOError, e:
        logger.error("IOError %d:%s" %(e.args[0], e.args[1]))
        return

    try:
        # check slurm version
        if slurm["slurmVersion"] != 1:
            logger.info("Slurm file version SHOULD be 1")
            return

        # check slurm target
        if check_target(conp, cursor, slurm["slurmTarget"]) != True:
            logger.info("Wrong target, Ingore file:%s" %file_path)
            return
        filter_list = slurm["validationOutputFilters"]
        assertions = slurm["locallyAddedAssertions"]
    except KeyError, e:
        logger.error("File syntax error: No Key:%s" %e)
        return

    if filter_list != "":
        # 循环处理所有的过滤条目
        try:
            prefixFilters = filter_list["prefixFilters"]
            # bgpsecFilters = filter_list["bgpsecFilters"]
        except KeyError, e:
            logger.error("File syntax error : No Key:%s" %e)
            return

        for item in prefixFilters:
            try:
                asn, prefix, length, max_length = get_value(item)
            except "FileSyntaxError":
                continue
            if table_list == []:
                # 未指定target中的asn, 从rtr_full中删除条目
                res = delete_from_table(conp, cursor, "rtr_full", asn, prefix, length, max_length)
            else:
                for table in table_list:
                    res = delete_from_table(conp, cursor, table, asn, prefix, length, max_length)
                    if res < 0:
                        break
            if res < 0:
                logger.error("Error occured when deal filter list. file:%s prefix:%s" %(file_path, prefix))

    if assertions != "":
        # 循环处理所有的插入条目
        try:
            prefixAssertions = assertions["prefixAssertions"]
            # bgpsecAssertions = assertions["bgpsecAssertions"]
        except KeyError, e:
            logger.error("File syntax error : No Key:%s" %e)
            return

        for item in prefixAssertions:
            try:
                asn, prefix, length, max_length = get_value(item)
            except "FileSyntaxError":
                continue

            # 添加条目中必须包含prefix和asn两个字段
            if prefix == "" or asn == -1:
                logger.error("File syntax error: Assertions MUST include both 'asn' and 'prefix'!")
                continue

            if table_list == []:
                res = insert_into_table(conp, cursor, "rtr_full", asn, prefix, length, max_length)
            else:
                for table in table_list:
                    res = insert_into_table(conp, cursor, table, asn, prefix, length, max_length)
                    if res < 0:
                        break
            if res < 0:
                logger.error("Error occured when deal assertions list. file:%s prefix:%s" %(file_path, prefix))

    logger.info("slurm_deal_file() end: file: %s" %file_path)

def get_value(item):
    # 如果prefix不能被分割成两个字符串，说明格式错误
    try:
        prefix, length = item["prefix"].split('/')
    except KeyError,e:
        prefix = ""
        length = -1
    except ValueError:
        logger.error("prefix syntax error: %s" %item["prefix"])
        raise Exception("FileSyntaxError")

    # 检查是否有asn字段
    try:
        asn = item["asn"]
    except KeyError,e:
        if prefix == "":
            # 没有prefix和asn字段，报错
            logger.error("File syntax error: No prefix, No asn!")
            raise Exception("FileSyntaxError")
        else:
            asn = -1

    # 检查是否有max prefix length字段
    try:
        max_length = item["maxPrefixLength"]
    except KeyError,e:
        max_length = length

    # asn格式应该为整数
    try:
        asn = int(asn)
    except ValueError:
        logger.info("asn syntax error: %s" %item["asn"])
        raise Exception("FileSyntaxError")
    # 验证ip prefix的合法性
    if not valid_ip(prefix):
        if prefix != "":
            logger.info("invalid prefix:%s" %item["prefix"])
            raise Exception("FileSyntaxError")
    # 验证前缀长的合法性
    try:
        length = int(length)
        max_length = int(max_length)
    except ValueError:
        logger.info("prefix length: %s/max length:%s should be int" %(length, max_length))
        raise Exception("FileSyntaxError")
    return asn,prefix,length,max_length

def delete_from_table(conp, cursor, table, asn, prefix, length, max_length):
    global logger
    logger.debug("delete_from_table(): table:%s asn:%d prefix:%s/%d-%d" %(table, asn, prefix, length, max_length))
    # 构建delete SQL 语句
    smt = "delete from %s where" % table
    if asn != -1:
        smt += " asn=%d" % asn
        if prefix != "":
            smt += " and"
    if prefix != "":
        smt += " prefix=unhex('%s') and prefix_length=%d and prefix_max_length=%d" %(form_ip(prefix), length, max_length)
    smt += ";"
    try:
        cursor.execute(smt)
        conp.commit()
    except:
        conp.rollback()
        return -1
    return 1

def insert_to_other(conp, cursor, asn, prefix, length, max_length, flag):
    global logger
    logger.debug("insert_to_other() started: asn:%d prefix:%s length:%d max_length:%d" %(asn, prefix, length, max_length))
    global table_list
    table_query = ""
    for i in table_list:
        table_query += "into %s " %i
    smt = "insert all %s values(%d,unhex('%s'),%d,%d,%d);" %(table_query, asn, form_ip(prefix), length, max_length, flag)
    try:
        cursor.execute(smt)
        conp.commit()
    except MySQLdb.Error, e:
        if e.args[0] == 1062:
            return 1
        return -1
    return 1

def insert_into_table(conp, cursor, table, asn, prefix, length, max_length):
    global logger
    logger.debug("insert_into_table(): table:%s asn:%d prefix:%s/%d-%d" %(table, asn, prefix, length, max_length))
    smt = "select max(serial_num),min(serial_num) from %s;" % table
    try:
        cursor.execute(smt)
        max_min = cursor.fetchall()[0]
        max_num = max_min[0]
        min_num = max_min[1]
        if max_num == 4294967295 and min_num == 0:
            max_num = 0
    except:
        logger.error("Can't get max serial_num!")
        return -1
    smt = "insert into %s values(%d,%d,unhex('%s'),%d,%d);" %(table, max_num, asn, form_ip(prefix), length, max_length)
    try:
        cursor.execute(smt)
        conp.commit()
    except MySQLdb.Error, e:
        conp.rollback()
        return -1
    return 1

def valid_ip(ip):
    if not ip or '\x00' in ip:
        return False
    try:
        res = socket.getaddrinfo(ip, 0, socket.AF_UNSPEC, socket.SOCK_STREAM, 0, socket.AI_NUMERICHOST)
        return bool(res)
    except socket.gaierror as e:
        if e.args[0] == socket.EAI_NONAME:
            return False
        raise
    return True

def slurm_deal_dir(file_dir):
    for root, dirs, files in os.walk(file_dir):
        for f in files:
            slurm_deal_file(os.path.join(root, f))

def slurm_deal_url(url):
    dirs = cf.get("config", "cachedir") + "%s/" % time.time()
    try:
        cmd = "wget -r -nd -P %s %s" %(dirs, url)
        os.system(cmd)
    except:
        logger.error("slurm_deal_url(): wget Error!")
        return
    slurm_deal_dir(dirs)
    return

try:
    opts, args = getopt.getopt(sys.argv[1:], "f:d:u:",["file=", "dir=", "url="])
except getopt.GetoptError, err:
    # print help information and exit:
    print str(err)
    sys.exit(2)

#Default variables
slurm_file = ""
slurm_dir = ""
slurm_url = ""

#Parse the options
for o, a in opts:
    if o in ("-f", "--file"):
        slurm_file = a
    elif o in ("-d", "--dir"):
        slurm_dir = a
    elif o in ("-u", "--url"):
        slurm_url = a
    else:
        print "unhandled option"
        sys.exit(1)


conp, cursor = db_init()

if slurm_file != "":
    slurm_deal_file(slurm_file)

if slurm_dir != "":
    slurm_deal_dir(slurm_dir)

if slurm_url != "":
    slurm_deal_url(slurm_url)

conp.close()
