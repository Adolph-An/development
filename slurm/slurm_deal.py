#!@PYTHON@
#!/bin/bash
import os
import sys
import json
import getopt
import socket

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
    return str

def check_target(t):
    for i in range(0, len(t)):
        if i["asn"] != "":
            # check asn
            return False
        if i["hostname"] != "":
            # check RP's hostname
            return False
    return True

def slurm_deal_file(file_path):
    if (os.path.splitext(file_path)[1] != '.json'):
        print "File extension SHOULD be .json"
        return

    with open(file_path, 'r') as f:
        slurm = json.load(f)

    # check slurm version
    if slurm["slurmVersion"] != 1:
        print "slurm version SHOULD be 1"
        return

    # check slurm target
    if check_target(slurm["slurmTarget"]) != True:
        return

    filter_list = slurm["validationOutputFilters"]
    assertions = slurm["locallyAddedAssertions"]

    if filter_list != "":
        # 循环处理所有的过滤条目
        prefixFilters = filter_list["prefixFilters"]
        bgpsecFilters = filter_list["bgpsecFilters"]
        for item in prefixFilters:
            # 如果prefix不能被分割成两个字符串，说明格式错误
            try:
                prefix, prefix_length = item["prefix"].split('/')
            except Exception:
                print "prefix syntax error"
                continue
            asn = item["asn"]
            # asn格式应该为整数
            if not isinstance(asn, int):
                print "asn syntax error"
                continue


def slurm_deal_dir(file_dir):
    for root, dirs, files in os.walk(file_dir):
        for f in files:
            slurm_deal_file(os.path.join(root, f))

try:
    opts, args = getopt.getopt(sys.argv[1:], "f:d:",["file=", "dir="])
except getopt.GetoptError, err:
    # print help information and exit:
    print str(err)
    sys.exit(2)

#Default variables
slurm_file = ""
slurm_dir = ""

#Parse the options
for o, a in opts:
    if o in ("-f", "--file"):
        slurm_file = a
    elif o in ("-d", "--dir"):
        slurm_dir = a
    else:
        print "unhandled option"
        sys.exit(1)

if slurm_file != "":
    slurm_deal_file(slurm_file)

if slurm_dir != "":
    slurm_deal_dir(slurm_dir)
