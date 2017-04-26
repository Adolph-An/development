#!@PYTHON@
#!/bin/bash
import os
import sys
import json
import getopt

def check_target(t):
    for i in range(0, len(t)):
        if i["asn"] != "":
            # check asn
        if i["hostname"] != "":
            # check RP's hostname
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
    if !check_target(slurm["slurmTarget"]):
        return

    filter_list = slurm["validationOutputFilters"]
    assertions = slurm["locallyAddedAssertions"]

    try:
        pass
    except Exception as e:
        raise

def slurm_deal_dir(file_dir):
    for root, dirs, files in os.walk(file_dir):
        for d in dirs:
            slurm_deal_dir(os.path.join(root, d))
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
