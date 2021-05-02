#!/usr/bin/python3

# mbox_getter.py
# download a (monthly) archive mbox file
# for processing to add to pipermail archiver
#
# 1 May 2021 by John Ackermann N8UR jra@febo.com
# no rights reserved

import os
from os import path
import sys
import time
import fileinput
import subprocess
import urllib.request
import urllib.error
import gzip
import shutil
import tempfile
from pathlib import Path
from datetime import datetime

#############################################################
# main config vars -- hopefully just change these to customize
list = "time-nuts"
list_domain = "lists.febo.com"
base_dir = "/var/www/febo.com/system/" # working dir created under this
archiver = "/var/lib/mailman/bin/arch"
archiver_list = "time-nuts_lists.febo.com"
###############################################################

# note the trailing slash on directory names
list_dir = base_dir + list + "_mbox/"
mbox_dir = list_dir + "mbox/"
diff_dir = list_dir + "diff/"
gzip_dir = list_dir + "gzip/"
cumulative_mbox_file = list_dir + list + "_cumulative.mbox"
base_cumulative_mbox_file = list + "_cumulative.mbox"

# these are used as global vars
base_file = ""
mbox_file = ""
gzip_file = ""
diff_file = ""
prior_gzip_file = ""
month_end_base_file = ""
month_end_gzip_file = ""
month_end_mbox_file = ""
month_end_diff_file = ""
prior_mbox_file_lines = 0

year = ""
month = ""
day ="" 
hour ="" 
minute = ""
second = ""
tz = ""
month_end_year = ""
month_end_month = ""

def main():
    global prior_mbox_file_lines
    global mbox_file_lines

    print()
    print("*** Files will be created under " + list_dir)

    # create the directories if necessary
    Path(list_dir).mkdir(parents=True, exist_ok=True)
    Path(mbox_dir).mkdir(parents=True, exist_ok=True)
    Path(diff_dir).mkdir(parents=True, exist_ok=True)
    Path(gzip_dir).mkdir(parents=True, exist_ok=True)

    # construct file names (global vars)
    make_file_names(list)

    # get gzipped mbox and unzip it
    get_gzipped_mbox(year, month, gzip_file)
    mbox_file_lines = unzip_file(gzip_file, mbox_file)
    if os.path.getsize(mbox_file) <=20: # no new content
        os.remove(gzip_file)
        os.remove(mbox_file)
        print("*** Empty mbox file, deleting new files and exiting...")
        print()
        sys.exit()

    # if no cumulative mbox, create it
    if not path.exists(cumulative_mbox_file):
        open(cumulative_mbox_file,'a').close;
        print("*** No cumulative mbox file found; creating " \
            + os.path.basename(cumulative_mbox_file))

    # see if there are existing diff files
    if len(sorted(os.listdir(diff_dir))) != 0:
        prior_mbox_file_lines = get_prior_mbox_lines(mbox_dir)
        diff_lines = mbox_file_lines - prior_mbox_file_lines
    else:
        # if not, assume that the downloaded
        # mbox is wholly new and should be archived
        copy_mbox_to_diff(mbox_file, diff_file)
        diff_lines = new_mbox_file_lines
        update_archive(diff_file, diff_lines)
        sys.exit()
      
    if diff_lines == 0:
        # there's no change so clean up and exit
        print("*** No new messages, deleting new files and exiting...")
        print()
        os.remove(gzip_file)
        os.remove(mbox_file)
        if os.path.isfile(prior_gzip_file):
            os.remove(prior_gzip_file)
        if os.path.isfile(month_end_mbox_file):
            os.remove(month_end_mbox_file)
        sys.exit()
    if diff_lines > 0:
        # a normal diff
        make_diff(diff_lines, mbox_file, diff_file)
        update_archive(diff_file, diff_lines)
        sys.exit()
    elif diff_lines < 0:
        # if new is smaller than old, it's a new period
        print()
        print("*** It's a new month, so downloading month-end mbox file...")
        dl = make_new_period_diff()
        update_archive(diff_file, dl)
        sys.exit()


########################### functions #################################
def get_timestamp():
    # need year and month to make download URL
    global year
    global month

    # return timestamp as list of strings
    # using UTC
    dt = datetime.utcnow()
    year = dt.strftime('%Y') # global
    month = dt.strftime('%m') # global
    day = dt.strftime('%d')
    hour = dt.strftime('%H')
    minute = dt.strftime('%M')
    second = dt.strftime('%S')
    tz = dt.strftime('%Z')
    return [year, month, day, hour, minute, second, tz]

#####
def make_base_file_name(list):
    # get and time to create unique
    # file names
    
    # if there's a look-back
    # mbox file at month end, we want it
    # to have an earlier timestamp than the
    # first file of the new month, so we
    # get a timestamp for its name and
    # then sleep a few seconds.
    global month_end_base_file
    global month_end_month
    global month_end_year

    # figure out last month/year
    dt = get_timestamp()
    month_end_base_file = list + str(dt[0]) + str(dt[1]) + str(dt[2]) \
        + str(dt[3]) + str(dt[4]) + str(dt[5])
    tmp_year = int(dt[0])
    tmp_month = int(dt[1])

    if tmp_month > 1:
        month_end_month = "{0:0>2}".format(str(tmp_month - 1))
        month_end_year = str(tmp_year)
    else:
        month_end_month = "{0:0>2}".format(str(12))
        month_end_year = str(tmp_year - 1)
    # sleep a bit to differentiate timestamps in filenames
    time.sleep(5)

    # now we can get current date and time
    dt = get_timestamp()
    base_file = list + str(dt[0]) + str(dt[1]) + str(dt[2]) \
        + str(dt[3]) + str(dt[4]) + str(dt[5])
    return base_file

#####
def make_file_names(list_name):
    global base_file
    global gzip_file
    global mbox_file
    global diff_file
    global month_end_gzip_file
    global month_end_mbox_file
    global month_end_diff_file

    base_file = make_base_file_name(list_name)
    gzip_file = gzip_dir + base_file + ".gz"
    mbox_file = mbox_dir + base_file + ".mbox"
    diff_file = diff_dir + base_file + ".diff"

    month_end_gzip_file = gzip_dir + month_end_base_file + ".gz"
    month_end_mbox_file = mbox_dir + month_end_base_file + ".mbox"
    month_end_diff_file = diff_dir + month_end_base_file + ".diff"

#####
def get_gzipped_mbox(tmpyear, tmpmonth, outfile):
    # we pass params to this function rather than
    # using the global vars because sometimes we need
    # to download the prior month's cumulative file.

    base_url = "https://" + list_domain + "/empathy/list/" \
            + list + "." + list_domain
    query = "/download?month=" + tmpyear + "-" + tmpmonth

    # download the gzipped mbox
    try:
        with urllib.request.urlopen(base_url + query, timeout=30) as response:
            fhand = open(outfile, 'wb')
            mbox_file_size = 0
            while True:
                info = response.read(100000)
                if len(info) < 1: break
                mbox_file_size = mbox_file_size + len(info)
                fhand.write(info)
            fhand.close()
            print("*** Downloaded " + os.path.basename(outfile) + " (" \
                    + str(mbox_file_size) + " characters)")
            if mbox_file_size == 0:
                print("*** No data downloaded, exiting...")
                print()
                os.remove(gzip_file)
                sys.exit()
    except urllib.error.URLError as e:
        print("*** URL Error:",str(e))
        return False;
    except ValueError:
        print("*** Invalid URL:",base_url + query)
        return False;
    except IOError as e:
        print("*** File error: ",str(e))
        return False;
    # success
    return True

def unzip_file(infile, outfile):
    try:
        with gzip.open(infile, 'rb') as inp:
            buf = bytearray(inp.read())
    except OSError as e:
        print("Couldn't open and read gzip file ", infile + \
                " (Error: " + str(e) + ") so exiting")
        sys.exit()

    try:
        with open(outfile, "w") as outp:
            outp.write(buf.decode("utf-8"))
    except OSError as e:
        print("Couldn't open and write " + os.path.basename(outfile) \
                + " (Error: " + str(e) + ") so exiting")
        sys.exit()

    # get the line count
    lines = count_lines(outfile)

    print("*** Created " + os.path.basename(outfile) + 
        " ("  + str(lines) + " lines)")

    return lines

# get last mbox file, if there is one
def get_prior_mbox_lines(mbox_dir):
    global prior_mbox_file

    lines = 0
    files = sorted(os.listdir(mbox_dir))
    if len(files) < 2:
        print("*** No prior mbox files found")
        return 0
    else:
        prior_mbox_file = files[-2] # the new file is last
        lines = count_lines(mbox_dir + prior_mbox_file)
        print("*** Last mbox file was " + prior_mbox_file + " (" \
            + str(lines) + " lines)")
    return lines

#####
def count_lines(infile):
    try:
        with open(infile) as inp:
            # get the line count
            lines = 0
            for line in inp.readlines():
                lines += 1
            return lines
    except OSError as e:
        print("Couldn't open " + infile + " for reading!")
        return 0
#####
def make_diff(diff_lines, in_file, diff_file):
    # normal update situation, adding new
    # messages to existing files so create diff
    with open(in_file) as inp:
        buf = inp.readlines()
        with open(diff_file,'w') as outp:
            # get the last diff_lines lines from file
            # subtract from zero because getting
            # tail means specifying a negative offset
            outp.writelines(buf[(0-diff_lines):])
        print("*** Created diff file " + os.path.basename(diff_file) \
            + "(" + str(diff_lines) + " lines)")
    return True    

#####
def make_new_period_diff():
    # Downloaded mbox starts over each month.  We should be
    # able to detect by new mbox file being smaller than the
    # last one.  (This should be a safe assumption unless the
    # the list has very low traffic.)
    # But... there may be new messages in the prior month file
    # that came in after our last run.  We don't want to lose
    # those.  So we do a final download of the prior month and
    # and add that at the beginning of the new month file so
    # the diff we create will include any final new messages from
    # prior, plus new messages from current.  Whew!

    global mbox_file_lines
    global prior_mbox_file_lines

    # get final mbox of last period and ungzip it
    get_gzipped_mbox(month_end_year, month_end_month, month_end_gzip_file)
    month_end_file_lines = unzip_file(month_end_gzip_file, month_end_mbox_file)

    if month_end_file_lines == prior_mbox_file_lines:
        print("*** No final prior month changes; deleting month-end files")
        print()
        tmp_file_lines = mbox_file_lines
        diff_lines = tmp_file_lines
        copy_mbox_to_diff(mbox_file,diff_file)
        if os.path.isfile(month_end_gzip_file):
            os.remove(month_end_gzip_file)
        if os.path.isfile(month_end_mbox_file):
            os.remove(month_end_mbox_file)
        return diff_lines
    else:
        # append new mbox file to final version of last month
        print("*** Creating merged diff file")
        merged_mbox_file = tempfile.TemporaryFile()
        with open(month_end_mbox_file) as fp:
            tmpbuf1 = fp.read()
        with open(mbox_file) as fp:
            tmpbuf2  = fp.read()
        tmpbuf1 += "\n"
        tmpbuf1 += tmpbuf2
        merged_mbox_file = "/tmp/mbox_getter.{}.tmp".format(os.getpid())
        with open(merged_mbox_file,'w') as fp:
            fp.write(tmpbuf1)
        tmp_file_lines = count_lines(merged_mbox_file)
        diff_lines = tmp_file_lines - prior_mbox_file_lines

        # create diff
        make_diff(diff_lines, merged_mbox_file, diff_file)
        print("*** Creating diff file" + os.path.basename(diff_file) \
             + " from " + os.path.basename(mbox_file) \
             + " (" + str(diff_lines) + " lines)")
        print()
        return diff_lines

#####
def copy_mbox_to_diff(mbox_file, diff_file):
    print("*** Creating diff file " + os.path.basename(diff_file))
    print("    as copy of " + os.path.basename(mbox_file) + \
            " (" + str(count_lines(mbox_file)) + ") lines")
    print()
    try:
        shutil.copyfile(mbox_file, diff_file)
    except:
        print("Couldn't create diff file " + os.path.basename(diff_file) \
            + " so exiting")
        sys.exit()
    return True

def update_archive(diff_file, diff_lines):
    if ((len(sys.argv) == 1) or (sys.argv[1] != "dry-run")):
        # first append diff to cumulative mbox
        with open(diff_file,'r') as inp:
            with open(cumulative_mbox_file,'a') as outp:
                outp.writelines(inp)
        print("*** Added " + str(diff_lines) + " lines to " \
            + os.path.basename(cumulative_mbox_file))

        # feed diff to archiver
        print("*** Running archiver to update pipermail")
        print()
        subprocess.run([archiver, archiver_list, diff_file])
    else:
        print("*** Dry run -- not adding to cumulative mbox or archiving")
        print()
    return


#########################################################################

if __name__ == '__main__':
    main()
    
