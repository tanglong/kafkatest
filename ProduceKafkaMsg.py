#!/usr/bin/python
#coding: utf-8

import random
import string
import time
import os
import sys

def system(cmd):
    ret = os.system(cmd)
    if ret!=0:
        sys.exit(ret)

def gen():
    inputPath = "log"
    countFile = 0
    countLine = 0
    for fd, subfds, fns in os.walk(inputPath):
        for filename in fns:
            pathname = os.path.join(fd, filename)
            with open(pathname, 'r') as src:
                countFile = countFile + 1
                for line in src:
                    countLine = countLine + 1
                    command = 'echo "'
                    command += line.rstrip('\n')
                    command += '" | '
                    command += "/usr/local/kafka/bin/kafka-console-producer.sh --topic wgame_log --broker-list 10.105.19.63:9092"
                    system(command)
                    print(command)

    print(countFile)
    print(countLine)

gen()
