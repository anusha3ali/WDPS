import gzip
import os
import pandas as pd
from bs4 import BeautifulSoup
import csv 

KEYNAME = "WARC-TREC-ID"
HTML="<HTML"
DOCTYPE_HTML="<!DOCTYPE HTML"
ENDTAG="</HTML>"

def find_entities(payload):
    mydict = {}
    value=""
    if payload == '':
        return
    key = None
    x=payload.splitlines()
    for line in x:
      #  print(line)
        if line.startswith(KEYNAME):
            key = line.split(': ')[1]
        elif HTML.lower()  in (line.lower()) or DOCTYPE_HTML.lower() in line.lower():
            print(line)
            a=x.index(line)
        elif "</html>" in line:
            b=x.index(line)
            print(key)
            value=x[a:b]
            break
    return key,value; 

    


def split_records(stream):
    payload = ''
    for line in stream:
        if line.strip() == "WARC/1.0":
            yield payload
            payload = ''
        else:
            payload += line
    yield payload


path = os.getcwd()

print(path)

with open('innovators.csv', 'w', newline='', encoding='UTF-8') as file:
    writer = csv.writer(file)
    with gzip.open("..\WDPS\sample.warc.gz", 'rt', errors='ignore') as fo:
            for record in split_records(fo):
                x=find_entities(record)
                if x is not None and len(x)==2:
                    key,value=x
            #     dict["key"]=key
                #    dict["value"]=value
                    soup = BeautifulSoup("".join(value), 'html.parser')
                    writer.writerow([key, soup.get_text()])

            
           



           