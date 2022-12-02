import gzip
import unicodedata
import argparse
import multiprocessing as mp
import csv
import datetime
from html import unescape
import os

import nltk
from bs4 import BeautifulSoup


def find_entities(payload: str) -> (str, [str]):
    if payload == '':
        return None, None

    key = None
    html_type = False

    lines = payload.splitlines()

    # WARC line always at index 2 in test input. TODO check if this assumption holds
    warc_trec_line = lines[2]
    if warc_trec_line.startswith("WARC-TREC-ID"):
        key = warc_trec_line.split(': ')[1]

    if key is None:
        return None, None

    max_i = len(lines)
    i = 10

    while i < max_i:
        line = lines[i].lower()
        i += 1
        if line.startswith("content-type") and "html" in line:
            html_type = True
        if line == "":  # Always newline after HTTP request.
            break

    if html_type is False:
        return None, None

    return key, lines[i:]


def split_records(stream):
    payload = ''
    for line in stream:
        if line.strip() == "WARC/1.0":
            yield payload
            payload = ''
        else:
            payload += line
    yield payload


def process_payload(warc_file):
    file_key, html_file = find_entities(warc_file)
    if file_key is not None:
        normalized_html = unicodedata.normalize("NFKC", unescape(" ".join(html_file)))
        html_soup = BeautifulSoup(normalized_html, "html.parser")
        text = html_soup.get_text()
        tokenized_text = nltk.tokenize.word_tokenize(text)
        # Tokenize doesn't do anything right now (except for removing whitespace). TODO define proper behaviour
        return file_key, " ".join(tokenized_text)
    return None, None


def process_warc_zip(file_name):
    # Dependency of nltk.tokenize
    nltk.download("punkt")

    res_directory = "pre-proc"
    if not os.path.exists(res_directory):
        os.makedirs(res_directory)

    with gzip.open("data/warcs/sample.warc.gz", 'rt', errors='ignore') as fo:
        pool_size = mp.cpu_count()

        # Force single threaded behaviour for debugging.
        # pool_size = 1
        with mp.Pool(processes=pool_size) as pool:
            processed_files = pool.map(process_payload, split_records(fo))

    with open(f"{res_directory}/{file_name}.csv", 'w', newline='', encoding='UTF-8') as file:
        writer = csv.writer(file)
        writer.writerows([[key, val] for key, val in processed_files if key is not None])
        print(f"{os.getcwd()}/{file.name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser("wdp")
    parser.add_argument(
        "--warc_output",
        dest="filename",
        required=False,
        help="A file name for the preprocessed warc zip.",
        type=str
    )
    args = parser.parse_args()

    if args.filename is None:
        warc_filename = f'warcs-{datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}'
    else:
        warc_filename = args.filename

    process_warc_zip(warc_filename)
