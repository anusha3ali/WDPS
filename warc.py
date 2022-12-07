import gzip
import unicodedata
import argparse
import multiprocessing as mp
import csv
import datetime
from html import unescape
import os
import re

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


def _join_sentences(sentences):
    return '. '.join(sentences)


def _valid_word(word: str) -> bool:
    return len(word) > 0 and \
           not (re.match(r'[^a-zA-Z\d$€:.,-]', word) or (len(word) == 1 and re.match(r'[^a-zA-Z\d]', word)))


def _sanitize_word(word: str) -> str:
    if len(word) == 0:
        return word
    if word[-1] == '.':
        if not word[-2].isalnum():
            return word[:-2] + '.'
    elif not word[-1].isalnum():
        return word[:-1]
    return word


def _process_text(text: str) -> str:
    filtered_words = [_sanitize_word(word) for word in text.split(' ') if _valid_word(word)]

    tokenized_text = nltk.tokenize.sent_tokenize(" ".join(filtered_words))

    return " ".join(tokenized_text)


def _get_soup_text(html_soup):
    flag = 1
    if flag == 1:
        text_tags = [text_tag.text for text_tag in html_soup.find_all(re.compile('^h[1-6]$')) + html_soup.find_all('p') if text_tag.text is not None]
        return _join_sentences(text_tags)
    else:
        return html_soup.get_text()


def process_payload(warc_file):
    file_key, html_file = find_entities(warc_file)
    if file_key is not None:
        normalized_html = unicodedata.normalize("NFKC", unescape(" ".join(html_file)))

        html_soup = BeautifulSoup(normalized_html, "html.parser")

        title = html_soup.title
        title_text = ""
        if title is not None and title.string is not None:
            title_text = title.string

        headers = [header.text for header in html_soup.find_all(re.compile('^h[1-6]$'))]
        headers_text = _join_sentences(headers)

        all_text = _get_soup_text(html_soup)

        processed_title = _process_text(title_text)
        processed_headers = _process_text(headers_text)
        processed_all_text = _process_text(all_text)

        title_sentence = processed_title
        if len(title_sentence) > 0 and title_sentence[-1] != '.':
            title_sentence += '. '
        else:
            title_sentence += ' '

        return file_key, processed_title, processed_headers, (title_sentence + processed_all_text).strip()
    return None, None, None, None


def _valid_row(row):
    if len(row) < 4:
        return False

    if row[0] is not None and row[3] is not None and len(row[3]) > 0:
        return True
    return False


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
        if pool_size > 1:
            with mp.Pool(processes=pool_size) as pool:
                processed_files = pool.map(process_payload, split_records(fo))
        else:
            processed_files = [process_payload(payload) for payload in split_records(fo)]

    with open(f"{res_directory}/{file_name}.csv", 'w', newline='', encoding='UTF-8') as file:
        writer = csv.writer(file, quoting=csv.QUOTE_NONE, escapechar='\\')
        writer.writerows([row for row in processed_files if _valid_row(row)])
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
