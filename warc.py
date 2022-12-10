import gzip
import unicodedata
import argparse
import multiprocessing as mp
import csv
import datetime
from html import unescape
import os
import re
from io import TextIOWrapper
from typing import Iterator, List, Tuple, Union

import nltk
from bs4 import BeautifulSoup


def _find_html(payload: str) -> Union[Tuple[str, List[str]], Tuple[None, None]]:
    """Finds the WARC-TREC-ID and HTML content in a warc file.
    Gives back None, None if the key or HTML wasn't found.

    :param payload: The payload is an entire warc file. It contains information on the WARC file followed by file header
    information like the file type and lastly contains the body of the file.
    :type payload: str.
    :return: Pair of the WARC-TREC-ID and HTML body. Returns None, None if no key or no html was found.
    :rtype: Union[Tuple[str, List[str]], Tuple[None, None]]
    """
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

    # lines[i:] contains the entire body.
    return key, lines[i:]


def split_records(stream: TextIOWrapper) -> Iterator[str]:
    """Splits the stream of warc files into separate warc files using the "WARC/1.0" flag.
    Gives back an iterator to step over the warc files.

    :param stream: Text from the entire warc zip given as an IO stream.
    :type stream: TextIOWrapper
    :return: Yields the payload of a single warc file.
    :rtype: Iterator[str]
    """
    payload = ''
    for line in stream:
        if line.strip() == "WARC/1.0":
            yield payload
            payload = ''
        else:
            payload += line
    yield payload


def _join_sentences(sentences: List[str]) -> str:
    """Join sentences with a dot for later processing as sentences in the entity linking and relation extraction.

    :param sentences: List of sentences
    :type sentences: List[str]
    :return: All sentences combined into a single string, separating sentences with a dot.
    :rtype: str
    """
    return ' '.join([
        sentence
        if len(sentence) > 0 and sentence[-1] == '.'
        else sentence + "."
        for sentence in sentences
    ])


def _valid_word(word: str) -> bool:
    """Filters invalid words that cannot be processed in later stages.
    Invalid words include:
    - Empty words: len of 0.
    - Words only containing one symbol: len of 0 and contains symbol.
    - Words containing invalid symbols: valid symbols include alphanumerical, '$', '€', ':', '.', ',', and '-'.

    :param word: A single word directly taken from the HTML file.
    :type word: str
    :return: True if the word is valid, False otherwise.
    :rtype: bool
    """
    # TODO might want to adjust regex so that it actually processes proper money format, dates, proper punctuation.
    return len(word) > 0 and \
           not (re.match(r'[^a-zA-Z\d$€:.,-]', word) or (len(word) == 1 and re.match(r'[^a-zA-Z\d]', word)))


def _sanitize_word(word: str) -> str:
    """Sanitizes a words. Removes potential double punctuation or other invalid symbols at the end of the word.

    :param word: A potentially dirty, but valid word. Could include additional punctuation due to joining of sentence.
    :type word: str
    :return: Word with double punctuation or invalid symbols at end of word removed.
    :rtype: str
    """
    # TODO double punctuation problem might have been solved by new join sentences approach.
    if len(word) == 0:
        return word
    # Check if last character is dot.
    if word[-1] == '.':
        # Check if second last character is not alpha numerical.
        if not word[-2].isalnum():
            # Prune invalid characters and add dot.
            return word[:-2] + '.'
    # Check if last character is not alpha numerical.
    elif not word[-1].isalnum():
        # Prune invalid character at end of word
        return word[:-1]
    return word


def _process_text(text: str) -> str:
    """Split text into sanitized words and tokenize to find sentences.
    Gives back all sentences as a combined body of text.

    :param text: All unprocessed text found within a tag.
    :type text: str
    :return: Processed body of text as combined sentences.
    :rtype: str
    """
    # Create list of valid sanitized words out of the text.
    filtered_words = [_sanitize_word(word) for word in text.split(' ') if _valid_word(word)]

    # Find sentences in the combined bag of words (bag of words still contain original dots.
    tokenized_sentences = nltk.tokenize.sent_tokenize(" ".join(filtered_words))

    return _join_sentences(tokenized_sentences)


def _get_soup_text(html_soup: BeautifulSoup) -> str:
    """Get all text from header and p tags of the BeautifulSoup object.

    :param html_soup: BeautifulSoup object of the HTML contents in the payload.
    :type html_soup: BeautifulSoup
    :return: Joined sentences from all header and p tags.
    :rtype: str
    """
    flag = 1
    if flag == 1:
        text_tags = [text_tag.text for text_tag in html_soup.find_all(re.compile('^h[1-6]$')) + html_soup.find_all('p')
                     if text_tag.text is not None]
        return _join_sentences(text_tags)
    # Could also return all text, would also include text included via div or span that is not in h or p tag.
    # return html_soup.get_text()


def process_payload(warc_file: str) -> Union[Tuple[str, str, str, str], Tuple[None, None, None, None]]:
    """Process the payload of a single warc file.
    Performs the following steps:
    1. Finds WARC-TREC-ID and HTML content.
    2. Normalizes the data.
    3. Processes HTML title, HTML headers, and HTML text tags.
    4. Return the results as a tuple.

    :param warc_file: contents of entire warc file.
    :type warc_file: str
    :return: Tuple of WARC-TREC-ID, HTML title, HTML headers, and HTML text tags. None tuple if no contents were found.
    :rtype: Union[Tuple[str, str, str, str], Tuple[None, None, None, None]
    """
    # Retrieve key and HTML content of warc file.
    file_key, html_file = _find_html(warc_file)

    if file_key is not None:
        # Turn unicode characters into python characters.
        normalized_html = unicodedata.normalize("NFKC", unescape(" ".join(html_file)))

        # Create Soup object from the HTML.
        html_soup = BeautifulSoup(normalized_html, "html.parser")

        # Get HTML title if there is a title.
        title = html_soup.title
        title_text = ""
        if title is not None and title.string is not None:
            title_text = title.string

        # Get all headers from HTML.
        headers = [header.text for header in html_soup.find_all(re.compile('^h[1-6]$'))]
        headers_text = _join_sentences(headers)

        # Get all text as defined in _get_soup_text() from HTML.
        all_text = _get_soup_text(html_soup)

        # Process title, headers, and all text into valid sentences.
        processed_title = _process_text(title_text)
        processed_headers = _process_text(headers_text)
        processed_all_text = _process_text(all_text)

        # Prepend title to all text. Should only happen if _get_soup_text() doesn't include the title.
        title_and_text = (processed_title + " " + processed_all_text).strip()

        return file_key, processed_title, processed_headers, title_and_text
    return None, None, None, None


def process_warc_zip() -> List[Tuple[str, str, str, str]]:
    """Parses warc contents of zip located at /data/warcs/sample.warc.gz.
    Does this using all present CPU cores using the Iterator from split_records.
    Gives back a list of processed files that were individual warc files in the zip with HTML as content.

    :return: List of processed warc files containing the WARC-TREC-ID, HTML title, HTML headers, and HTML text tags.
    :rtype: List[Tuple[str, str, str, str]]
    """
    # Dependency of nltk.tokenize
    nltk.download("punkt")

    with gzip.open("data/warcs/sample.warc.gz", 'rt', errors='ignore') as fo:
        pool_size = mp.cpu_count()

        # Force single threaded behaviour for debugging.
        # pool_size = 1
        if pool_size > 1:
            with mp.Pool(processes=pool_size) as pool:
                processed_files = pool.map(process_payload, split_records(fo))
        else:
            processed_files = [process_payload(payload) for payload in split_records(fo)]

        return processed_files


def save_pre_proc(
        processed_files: List[Union[Tuple[str, str, str, str], Tuple[None, None, None, None]]],
        filename: str
):
    """Store the processed files as CSV in folder /pre-proc/ under the name of filename.

    :param processed_files: Rows to store containing WARC-TREC-ID, HTML title, HTML headers, and HTML text tags.
    :type processed_files: List[Tuple[str, str, str, str]]
    :param filename: Filename of csv to store processed files in.
    :type filename: str
    """
    res_directory = "pre-proc"
    # Create pre-proc directory.
    if not os.path.exists(res_directory):
        os.makedirs(res_directory)

    with open(f"{res_directory}/{filename}.csv", 'w', newline='', encoding='UTF-8') as file:
        writer = csv.writer(file, quoting=csv.QUOTE_NONE, escapechar='\\')

        # Write rows if the row is valid.
        writer.writerows([row for row in processed_files if _valid_row(row)])

        # Print complete path to pre processed CSV file.
        print(f"{os.getcwd()}/{file.name}")


def _valid_row(row: Union[Tuple[str, str, str, str], Tuple[None, None, None, None]]) -> bool:
    """Check if the row contains a key and parsed the text tags.

    :param row: Union[Tuple[str, str, str, str], Tuple[None, None, None, None]]
    :type row: Row tuple with string content or None tuple.
    :return: True if the tuple contained strings in index 0 and 3.
    :rtype: bool
    """
    if len(row) < 4:
        return False

    # Check whether key is present and HTML text tags contains non-empty string.
    if row[0] is not None and row[3] is not None and len(row[3]) > 0:
        return True
    return False


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

    pre_proc = process_warc_zip()

    save_pre_proc(pre_proc, warc_filename)
