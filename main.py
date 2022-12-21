import argparse
import csv
import datetime
import os
import multiprocessing as mp
from typing import List, Tuple

import spacy
import spacy_transformers
import logging

from warc import process_warc_zip, save_pre_proc
from relation_extraction import ReverbNoNlp
from dbpedia_with_EL import link_entity

# Disable spaCy warnings.
logger = logging.getLogger("spacy")
logger.setLevel(logging.ERROR)


def create_dirs(*dirs: List[str]):
    """Create all directories used in the program. Only data and data/warcs folders are guaranteed to exist.

    :param dirs: List of directories to create if they don't exists.
    :type dirs: List[str]
    :return: Return nothing
    :rtype: None
    """
    for directory in dirs:
        if not os.path.exists(directory):
            os.makedirs(directory)


def pre_proc_stage(pre_proc_dir: str, filename: str) -> List[Tuple[str, str, str, str]]:
    """Perform pre-processing on the warc zip, store the rows, and return the rows.

    :param pre_proc_dir: Relative directory to store the pre-processed files in.
    :type pre_proc_dir: str
    :param filename: File name of the pre-processed files.
    :type filename: str
    :return: Rows of processed warc files. A row contains the key, title, headers, and processed text.
    :rtype: List[Tuple[str, str, str, str]
    """
    if filename is None:
        # Use current date and time as unique identifier.
        warc_filename = f'warcs-{datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}'
    else:
        warc_filename = args.filename

    # Pre-process warc zip into rows containing key, title, headers, and processed text.
    pre_proc = process_warc_zip()

    # Save the rows.
    save_pre_proc(pre_proc_dir, pre_proc, warc_filename)

    return pre_proc


class Extraction:
    rev = None
    process_entity_dict = None

    def __init__(self, vocab: List):
        """Initializes ReVerb and the dict that prevents unnecessary querying by storing performed query results.

        :param vocab: The vocabulary used during the NER.
        :type vocab: List
        """
        self.rev = ReverbNoNlp(vocab)
        self.process_entity_dict = {}

    def process_row(self, text_key: Tuple[str, str]) -> List[str]:
        """Process one row, which is one warc file that contained HTML.

        :param text_key: Text-key pair containing the processed spaCy doc object and the warc file key.
        :type text_key: Tuple[str, str]
        :return: List of formatted assignment strings containing the entities and relations.
        :rtype: List[str]
        """
        text, key = text_key
        linked_entity_dict = link_entity(text, self.process_entity_dict)
        relations = self.rev.extract_spacy_relations(text, linked_entity_dict)

        res = []
        for mention, link in linked_entity_dict.items():
            res.append(Extraction.entity_to_str(key, mention, link))
        for wiki1, relation, wiki2 in relations:
            res.append(Extraction.relation_to_str(key, linked_entity_dict[wiki1], linked_entity_dict[wiki2], relation))
        return res

    @staticmethod
    def entity_to_str(key: str, mention: str, link: str) -> str:
        """Entity data to valid assignment string.

        :param key: warc file key.
        :type key: str
        :param mention: Mention to which the Wikipedia page refers to.
        :type mention: str
        :param link: URL to the Wikipedia page of the mention.
        :type link: str
        :return: Properly formatted assignment string.
        :rtype: str
        """
        return f"ENTITY: {key}\t{mention}\t{link}"

    @staticmethod
    def relation_to_str(key: str, wiki1: str, wiki2: str, relation: str) -> str:
        """Relation data to valid assignment string.

        :param key: warc file key.
        :type key: str
        :param wiki1: URL to the Wikipedia page of the mention.
        :type wiki1: str
        :param wiki2: URL to the Wikipedia page of the mention.
        :type wiki2: str
        :param relation: Properly formatted assignment string.
        :type relation: str
        """
        return f"RELATION: {key}\t{wiki1}\t{wiki2}\t{relation}"


def find_linked_relations(pre_proc_files: List[Tuple[str, str, str, str]], model_name: str, pool_size: int):
    """Performs entity linking and relation extraction. Both only output linked entities.

    :param pre_proc_files: 1 row per HTML warc. Row contains key, title, headers, and combined text.
    :type pre_proc_files: List[Tuple[str, str, str, str]]
    :param model_name: Name of the used spaCy model.
    :type model_name: str
    :param pool_size: Amount of processes to spawn in the parallel pool.
    :type pool_size: int
    :return: No output, everything is written to console and the data/out file.
    :rtype: None
    """
    # Processing of entire warc file using nlp.pipe with sm model takes 38s and with trf 2197s (about 36.6 minutes)
    # These timings are just the time in nlp.pipe, nothing is performed on the results.
    nlp = spacy.load(model_name, disable=[
        "textcat",
        "tok2vec",
        "parser",
        "lemmatizer"
    ])
    nlp.add_pipe("sentencizer")

    # Retrieve the used spaCy vocab. Later used by ReVerb.
    vocab = nlp.vocab

    # Pack all text together with the key as context.
    text_context = [(pre_proc_file[3], pre_proc_file[0]) for pre_proc_file in pre_proc_files]
    # Processes all text in parallel, does nothing with the context except for passing it through.
    doc_tuples = nlp.pipe(text_context, as_tuples=True)

    # Perform sequentially if only 1 process is used. Outside of multiprocessing Pool abstraction.
    if pool_size == 1:
        results = []
        # Class keeping with 1 ReVerb instance, storing global dict to prevent duplicate queries.
        extraction = Extraction(vocab)
        for text_key in doc_tuples:
            results.append(extraction.process_row(text_key))
    else:
        with mp.Pool(processes=pool_size) as pool:
            # Class keeping with 1 ReVerb instance, storing process wide dict to prevent duplicate queries.
            extraction = Extraction(vocab)
            results = pool.map(extraction.process_row, doc_tuples)

    # Print the results to the console.
    for result in results:
        for entry in result:
            print(entry)

    # Store the results in data/out.
    with open("data/out", "w", encoding='UTF-8') as out_file:
        for result in results:
            for entry in result:
                out_file.write(entry)
                out_file.write("\n")


def _load_proc_files_from_csv(file_path: str) -> List:
    """Load all rows from a csv file with encoding as UTF-8 and no quoting used and escapechar \\.

    :param file_path: Path to the file to load.
    :type file_path: str
    :return: List of rows
    :rtype: List
    """
    with open(file_path, newline='', encoding='UTF-8') as file:
        csv_reader = csv.reader(file, quoting=csv.QUOTE_NONE, escapechar='\\')
        return [row for row in csv_reader]


if __name__ == "__main__":
    parser = argparse.ArgumentParser("wdp")
    parser.add_argument(
        "--warc_pre_proc_out",
        dest="pre_proc_filename",
        required=False,
        help="A file name for the preprocessed warc zip.",
        type=str
    )
    parser.add_argument(
        "--pre_proc_dir",
        dest="pre_proc_dir",
        required=False,
        default="pre-proc",
        help="Directory the preprocessed file gets stored.",
        type=str
    )
    parser.add_argument(
        "--relations_dir",
        dest="relations_dir",
        required=False,
        default="relations",
        help="Directory the relations file gets stored.",
        type=str
    )
    args = parser.parse_args()

    # Default dir is pre-proc and relations_dir, both can be adjusted via the given args.
    create_dirs(args.pre_proc_dir, args.relations_dir)

    # Default dir is pre-proc and no default filename is given, both values can be set by given args.
    # Performs the pre-processing stage.
    pre_proc_files = pre_proc_stage(args.pre_proc_dir, args.pre_proc_filename)

    # Performs entity linking and relation extraction using spaCy NER on the en_core_web_trf model.
    find_linked_relations(pre_proc_files, "en_core_web_trf", mp.cpu_count())
