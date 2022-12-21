import argparse
import csv
import datetime
import os
import multiprocessing as mp

import spacy
import spacy_transformers

from warc import process_warc_zip, save_pre_proc
from relation_extraction import Reverb, Patty, ReverbNoNlp
from dbpedia_with_EL import generate_candidates, get_most_popular_pages, get_most_refered_page, link_entity

current_datetime = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

pruned_groups_dict = {
    "PERSON"        : "dbo:Person",
    "GPE"           : "geo:SpatialThing",
    "LOC"           : "dbo:Location",
    "FAC"           : "geo:SpatialThing",
    "ORG"           : "dbo:Organisation",
    "PRODUCT"       : "owl:Thing",
    "EVENT"         : "dbo:Event",
    "LANGUAGE"      : "dbo:Language",
    "DATE"          : "owl:Thing",
    "NORP"          : "owl:Thing",
    "WORK_OF_ART"   : "dbo:Work"
}


def create_dirs(*dirs):
    for directory in dirs:
        if not os.path.exists(directory):
            os.makedirs(directory)


def pre_proc_stage(pre_proc_dir, filename):
    if filename is None:
        warc_filename = f'warcs-{current_datetime}'
    else:
        warc_filename = args.filename

    pre_proc = process_warc_zip()

    save_pre_proc(pre_proc_dir, pre_proc, warc_filename)

    return pre_proc


class Extraction:
    rev = None

    def __init__(self, vocab):
        self.rev = ReverbNoNlp(vocab)

    def process_row(self, text_key):
        text, key = text_key
        linked_entity_dict = link_entity(text)
        relations = self.rev.extract_spacy_relations(text, linked_entity_dict)

        res = []
        for mention, link in linked_entity_dict.items():
            res.append(Extraction.entity_to_str(key, mention, link))
        for wiki1, relation, wiki2 in relations:
            res.append(Extraction.relation_to_str(key, wiki1, wiki2, relation))
        return res

    @staticmethod
    def entity_to_str(key, mention, link):
        return f"ENTITY: {key}\t{mention}\t{link}"

    @staticmethod
    def relation_to_str(key, wiki1, wiki2, relation):
        return f"RELATION: {key}\t{wiki1}\t{wiki2}\t{relation}"


def find_linked_relations(pre_proc_files, model_name, pool_size):
    # Processing of entire warc file using nlp.pipe with sm model takes 38s and with trf 2197s (about 36.6 minutes)
    # These timings are just the time in nlp.pipe, nothing is performed on the results.
    nlp = spacy.load(model_name, disable=[
        "textcat",
        "tok2vec",
        "tagger",
        "parser",
        "attribute_ruler",
        "lemmatizer"
    ])
    nlp.add_pipe("sentencizer")

    vocab = nlp.vocab

    text_context = [(pre_proc_file[3], pre_proc_file[0]) for pre_proc_file in pre_proc_files]
    doc_tuples = nlp.pipe(text_context, as_tuples=True)

    if pool_size == 1:
        results = []
        extraction = Extraction(vocab)
        for text_key in doc_tuples:
            results.append(extraction.process_row(text_key))
    else:
        with mp.Pool(processes=pool_size) as pool:
            extraction = Extraction(vocab)
            results = pool.map(extraction.process_row, doc_tuples)

    # Use default sort. Should sort on warc key first as it uses default string sort.
    results.sort()

    for result in results:
        for entry in result:
            print(entry)

    with open("data/out", "w", encoding='UTF-8') as out_file:
        for result in results:
            if len(result) == 0:
                continue
            out_file.write("\n".join(result))
            out_file.write("\n")


def _load_proc_files_from_csv(file_path):
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

    create_dirs(args.pre_proc_dir, args.relations_dir)

    pre_proc_files = pre_proc_stage(args.pre_proc_dir, args.pre_proc_filename)
    pre_proc_files = pre_proc_files[100:200]

    find_linked_relations(pre_proc_files, "en_core_web_trf", mp.cpu_count())
