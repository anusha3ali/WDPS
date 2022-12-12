import argparse
import csv
import datetime
import os

import spacy
from typing import List, Tuple

from warc import process_warc_zip, save_pre_proc
from relation_extraction import Patty
from ner import get_entities
from dbpedia_with_EL import generate_candidates, get_most_popular

current_datetime = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

pruned_groups_dict = {
    "PERSON": "dbo:Person",
    "GPE": "geo:SpatialThing",
    "LOC": "geo:Location",
    "ORG": "dbo:Organisation",
    "PRODUCT": "dbo:Work",
    "EVENT": "dbo:Event",
    "LANGUAGE": "dbo:Language"
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


def entity_linking_stage(nlp, rows: List[Tuple[str, str, str, str]]):
    entities = []
    entity_to_url = {}
    for key, _, _, text in rows:
        # TODO scalability, start using pipes https://github.com/explosion/spaCy/issues/1839#issuecomment-357510790
        text_ents = get_entities(nlp, text)
        for mention, ent_group in text_ents.items():
            if ent_group in pruned_groups_dict:
                results = generate_candidates(mention, pruned_groups_dict[ent_group])
                url = get_most_popular(results)

                if url is not None:
                    entities.append((key, mention, url))
                    entity_to_url[(key, mention)] = url
    return entities, entity_to_url


def relation_extraction_stage(nlp, entity_to_url, pre_proc_dir, relation_dir):
    # TODO scalability, start using pipes https://github.com/explosion/spaCy/issues/1839#issuecomment-357510790
    patty = Patty(nlp=nlp)
    rows = patty.extract_relations_from_zip(f"{pre_proc_dir}/warcs-20221210-141217.csv", with_matcher=True)
    filename = f"{relation_dir}/{current_datetime}"  # Dir seems to be included via save file func TODO centralize this behaviour
    patty.save_file(current_datetime, rows)
    return []


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
    parser.add_argument(
        "--pre-proc-off",
        dest="skip_pre_proc",
        action='store_true',
        help="Include flag to skip preprocessing."
    )
    parser.add_argument(
        "--entity-linking-off",
        dest="skip_entity_linking",
        action='store_true',
        help="Include flag to skip preprocessing."
    )
    args = parser.parse_args()

    # TODO maybe allow turning on/off certain steps

    create_dirs(args.pre_proc_dir, args.relations_dir)

    if not args.skip_pre_proc:
        pre_proc_files = pre_proc_stage(args.pre_proc_dir, args.pre_proc_filename)
    else:
        # TODO shouldn't hard code this.
        pre_proc_files = _load_proc_files_from_csv("pre-proc/warcs-20221210-141217.csv")

    # tags disabled in NER disable=["tagger", "attribute_ruler", "lemmatizer"]
    # tags disabled in relation extraction disable=["textcat"]
    nlp_model = spacy.load("en_core_web_trf", disable=["textcat"])

    if not args.skip_entity_linking:
        linked_entities, entity_to_url = entity_linking_stage(nlp_model, pre_proc_files)
    else:
        linked_entities, entity_to_url = [], {}

    extracted_relations = relation_extraction_stage(nlp_model, entity_to_url, args.pre_proc_dir, args.relations_dir)

    entity_strings = [f"ENTITY: {key}\t{mention}\t{url}" for key, mention, url in linked_entities]
    relation_strings = [f"RELATION:\t{key}\t{url1}\t{url2}\t{relation}\t{wikidata}"
                        for key, url1, url2, relation, wikidata in extracted_relations]

    predictions = entity_strings + relation_strings
    predictions.sort()

    with open("data/predictions_out.txt", "w") as file:
        file.writelines(predictions)
