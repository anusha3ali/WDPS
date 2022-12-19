import argparse
import csv
import datetime
import os

import spacy
import spacy_transformers
from typing import List, Tuple

from warc import process_warc_zip, save_pre_proc
from relation_extraction import Reverb, Patty, ReverbNoNlp
from dbpedia_with_EL import generate_candidates, get_most_popular_pages, get_most_refered_page

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


# def entity_linking_stage(nlp, rows: List[Tuple[str, str, str, str]]):
#     entities = []
#     entity_to_url = {}
#     for key, _, _, text in rows:
#         # TODO scalability, start using pipes https://github.com/explosion/spaCy/issues/1839#issuecomment-357510790
#         text_ents = [(ent.text, ent.label_) for ent in nlp(text).ents]
#         for mention, ent_group in text_ents:
#             if ent_group in pruned_groups_dict:
#                 results = generate_candidates(mention, pruned_groups_dict[ent_group])
#                 url = get_most_popular_pages(results)
#                 if url is not None:
#                     entities.append((key, mention, url))
#                     entity_to_url[(key, mention)] = url
#     return entities, entity_to_url
#
#
# def relation_extraction_stage(nlp, entity_to_url, pre_proc_dir, relation_dir):
#     # TODO scalability, start using pipes https://github.com/explosion/spaCy/issues/1839#issuecomment-357510790
#     extractor = Reverb(nlp=nlp)
#     rows = extractor.extract_relations_from_zip(f"{pre_proc_dir}/warcs-20221210-141217.csv", with_matcher=True)
#     filename = f"{relation_dir}/{current_datetime}"  # Dir seems to be included via save file func TODO centralize this behaviour
#     extractor.save_file(current_datetime, rows)
#     return []


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

    if not args.skip_pre_proc and False:
        pre_proc_files = pre_proc_stage(args.pre_proc_dir, args.pre_proc_filename)
    else:
        # TODO shouldn't hard code this.
        pre_proc_files = _load_proc_files_from_csv("pre-proc/warcs-20221210-141217-TMP.csv")

    # tags disabled in NER disable=["tagger", "attribute_ruler", "lemmatizer"]
    # tags disabled in relation extraction disable=["textcat"]
    # Processing of entire warc file using nlp.pipe with sm model takes 38s and with trf 2197s (about 36.6 minutes)
    nlp_model = spacy.load("en_core_web_sm", disable=["textcat"])
    
    # TODO Disambling more pipelines might also help
    # nlp_model = spacy.load("en_core_web_trf", disable=["textcat", "tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"])

    text_context = [(pre_proc_file[3], pre_proc_file[0]) for pre_proc_file in pre_proc_files]
    doc_tuples = nlp_model.pipe(text_context, as_tuples=True)

    print("loop over generator")
    vocab = nlp_model.vocab
    import time
    s = time.time()
    for doc, key in doc_tuples:
        entities = []
        entity_to_url = {}
        mentions = set()
        for ent in doc.ents:
            mention, ent_group = ent.text, ent.label_
            if mention in mentions or mention in entity_to_url:
                continue
            mentions.add(mention)
            if ent_group in pruned_groups_dict:
                # TODO: still need to remove the need for this third variable
                # TODO it groups "Tunis Tunisia" as a single entity which is weird
                # TODO WP, Flash Player, WordPress returning no results in first doc.
                # TODO resolve WP to WordPress
                results = generate_candidates(mention, pruned_groups_dict[ent_group], "dbpedia_with_EL")
                urls = get_most_refered_page(mention, results)
                if urls is not None and len(urls) > 0:
                    entities.append((key, mention, urls[1]))
                    entity_to_url[mention] = urls[1]
        confirmed_relations = []
        # TODO find a way to create less reverbs, matcher is reinitialized on vocab every time.
        rnn = ReverbNoNlp(vocab)
        for sentence in doc.sents:
            relations = rnn.extract_relation(sentence)
            for e1, r, e2 in relations:
                e1_txt = e1.text
                e2_txt = e2.text
                if e1_txt in entity_to_url and e2_txt in entity_to_url:
                    confirmed_relations.append((entity_to_url[e1_txt], r, entity_to_url[e2_txt]))
        print()
    e = time.time()
    print(e - s)

    # if not args.skip_entity_linking:
    #     linked_entities, entity_to_url = entity_linking_stage(nlp_model, pre_proc_files)
    # else:
    #     linked_entities, entity_to_url = [], {}
    #
    # extracted_relations = relation_extraction_stage(nlp_model, entity_to_url, args.pre_proc_dir, args.relations_dir)
    #
    # entity_strings = [f"ENTITY: {key}\t{mention}\t{url}" for key, mention, url in linked_entities]
    # relation_strings = [f"RELATION:\t{key}\t{url1}\t{url2}\t{relation}\t{wikidata}"
    #                     for key, url1, url2, relation, wikidata in extracted_relations]
    #
    # predictions = entity_strings + relation_strings
    # predictions.sort()
    #
    # with open("data/predictions_out.txt", "w") as file:
    #     file.writelines(predictions)
