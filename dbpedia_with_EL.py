import ssl
import requests
import numpy as np
import csv
import time
import json
from Levenshtein import distance as levenshtein_distance

from dbpedia_utils import generate_candidates

ssl._create_default_https_context = ssl._create_unverified_context

pruned_groups_dict = {
    "PERSON"        : "dbo:Person",
    "GPE"           : "geo:SpatialThing",
    "LOC"           : "geo:SpatialThing",
    "FAC"           : "geo:SpatialThing",
    "LANGUAGE"      : "owl:Language",
    "ORG"           : "owl:Thing",
    "PRODUCT"       : "owl:Thing",
    "EVENT"         : "owl:Thing",
    "DATE"          : "owl:Thing",
    "NORP"          : "owl:Thing",
    "WORK_OF_ART"   : "owl:Thing",
}


def get_most_popular_pages(mention, candidates):
    max_backlinks_len = 0
    popular_pages = []
    session = requests.Session()
    url = "https://en.wikipedia.org/w/api.php"

    if candidates is None or len(candidates["results"]["bindings"]) == 0:
        return None
    
    if len(candidates) == 1:
        entity_name = candidates[0]["name"]["value"] if "value" in candidates[0]["name"] else candidates[0]["name"]
        return [(entity_name, candidates[0]["page"]["value"], candidates[0]["item"]["value"])]

    for candidate in candidates["results"]["bindings"]:
        name = candidate["page"]["value"].split("/")[-1]
        params = {
            "action"        : "query",
            "format"        : "json",
            "list"          : "backlinks",
            "bltitle"       : name, 
            "bllimit"       : 'max',
            "blnamespace"   : 4,
            "blredirect"    : "False"
        }

        response = session.get(url=url, params=params)
        if not response:
            time.sleep(5)
            response = session.get(url=url, params=params)
            if not response:
                continue
        json_data = response.json()
        if "query" in json_data:
            backlinks = json_data["query"]["backlinks"]
            backlinks_len = len(backlinks)
            if backlinks_len >= max_backlinks_len:
                max_backlinks_len = backlinks_len
                entity_name = candidate["name"]["value"] if "value" in candidate["name"] else candidate["name"]
                popular_pages.append((entity_name, candidate["page"]["value"], candidate["item"]["value"], backlinks_len))
    
    if len(popular_pages) > 0:
        most_popular_pages = [page for page in popular_pages if page[-1] == max_backlinks_len]
        distances = [levenshtein_distance(mention, page[0]) for page in most_popular_pages]
        best = np.argmin(distances)
        return most_popular_pages[best]
    
    distances = []
    for candidate in candidates["results"]["bindings"]:
        entity_name = candidate["name"]["value"] if "value" in candidate["name"] else candidate["name"]
        distance = levenshtein_distance(mention, entity_name)
        if distance == 0:
            return (entity_name, candidate["page"]["value"], candidate["item"]["value"])
        distances.append(distance)
    best = np.argmin(distances)
    candidate = candidates[best]
    entity_name = candidate["name"]["value"] if "value" in candidate["name"] else candidate["name"]
    return (entity_name, candidate["page"]["value"], candidate["item"]["value"])
    

def get_most_similar_entity(mention, pages):
    if not pages or len(pages) == 0:
        return None

    if len(pages) == 1:
        return pages[0]

    # distances = [levenshtein_distance(mention, page[0]) for page in pages]
    distances = []
    for page in pages:
        distance = levenshtein_distance(mention, page[0]) 
        if distance == 0:
            return page
        distances.append(distance)
    best = np.argmin(distances)
    return pages[best]


def get_most_refered_page(mention, candidates):
    if candidates is None or len(candidates["results"]["bindings"]) == 0:
        return None
    
    if len(candidates) == 1:
        entity_name = candidates[0]["name"]["value"] if "value" in candidates[0]["name"] else candidates[0]["name"]
        return [(entity_name, candidates[0]["page"]["value"], candidates[0]["item"]["value"])] # TODO why is this a list?
    
    max_refered_count = 0
    popular_pages = []
    for candidate in candidates["results"]["bindings"]:
        refered_count = int(candidate["count"]["value"])
        if refered_count >= max_refered_count:
            max_refered_count = refered_count
            entity_name = candidate["name"]["value"] if "value" in candidate["name"] else candidate["name"]
            popular_pages.append((entity_name, candidate["page"]["value"], candidate["item"]["value"], refered_count))
    
    most_popular_pages = [page for page in popular_pages if page[-1] == max_refered_count]
    if len(most_popular_pages) == 1:
        return most_popular_pages[0]
    distances = [levenshtein_distance(mention, page[0]) for page in most_popular_pages]
    best = np.argmin(distances)
    return most_popular_pages[best]


def get_wikipedia_entity(nlp):
    global_mention_entity = {}
    rows = []
    unliked_mentions = 0
    total_mentions = 0
    total_documents = 0
    total_dictionary_vist = 0
    
    with open('popular_page_db.csv', 'w', newline='', encoding='UTF-8') as file:
        writer_db = csv.writer(file, delimiter='\t')
        with open('popular_page_wiki.csv', 'w', newline='', encoding='UTF-8') as file:
            writer = csv.writer(file, delimiter='\t')
            with open("./pre-proc/warcs-20221207-182114.csv", newline='', encoding='cp850') as file:
                csv_reader = csv.reader(file, quoting=csv.QUOTE_NONE, escapechar='\\')
                for row in csv_reader:
                    text = row[-1]
                    ents = nlp(text).ents
                    local_mention_entity = {}
                    total_documents += 1
                    total_mentions += len(ents)
                    print(total_documents)
                    for ent in ents:
                        mention = ent.text
                        mention_key = ' '.join(mention.strip().lower().split())
                        group = ent.label_
                        if group in pruned_groups_dict:
                            if mention_key not in global_mention_entity:
                                candidates = generate_candidates(mention, pruned_groups_dict[group], "dbpedia_with_EL")
                                selected_entity = get_most_refered_page(mention, candidates)
                                # selected_entity = get_most_popular_pages(mention, candidates)
                                print(mention, selected_entity)
                                if selected_entity:
                                    global_mention_entity[mention_key] = selected_entity[1], selected_entity[2]
                                    local_mention_entity[mention] = selected_entity[1], selected_entity[2]
                                else:
                                    global_mention_entity[mention_key] = None
                            elif global_mention_entity[mention_key]:
                                total_dictionary_vist += 1
                                local_mention_entity[mention] = global_mention_entity[mention_key]
                            else:
                                unliked_mentions += 1

                    if len(local_mention_entity) > 0:
                        rows = [[f"ENTITY: {row[0]}", mention, link[0]] for mention, link in local_mention_entity.items()]
                        writer.writerows(rows)
                        writer_db.writerows(rows)
    json.dump(global_mention_entity, open("globa_dict.json", "w"))                       
    print(f"DONE, {unliked_mentions} unliked mentions out of {total_documents} documents and {total_mentions} mentions.")


if __name__ == "__main__":
    import spacy, spacy_transformers
    nlp_model = spacy.load("en_core_web_trf", disable=["textcat", "tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"])
    start = time.time()
    get_wikipedia_entity(nlp_model)
    print(time.time() - start)
