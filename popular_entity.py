import ssl
import nltk
from nltk import word_tokenize, pos_tag
import pandas as pd
import csv
import requests

from nltk import Tree

from dbpedia_utils import groups, generate_candidates

ssl._create_default_https_context = ssl._create_unverified_context

df = pd.read_csv("innovators.csv")


def get_continuous_chunks(chunked, label):
    prev = None
    continuous_chunk = []
    current_chunk = []
    dictionary = {}
    lis = []
    for subtree in chunked:
        if type(subtree) == Tree and subtree.label() in label:
            current_chunk.append(" ".join([token for token, pos in subtree.leaves()]))
        if current_chunk:
            named_entity = " ".join(current_chunk)
            if named_entity not in continuous_chunk:
                continuous_chunk.append(named_entity)
                lis.append(named_entity)
                if subtree.label() in dictionary:
                    dictionary[subtree.label()].append(named_entity)
                else:
                    dictionary[subtree.label()] = lis
                lis = []
                current_chunk = []
        else:
            continue
    return dictionary


def get_most_popular(results):
    max_backlinks_len = 0
    popular_page = None
    session = requests.Session()
    url = "https://en.wikipedia.org/w/api.php"
    for result in results["results"]["bindings"]:
        name = result["page"]
        x = name["value"].split("/")
        name = x[-1]
        params = {
            "action": "query",
            "format": "json",
            "list": "backlinks",
            "bltitle": name,
            'bllimit': 'max'

        }

        response = session.get(url=url, params=params)
        json_data = response.json()
        backlinks = json_data["query"]["backlinks"]
        backlinks_len = len(backlinks)
        if backlinks_len > max_backlinks_len:
            max_backlinks_len = backlinks_len
            popular_page = result
    return popular_page


popular_page = []
labels = ["PERSON", "GPE", "ORGANIZATION"]
dictionary = {}
with open('popular_page.csv', 'w', newline='', encoding='UTF-8') as file:
    writer = csv.writer(file)
    for index, row in df.iterrows():
        popular_page = []
        dictionary = {}
        if pd.isna(row["text"]) is False:
            tokens = word_tokenize(row["text"])
            tag = pos_tag(tokens)
            ne_tree = nltk.ne_chunk(tag)
            extracted_entities = get_continuous_chunks(ne_tree, labels)
            for key, value in extracted_entities.items():
                for entity in value:
                    if str(key) == "PERSON":
                        results = generate_candidates(entity, groups[0], "popular_entity")
                    elif str(key) == "GPE":
                        results = generate_candidates(entity, groups[1], "popular_entity")
                    elif str(key) == "ORGANIZATION":
                        results = generate_candidates(entity, groups[2], "popular_entity")
                    link = get_most_popular(results)
                    if link is not None:
                        dictionary[entity] = link
        writer.writerow([row["key"], dictionary])
