import ssl
import spacy

import csv
import requests

import urllib.request

from dbpedia_utils import groups, generate_candidates

ssl._create_default_https_context = ssl._create_unverified_context

# if the arg is empty in ProxyHandler, urllib will find itself your proxy config.

proxy_support = urllib.request.ProxyHandler({})
opener = urllib.request.build_opener(proxy_support)
urllib.request.install_opener(opener)


def get_most_popular(results):
    backlinks = 0
    popular_page = []
    name = None
    pagex = None
    S = requests.Session()
    URL = "https://en.wikipedia.org/w/api.php"
    if results is not None:
        for result in results["results"]["bindings"]:
            name = result["page"]
            x = name["value"].split("/")
            name = x[-1]
            PARAMS = {
                "action": "query",
                "format": "json",
                "list": "backlinks",
                "bltitle": name,
                'bllimit': 'max',
                "blnamespace": 4,
                "blredirect": "False"
            }

            R = S.get(url=URL, params=PARAMS)
            DATA = R.json()
            if "query" in DATA:
                BACKLINKS = DATA["query"]["backlinks"]
                l = len(BACKLINKS)
                if l >= backlinks:
                    backlinks = l
                    popular_page.append(result)
        # Unique from this point on,
        # only missing info from dbpedia_with_EL.get_most_popular_pages
        # is value of name attribute.
        if len(popular_page) > 0:
            for page in popular_page:
                print("done")

                if page["name"] == name:
                    return (page["page"]["value"])

                elif name in page["name"]:
                    print(page["page"]["value"])

            return popular_page[0]["page"]["value"]

    return None


nlp = spacy.load("en_core_web_trf")
dictionary = {}
disambiguated_pages = []
record_dict = {}
count = 0
count_warx = 0
valid_entity = False
valid_entity_types = ["PERSON", "GPE", "LOC", "ORG", "PRODUCT", "WORK_OF_ART", "LANGUAGE"]
with open('popular_page2.csv', 'w', newline='', encoding='UTF-8') as file:
    writer = csv.writer(file)
    with open("warcs-20221207-182114.csv", newline='', encoding='cp850') as file:
        csv_reader = csv.reader(file, quoting=csv.QUOTE_NONE, escapechar='\\')
        c = 0
        for row in csv_reader:
            record_dict = {}
            document = row[-1]
            doc = nlp(document)
            for ent in doc.ents:
                link = None
                entity_name = ent.text.lower()
                entity_type = ent.label_
                if entity_type in valid_entity_types:
                    if entity_name not in dictionary:
                        if entity_type == "PERSON":
                            results = generate_candidates(entity_name, groups[0], "pipeline_with_spacy")
                        elif entity_type == "GPE" or entity_type == "LOC":
                            results = generate_candidates(entity_name, groups[1], "pipeline_with_spacy")
                        elif entity_type == "ORG" or entity_type == "PRODUCT":
                            results = generate_candidates(entity_name, groups[2], "pipeline_with_spacy")
                        elif entity_type == "WORK_OF_ART":
                            results = generate_candidates(entity_name, groups[3], "pipeline_with_spacy")
                        elif entity_type == "LANGUAGE":
                            results = generate_candidates(entity_name, groups[5], "pipeline_with_spacy")
                        link = get_most_popular(results)
                        if link is not None:
                            dictionary[entity_name] = link
                            record_dict[entity_name] = dictionary[entity_name]
                        else:
                            dictionary[entity_name] = "None"
                    else:
                        if entity_name not in record_dict and dictionary[entity_name] != "None":
                            record_dict[entity_name] = dictionary[entity_name]
            count_warx = count_warx+1
            if len(record_dict) > 0:
                writer.writerow([row[0], record_dict])
        print(dictionary)
