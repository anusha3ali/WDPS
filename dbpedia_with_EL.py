import ssl
import requests
import numpy as np
import csv

from dbpedia_utils import generate_candidates

ssl._create_default_https_context = ssl._create_unverified_context

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


def levenshtein_distance(s1, s2):
    # base case: if either string is empty, the distance is the length of the other string
    if len(s1) == 0:
        return len(s2)
    if len(s2) == 0:
        return len(s1)

    # initialize a matrix to store the distances between substrings
    distances = [[0 for j in range(len(s2) + 1)] for i in range(len(s1) + 1)]

    # set the distance between the empty string and each substring of s2 to be the index of the substring
    for i in range(len(s2) + 1):
        distances[0][i] = i

    # set the distance between each substring of s1 and the empty string to be the index of the substring
    for i in range(len(s1) + 1):
        distances[i][0] = i

    # compute the distance between each substring of s1 and each substring of s2
    for i in range(1, len(s1) + 1):
        for j in range(1, len(s2) + 1):
            # if the characters at the current indices are the same,
            # the distance is the same as the distance between the substrings without those characters
            if s1[i - 1] == s2[j - 1]:
                distances[i][j] = distances[i - 1][j - 1]
            else:
                # otherwise, the distance is the minimum of the distances
                # obtained by inserting, deleting, or substituting a character
                distances[i][j] = min(distances[i][j - 1] + 1, distances[i - 1][j] + 1, distances[i - 1][j - 1] + 1)

    return distances[len(s1)][len(s2)]


def get_most_popular_pages(results):
    if results is None:
        return None

    max_backlinks_len = 0
    popular_pages = []
    session = requests.Session()
    url = "https://en.wikipedia.org/w/api.php"

    for result in results["results"]["bindings"]:
        name = result["page"]
        x = name["value"].split("/")
        name = x[-1]
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
        json_data = response.json()
        if "query" in json_data:
            backlinks = json_data["query"]["backlinks"]
            backlinks_len = len(backlinks)
            if backlinks_len >= max_backlinks_len:
                max_backlinks_len = backlinks_len
                popular_pages.append((result["name"], result["page"]["value"]))
    
    return popular_pages


def get_most_similar_entity(mention, popular_pages):
    if len(popular_pages) == 0:
        return None

    distances = [levenshtein_distance(mention, page[0]) for page in popular_pages]
    best = np.argmax(distances)
    return popular_pages[best][1]


def get_wikipedia_entity(nlp):
    global_mention_entity = {}

    with open('popular_page2.csv', 'w', newline='', encoding='UTF-8') as file:
        writer = csv.writer(file)
        with open("warcs-20221207-182114.csv", newline='', encoding='cp850') as file:
            csv_reader = csv.reader(file, quoting=csv.QUOTE_NONE, escapechar='\\')
            for row in csv_reader:
                document = row[-1]
                doc = nlp(document)
                local_mention_entity = {}
                for ent in doc.ents:
                    mention = ent.text
                    group = ent.label_
                    if group in pruned_groups_dict:
                        if mention not in global_mention_entity:
                            candidates = generate_candidates(mention, pruned_groups_dict[group], "dbpedia_with_EL")
                            popular_pages = get_most_popular_pages(candidates)
                            link = get_most_similar_entity(mention, popular_pages)
                            global_mention_entity[mention] = link
                            local_mention_entity[mention] = link
                        elif global_mention_entity[mention]:
                            local_mention_entity[mention] = link

                if len(local_mention_entity) > 0:
                    writer.writerow([row[0], local_mention_entity])
