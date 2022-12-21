import ssl
from typing import Tuple, List

import requests
import numpy as np
import time
import json
from Levenshtein import distance as levenshtein_distance

from dbpedia_utils import generate_candidates

# Prevent crash from SSL verification.
ssl._create_default_https_context = ssl._create_unverified_context

# Mapping of NER tag to SPARQL query group.
pruned_groups_dict = {
    "PERSON"        : "dbo:Person",
    "GPE"           : "geo:SpatialThing",
    "LOC"           : "geo:SpatialThing",
    "FAC"           : "geo:SpatialThing",
    "LANGUAGE"      : "dbo:Language",
    "ORG"           : "dbo:Organisation",
    "PRODUCT"       : "owl:Thing",
    "EVENT"         : "dbo:Event",
    "DATE"          : "owl:Thing",
    "NORP"          : "owl:Thing",
    "WORK_OF_ART"   : "dbo:Work"
}


def get_most_popular_pages(mention: str, candidates: dict) -> Tuple:
    """Get the most popular candidate based on the backlinks in other Wikipedia articles using the Wikipedia API.

    :param mention: Entity mention.
    :type mention: str
    :param candidates: SPARQL dict containing all resulting bindings.
    :type candidates: dict
    :return: Tuple with link to most popular page and the entity name.
    :rtype: Tuple
    """
    max_backlinks_len = 0
    popular_pages = []
    session = requests.Session()
    url = "https://en.wikipedia.org/w/api.php"

    # No pages present.
    if candidates is None or len(candidates["results"]["bindings"]) == 0:
        return None

    # Only one page present.
    if len(candidates) == 1:
        entity_name = candidates[0]["name"]["value"] if "value" in candidates[0]["name"] else candidates[0]["name"]
        return [(entity_name, candidates[0]["page"]["value"], candidates[0]["item"]["value"])]

    for candidate in candidates["results"]["bindings"]:
        # Get the candidate name.
        name = candidate["page"]["value"].split("/")[-1]

        # Set request params.
        params = {
            "action"        : "query",
            "format"        : "json",
            "list"          : "backlinks",
            "bltitle"       : name, 
            "bllimit"       : 'max',
            "blnamespace"   : 4,
            "blredirect"    : "False"
        }

        # Get response.
        response = session.get(url=url, params=params)
        if not response:
            # Reattempt get once if it failed initially.
            time.sleep(5)
            response = session.get(url=url, params=params)

            # Go to next candidate if a response is still missing.
            if not response:
                continue

        json_data = response.json()
        if "query" in json_data:
            # Get the backlinks length.
            backlinks = json_data["query"]["backlinks"]
            backlinks_len = len(backlinks)

            # Store popular page if new backlinks_len was found.
            if backlinks_len >= max_backlinks_len:
                max_backlinks_len = backlinks_len
                entity_name = candidate["name"]["value"] if "value" in candidate["name"] else candidate["name"]
                popular_pages.append((entity_name, candidate["page"]["value"], candidate["item"]["value"], backlinks_len))

    # Return first page as default.
    if len(popular_pages) == 0:
        return popular_pages[0]

    # Break ties using levenshtein distance.
    if len(popular_pages) > 0:
        most_popular_pages = [page for page in popular_pages if page[-1] == max_backlinks_len]

        # Calculate levenshtein distance from mention to page.
        distances = [levenshtein_distance(mention, page[0]) for page in most_popular_pages]
        best = np.argmin(distances)
        return most_popular_pages[best]

    # Alternative to levenshtein distance as tie breaker.
    distances = []
    for candidate in candidates["results"]["bindings"]:
        entity_name = candidate["name"]["value"] if "value" in candidate["name"] else candidate["name"]
        distance = levenshtein_distance(mention, entity_name)
        if distance == 0:
            return entity_name, candidate["page"]["value"], candidate["item"]["value"]
        distances.append(distance)
    best = np.argmin(distances)
    candidate = candidates[best]
    entity_name = candidate["name"]["value"] if "value" in candidate["name"] else candidate["name"]
    return entity_name, candidate["page"]["value"], candidate["item"]["value"]
    

def get_most_similar_entity(mention: str, pages: List) -> object:
    """Get the most similar entity using levenshtein distance.

    :param mention: Entity mention.
    :type mention: str
    :param pages: Pages from the possible candidates.
    :type pages: List
    :return: Most similar page.
    :rtype: object
    """
    # No pages present.
    if not pages or len(pages) == 0:
        return None

    # Only one page present.
    if len(pages) == 1:
        return pages[0]

    distances = []
    for page in pages:
        # Calculate the levenshtein distance from the entity to the page.
        distance = levenshtein_distance(mention, page[0]) 
        if distance == 0:
            return page
        distances.append(distance)
    # Find the index of the best levenshtein distance.
    best = np.argmin(distances)
    return pages[best]


def get_most_refered_page(mention: str, candidates: List) -> str:
    """Select the most referred page, with referred being the most count of dbpedia referrals.

    :param mention: Entity mention.
    :type mention: str
    :param candidates: List of candidates to asses referral counts.
    :type candidates: List
    :return: The link from the most referred page, None if no value is found.
    :rtype: str
    """
    # No candidates present.
    if candidates is None or candidates["results"]["bindings"] is None or len(candidates["results"]["bindings"]) == 0:
        return None
    
    candidates = candidates["results"]["bindings"]
    # Only one candidate present.
    if len(candidates) == 1:
        return candidates[0]["page"]["value"]
    
    max_refered_count = 0
    popular_pages = []
    for candidate in candidates:
        # Get referred count.
        refered_count = int(candidate["count"]["value"])

        # Check if the referred count is better.
        if refered_count >= max_refered_count:
            # Set the new max.
            max_refered_count = refered_count
            entity_name = candidate["name"]["value"] if "value" in candidate["name"] else candidate["name"]

            # Store page.
            popular_pages.append((entity_name, candidate["page"]["value"], refered_count))
    
    most_popular_pages = [page for page in popular_pages if page[-1] == max_refered_count]
    # One page is the most referred.
    if len(most_popular_pages) == 1:
        return most_popular_pages[0][1]

    # Calculate the levenshtein distances to the mention. Used to break ties.
    distances = [levenshtein_distance(mention, page[0]) for page in most_popular_pages]

    # Find index based on best levenshtein distance to the mention.
    best = np.argmin(distances)
    return most_popular_pages[best][1]


def link_entity(text: object, global_mention_entity: dict) -> dict:
    """Links all entities in the spaCy Doc object to a Wikipedia URL if one can be found.

    :param text: spaCy Doc text object containing the named entities.
    :type text: object
    :param global_mention_entity: Dictionary of already queried mentions.
    :type global_mention_entity: dict
    :return: Dictionary of linked entities.
    :rtype: dict
    """
    # Pack ents.
    ents = {(ent.text, ent.label_) for ent in text.ents}

    local_mention_entity = {}
    for mention, group in ents:
        mention_key = ' '.join(mention.strip().lower().split())
        if group in pruned_groups_dict:
            # Check if mention is not in global dictionary.
            if mention_key not in global_mention_entity:
                # Generate candidates using SPARQL query on named entity mention and group.
                candidates = generate_candidates(mention, pruned_groups_dict[group])

                # Pick the most referred link from the possible candidates.
                link = get_most_refered_page(mention, candidates)

                # Check if mention is linked.
                if link:
                    global_mention_entity[mention_key] = link
                    local_mention_entity[mention] = link
                # Mention is not linked.
                else:
                    global_mention_entity[mention_key] = None
            # Mention has a valid entity link in global dictionary.
            elif global_mention_entity[mention_key]:
                local_mention_entity[mention] = global_mention_entity[mention_key]
    return local_mention_entity
