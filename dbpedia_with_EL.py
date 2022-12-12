import ssl

from SPARQLWrapper import SPARQLWrapper, JSON
import requests

ssl._create_default_https_context = ssl._create_unverified_context

sparql = SPARQLWrapper("http://dbpedia.org/sparql")
sparql.setReturnFormat(JSON)

place = 'Place'
person = 'Person'
org = 'Organisation'
product = 'Work'  # Organisation,,,?
work = 'Work'
event = 'Event'
language = "Language"

norp = ["EthnicGroup", "PoliticalParty"]  # low coverage
fac = ['Infrastructure', 'Airport', "Building", 'Bridge', "Highway"]  # low coverage

groups_dict = {
    "Person"        : ["Person"],
    "GPE"           : ["Location", "Place", "Country", "SpatialThing", "Geo"],  # ? Yago:GeoEntity/Region or geo:SpatialThing (this for all spatial things)
    "LOC"           : ["Location"],
    "PRODUCT"       : ["Work", "Organisation"],
    "EVENT"         : ["Event"],
    "FAC"           : ["Infrastructure", "Airport", "Bridge", "Highway", "Building"],  # ? geo:SpatialThing
    "LANGUAGE"      : ["Language"],
    "NORP"          : ["EthincGroup", "PoliticalParty", "Country"],  # ?
    "WORK_OF_ART"   : ["Work"],
    "LAW"           : [],
    "MONEY"         : ["Currency"],  # ?
    "DATE"          : ["Year", "Month", "Day", "Time"],  # ?
    "TIME"          : ["Time"],  # ?
    "CARDINAL"      : [],  # ?
    "ORDINAL"       : [],  # ?
    "PERCENT"       : []  # ?
}

groups = ["dbo:Person", "geo:SpatialThing", "dbo:Organisation", "dbo:Work", "dbo:Event", "dbo:Language"]


def dbpedia_format(mention):
    mention = mention.title().strip()
    mention_1 = ' '.join((mention.split()))
    mention_2 = mention_1.replace(' ', '_')
    return mention_1, mention_2


def build_query(mention, group):
    mention_1, mention_2 = dbpedia_format(mention)
    print(f"({mention_1}) & ({mention_2})")
    print(group)
    return f"""
        PREFIX owl:     <http://www.w3.org/2002/07/owl#>
        PREFIX xsd:     <http://www.w3.org/2001/XMLSchema#>
        PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
        PREFIX dc:      <http://purl.org/dc/elements/1.1/>
        PREFIX dbr:     <http://dbpedia.org/resource/>
        PREFIX dpr:     <http://dbpedia.org/property/>
        PREFIX dbpedia: <http://dbpedia.org/>
        PREFIX skos:    <http://www.w3.org/2004/02/skos/core#>
        PREFIX dbo:     <http://dbpedia.org/ontology/>
        SELECT DISTINCT ?item ?name ?page WHERE {{
            # VALUES ?groups {{dbo:Person dbo:Location}}
        {{
            # [Case 1] no disambiguation at all (eg. Twitter)
            ?item rdfs:label "{mention_1}"@en .
        }}
        UNION
        {{
            # [Case 1] lands in a redirect page (eg. "Google, Inc." -> "Google")
            ?temp rdfs:label "{mention_1}"@en .
            ?temp dbo:wikiPageRedirects ? ?item .   
        }}
        UNION
        {{
            # [Case 2] a dedicated disambiguation page (eg. Michael Jordan)
            <http://dbpedia.org/resource/{mention_2}_(disambiguation)> dbo:wikiPageDisambiguates ?item.
        }}
        UNION
        {{
            # [Case 3] disambiguation list within entity page (eg. New York)
            <http://dbpedia.org/resource/{mention_2}> dbo:wikiPageDisambiguates ?item .
        }}
        # Filter by entity class
        ?item rdf:type {group} .
        # Grab wikipedia link
        ?item foaf:isPrimaryTopicOf ?page .
        # Get name
        ?item rdfs:label ?name .
        FILTER (langMatches(lang(?name),"en"))
        # ?item rdf:type ?group .
        # ?group rdfs:label ?group_name
        # FILTER (STR(?group_name) IN ("Building", "Airport"))
    }}
    """


def generate_candidates(mention, group):
    query = build_query(mention, group)
    sparql.setQuery(query)
    results = sparql.query().convert()
    for result in results["results"]["bindings"]:
        print(result)
    return results


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
        print(name, backlinks_len)

        if backlinks_len > max_backlinks_len:
            max_backlinks_len = backlinks_len
            popular_page = result
    print("max_popularity", popular_page)
    if popular_page is None:
        return None
    return popular_page["page"]["value"]


def main():
    mention = input("mention: ")
    group_num = int(input(' [1] Person\n [2] Place\n [3] Org\n [4] Product\n [5] Event\n [6] Language\ngroup: '))
    group = groups[group_num-1]
    results = generate_candidates(mention, group)
    get_most_popular(results)
    print()


if __name__ == "__main__":
    main()
