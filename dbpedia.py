from SPARQLWrapper import SPARQLWrapper, JSON

sparql = SPARQLWrapper("http://dbpedia.org/sparql")
sparql.setReturnFormat(JSON)

place = 'Place'
person = 'Person'
org = 'Organisation'
product = 'Work' # Organisation,,,?
work = 'Work'
event = 'Event'
language = "Language"

norp = ["EthnicGroup", "PoliticalParty"] # low coverage
fac = ['Infrastructure', 'Airport', "Building", 'Bridge', "Highway"] # low coverage


# DATE - Absolute or relative dates or periods
# PERSON - People, including fictional
# GPE - Countries, cities, states
# LOC - Non-GPE locations, mountain ranges, bodies of water
# MONEY - Monetary values, including unit
# TIME - Times smaller than a day
# PRODUCT - Objects, vehicles, foods, etc. (not services)
# CARDINAL - Numerals that do not fall under another type
# ORDINAL - "first", "second", etc.
# QUANTITY - Measurements, as of weight or distance
# EVENT - Named hurricanes, battles, wars, sports events, etc.
# FAC - Buildings, airports, highways, bridges, etc.
# LANGUAGE - Any named language
# LAW - Named documents made into laws.
# NORP - Nationalities or religious or political groups
# PERCENT - Percentage, including "%"
# WORK_OF_ART - Titles of books, songs, etc.

groups = {
    "Person"        : ["Person"],
    "GPE"           : ["Location"],
    "LOC"           : ["Location"],
    "PRODUCT"       : ["Work", "Organisation"],
    "EVENT"         : ["Event"],
    "FAC"           : ["Infrastructure", "Airport", "Bridge", "Highway", "Building"], #?
    "LANGUAGE"      : ["Language"],
    "NORP"          : ["EthincGroup", "PoliticalParty", "Country"], #?
    "WORK_OF_ART"   : ["Work"],
    "LAW"           : [],
    "MONEY"         : ["Currency"], #?
    "DATE"          : ["Year", "Month", "Day", "Time"], #?
    "TIME"          : ["Time"], #?
    "CARDINAL"      : [], #?
    "ORDINAL"       : [], #?
    "PERCENT"       : [] #?
,}



groups = ["Person", "Place", "Organisation", "Work", "Event", "Language"]

def dbpedia_format(mention):
    mention_1 = mention.title().strip()
    mention_2 = mention_1.replace(' ', '_')
    return mention_1, mention_2


def build_query(mention, group):
    mention_1, mention_2 = dbpedia_format(mention)
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

        SELECT DISTINCT ?item ?page ?group WHERE {{
        # [Case 1] no disambiguation at all (eg. Twitter)
        {{
            ?item rdfs:label "{mention_1}"@en .
        }}
        # UNION
        # {{
        #     ?item rdf:type dbo:{group} .
        #     ?item rdfs:label ?temp .
        #     FILTER (CONTAINS (?temp, "{mention_1}"))
        # }}
        UNION
        # [Case 2] a dedicated disambiguation page (eg. Michael Jordan)
        {{
            <http://dbpedia.org/resource/{mention_2}_(disambiguation)> dbo:wikiPageDisambiguates ?item.
        }}
        UNION
        # [Case 3] disambiguation list within entity page (eg. New York)
        {{
            dbr:{mention_2} dbo:wikiPageDisambiguates ?item .
        }}

        # Filter by entity class
        ?item rdf:type dbo:{group} .

        # ?item rdf:type ?group .
        # ?group rdfs:label ?group_name
        # FILTER (STR(?group_name) IN ("Building", "Airport"))

        # Grab wikipedia link
        ?item foaf:isPrimaryTopicOf ?page .
    }}
    """


def generate_candidates(mention, group):
    query = build_query(mention, group)
    sparql.setQuery(query)
    results = sparql.query().convert()
    for result in results["results"]["bindings"]:
        print(result)


def main():
    while True:
        mention = input("mention: ")
        group_num = int(input(' [1] Person\n [2] Place\n [3] Org\n [4] Product\n [5] Event\n [6] Language\ngroup: '))
        group = groups[group_num-1]
        generate_candidates(mention, group)
        print()

if __name__ == "__main__":
    main()
