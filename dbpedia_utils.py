import ssl
from SPARQLWrapper import SPARQLWrapper, JSON

# TODO: is this only for documentation purposes? I don't see any references to it
# place = 'Place'
# person = 'Person'
# org = 'Organisation'
# product = 'Work'  # Organisation,,,?
# work = 'Work'
# event = 'Event'
# language = "Language"
#
# norp = ["EthnicGroup", "PoliticalParty"]  # low coverage
# fac = ['Infrastructure', 'Airport', "Building", 'Bridge', "Highway"]  # low coverage
#
# groups_dict = {
#     "Person": ["Person"],
#     "GPE": ["Location", "Place", "Country", "SpatialThing", "Geo"],
#     # ? Yago:GeoEntity/Region or geo:SpatialThing (this for all spatial things)
#     "LOC": ["Location"],
#     "PRODUCT": ["Work", "Organisation"],
#     "EVENT": ["Event"],
#     "FAC": ["Infrastructure", "Airport", "Bridge", "Highway", "Building"],  # ? geo:SpatialThing
#     "LANGUAGE": ["Language"],
#     "NORP": ["EthincGroup", "PoliticalParty", "Country"],  # ?
#     "WORK_OF_ART": ["Work"],
#     "LAW": [],
#     "MONEY": ["Currency"],  # ?
#     "DATE": ["Year", "Month", "Day", "Time"],  # ?
#     "TIME": ["Time"],  # ?
#     "CARDINAL": [],  # ?
#     "ORDINAL": [],  # ?
#     "PERCENT": []  # ?
# }

# TODO Okay this is used, but is it necessary to use it as an options list?
groups = ["dbo:Person", "geo:SpatialThing", "dbo:Organisation", "dbo:Work", "dbo:Event", "dbo:Language"]

ssl._create_default_https_context = ssl._create_unverified_context

sparql = SPARQLWrapper("http://dbpedia.org/sparql")
sparql.setReturnFormat(JSON)


def dbpedia_format(mention):
    mention = mention.title().strip()
    mention_1 = ' '.join((mention.split()))
    mention_2 = mention_1.replace(' ', '_')
    return mention_1, mention_2


def build_query(mention, group, query_identifier):
    mention_1, mention_2 = dbpedia_format(mention)

    # TODO: this is def bad code, but we need to pick one query and go with it
    if query_identifier == "pipeline_with_spacy":
        return f"""
        PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
        PREFIX dbpedia: <http://dbpedia.org/>
        PREFIX dbo:     <http://dbpedia.org/ontology/>
        SELECT DISTINCT ?item ?name ?page WHERE {{
            # VALUES ?groups {{dbo:Person dbo:Location}}
        {{
            # [Case 1] no disambiguation at all (eg. Twitter)
            ?item rdfs:label "{mention_1}"@en .
        }}

        UNION
        {{
            # [Case 2] a dedicated disambiguation page (eg. Michael Jordan)
            <http://dbpedia.org/resource/{mention_2}_(disambiguation)> dbo:wikiPageDisambiguates ?item.
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
    elif query_identifier == "dbpedia_with_EL" or query_identifier == "popular_entity":
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


def generate_candidates(mention, group, query_identifier):
    query = build_query(mention, group, query_identifier)
    sparql.setQuery(query)
    sparql.setTimeout(1000)
    for i in range(2):
        try:
            results = sparql.query().convert()
            return results
        except (ConnectionError, TimeoutError):
            print("Will retry again in a little bit")
        except Exception as e:
            print(e)
        time.sleep(15)
