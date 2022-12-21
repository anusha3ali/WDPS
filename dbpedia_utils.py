import ssl
import time

from SPARQLWrapper import SPARQLWrapper, JSON
from titlecase import titlecase
import time


ssl._create_default_https_context = ssl._create_unverified_context

sparql = SPARQLWrapper("http://dbpedia.org/sparql")
sparql.setReturnFormat(JSON)


def dbpedia_format(mention):
    mention_0 = ' '.join(mention.strip().strip("'\"").split())
    mention_1 = mention_0.replace(' ', '_')
    mention_2 = titlecase(mention_0)
    mention_3 = mention_2.replace(' ', '_')
    return mention_0, mention_1, mention_2, mention_3


def build_query(mentions, group, extra=False, tmp=False):
    mention_0, mention_1, mention_2, mention_3 = mentions
    if extra:
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


            SELECT DISTINCT ?item ?name ?page (COUNT(?source) as ?count) WHERE {{
            {{
                # [Case 1] no disambiguation at all (eg. Twitter)
                ?item rdfs:label "{mention_2}"@en .
            }}
            UNION
            {{
                # [Case 1] lands in a redirect page (eg. "Google, Inc." -> "Google")
                ?temp rdfs:label "{mention_2}"@en .
                ?temp dbo:wikiPageRedirects ? ?item .   
            }}
            UNION
            {{
                # [Case 2] a dedicated disambiguation page (eg. Michael Jordan)
                <http://dbpedia.org/resource/{mention_3}_(disambiguation)> dbo:wikiPageDisambiguates ?item.
            }}
            UNION
            {{
                # [Case 3] disambiguation list within entity page (eg. New York)
                <http://dbpedia.org/resource/{mention_3}> dbo:wikiPageDisambiguates ?item .
            }}
            UNION
            {{
                # [Case 1] no disambiguation at all (eg. Twitter)
                ?item rdfs:label "{mention_0}"@en .
            }}
            UNION
            {{
                # [Case 1] lands in a redirect page (eg. "Google, Inc." -> "Google")
                ?temp rdfs:label "{mention_0}"@en .
                ?temp dbo:wikiPageRedirects ? ?item .   
            }}
            UNION
            {{
                # [Case 2] a dedicated disambiguation page (eg. Michael Jordan)
                <http://dbpedia.org/resource/{mention_1}_(disambiguation)> dbo:wikiPageDisambiguates ?item.
            }}
            UNION
            {{
                # [Case 3] disambiguation list within entity page (eg. New York)
                <http://dbpedia.org/resource/{mention_1}> dbo:wikiPageDisambiguates ?item .
            }}

            # Filter by entity class
            ?item rdf:type {group} .

            ?source dbo:wikiPageWikiLink ?item .

            # Grab wikipedia link
            ?item foaf:isPrimaryTopicOf ?page .

            # Get name
            ?item rdfs:label ?name .
            FILTER (langMatches(lang(?name),"en"))
        }}
        """
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


            SELECT DISTINCT ?item ?name ?page (COUNT(?source) as ?count) WHERE {{
            {{
                # [Case 1] no disambiguation at all (eg. Twitter)
                ?item rdfs:label "{mention_2}"@en .
            }}
            UNION
            {{
                # [Case 1] lands in a redirect page (eg. "Google, Inc." -> "Google")
                ?temp rdfs:label "{mention_2}"@en .
                ?temp dbo:wikiPageRedirects ? ?item .   
            }}
            UNION
            {{
                # [Case 2] a dedicated disambiguation page (eg. Michael Jordan)
                <http://dbpedia.org/resource/{mention_3}_(disambiguation)> dbo:wikiPageDisambiguates ?item.
            }}
            UNION
            {{
                # [Case 3] disambiguation list within entity page (eg. New York)
                <http://dbpedia.org/resource/{mention_3}> dbo:wikiPageDisambiguates ?item .
            }}

            # Filter by entity class
            ?item rdf:type {group} .

            ?source dbo:wikiPageWikiLink ?item .

            # Grab wikipedia link
            ?item foaf:isPrimaryTopicOf ?page .

            # Get name
            ?item rdfs:label ?name .
            FILTER (langMatches(lang(?name),"en"))
        }}
        """


def generate_candidates(mention, group, query_identifier):
    mentions = dbpedia_format(mention)
    extra = mentions[0] != mentions[2]
    query = build_query(mentions, group, extra=extra)
    sparql.setQuery(query)
    sparql.setTimeout(150)
    for _ in range(2):
        try:
            results = sparql.query().convert()
            return results
        except (ConnectionError, TimeoutError):
            pass
        except Exception as e:
            pass
        time.sleep(15)
    # TODO: remove this before submitting
    print(f"{mention} in {group} not found")
