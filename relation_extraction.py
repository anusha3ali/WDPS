import spacy
import spacy_transformers
from spacy.matcher import Matcher
from spacy.tokens import Span
import networkx as nx
from itertools import combinations
import csv
import datetime

pre_proc_directory  = 'pre-proc'
res_directory       = "relations"


class RelationExtractor:
    def __init__(self, nlp):
        self.nlp = nlp

    def extract_relations(self, sentence, *args, **kwargs):
        pass

    def extract_relations_from_zip(self, filename, *args, **kwargs):
        rows = []
        with open(filename, newline='', encoding='UTF-8') as file:
            csv_reader = csv.reader(file, quoting=csv.QUOTE_NONE, escapechar='\\')
            for row in csv_reader:
                document = self.nlp(row[-1])
                sentences = [s for s in document.sents]
                for sentence in sentences:
                    relations = self.extract_relations(sentence, *args, **kwargs)
                    for e1, r, e2 in relations:
                        rows.append((row[0], sentence, f"{e1}-{r}-{e2}"))
        return rows

    def save_file(self, filename, rows):
        with open(f"{res_directory}/{filename}.csv", 'w', newline='', encoding='UTF-8') as file:
            writer = csv.writer(file)
            writer.writerows([row for row in rows])


class Reverb(RelationExtractor):
    def __init__(self, nlp=None):
        if nlp is None:
            nlp = spacy.load("en_core_web_trf", disable=["textcat"])
        super().__init__(nlp)
        self.pattern = [[
            {"POS": "VERB"},
            {"POS": "PART", "OP": "?"},
            {"POS": "ADV",  "OP": "?"},
            {"POS": {"IN": ["ADJ", "ADV", "NOUN", "PRON", "DET"]}, "OP": "*"},
            {"POS": {"IN": ["PART", "ADP"]}, "OP": "?"}    
        ]]
        self.matcher = Matcher(self.nlp.vocab)
        self.matcher.add("pattern", self.pattern)
        self.ner_pos = {"PROPN", "NOUN", "NUM"}
    
    def get_relations(self, sentence):
        spans = []
        matches = self.matcher(sentence)
        spans = [sentence[start:end] for _, start, end in matches]
        spans = spacy.util.filter_spans(spans)
        return spans

    # TODO shouldnt this be extract_relations and not ..._relation to override the superclass.
    def extract_relation(self, sentence):
        relations = []
        ss = sentence.start
        
        # Get all named entities that has at least one NOURN/PROPN
        ents = [e for e in sentence.ents if {t.pos_ for t in sentence[e.start - ss:e.end - ss]} & self.ner_pos]
            
        # Stop if there are less than 2 named entities
        if len(ents) < 2:
            return relations
        
        # Extract all possible relations in the sentence
        spans = self.get_relations(sentence)

        # Find two entities on the either side of the relation to create a relation tuple
        for relation in spans:
            rs = relation.start
            left = []
            right = []
            # Divide the entities based on its orientation w.r.t. the relation
            for e in ents:
                offset = e.start - rs
                if offset < 0:
                    left.append((-offset, e))
                else:
                    right.append((offset, e))
            # Select entities that are closest to the relation on the either side
            if len(left) and len(right):
                sorted_left = [x for _, x in sorted(left)]
                sorted_right = [x for _, x in sorted(right)]
                e1, e2 = sorted_left[0], sorted_right[0]
                relations.append((e1, relation.text.lower(), e2))
        return relations


class Patty(RelationExtractor):
    def __init__(self, nlp=None):
        if nlp is None:
            nlp = spacy.load("en_core_web_trf", disable=["textcat"])
        nlp.add_pipe("merge_entities")
        super().__init__(nlp)

    def build_dependency_tree(self, sentence):
        edges = []
        for token in sentence:
            for child in token.children:
                edges.append((token, child))
        graph = nx.Graph(edges)
        return graph

    def extract_relations(self, sentence, with_matcher=False):
        print(with_matcher)
        if with_matcher:
            pattern = [
                [
                    {"POS": "VERB"},
                    {"POS": "PART", "OP": "?"},
                    {"POS": "ADV",  "OP": "?"},
                    {"POS": {"IN": ["ADJ", "ADV", "PRON", "DET"]}, "OP": "*"},
                    {"POS": {"IN": ["PART", "ADP"]}, "OP": "?"}
                ],
                [
                    {"POS": "NOUN"},
                    {"DEP": "prep"}
                ]
            ]
            matcher = Matcher(self.nlp.vocab)
            matcher.add("pattern", pattern)
            return self.extract_relations_with_matcher(sentence, matcher)
        return self.extract_relations_without_matcher(sentence)

    def extract_relations_without_matcher(self, sentence):
        no_pos = {"AUX"}
        no_dep = {"dobj", 'nsubj', 'probj', 'conj'}
        relations = []
        ss = sentence.start
        ents = sentence.ents
        
        # Create pairs of entities
        ents_pairs = [(e1, e2) for e1, e2 in list(combinations(ents, 2))]
        
        # Create a dependency tree
        graph = self.build_dependency_tree(sentence)

        # Find the shortest path between entities in a pair
        for e1, e2 in ents_pairs:
            verb, noun = 0, 0
            path = nx.shortest_path(graph, source=sentence[e1.start-ss], target=sentence[e2.start-ss])
            path = path[1:-1]
            
            # Invalid path if it contains 2+ verb/nouns
            for t in path:
                if t.pos_ == "VERB":
                    verb += 1
                elif t.pos_ in {"NOUN", "PROPN"}:
                    noun += 1
            if verb > 1 or noun > 1 or (len(path) == noun):
                continue
            
            # Remove invalid tokens (auxilary, named entity, certain types of nouns)
            path = [t for t in path
                    if t.pos_ not in no_pos
                    and not t.ent_type
                    and not (t.pos_ == "NOUN" and t.dep_ in no_dep)]
            if not len(path):
                continue
            
            # If path starts with a preposition, swap the order
            # eg. in located => located in
            if path[0].dep_ == "prep":
                path = path[::-1]

            relations.append((e1, ' '.join([t.text for t in path]).lower(), e2))
        return relations

    def extract_relations_with_matcher(self, sentence, matcher):
        relations = []
        ss = sentence.start
        ents = sentence.ents

        # Create pairs of entities
        ents_pairs = [(e1, e2) for e1, e2 in list(combinations(ents, 2))]
        
        # Create a dependency tree
        graph = self.build_dependency_tree(sentence)

        for e1, e2 in ents_pairs:
            verb, noun = 0, 0
            path = nx.shortest_path(graph, source=sentence[e1.start-ss], target=sentence[e2.start-ss])
            path = path[1:-1]
            
            if not len(path):
                continue
            
            # Invalid path if it contains 2+ verb/nouns
            for t in path:
                if t.pos_ == "VERB":
                    verb += 1
                elif t.pos_ == "NOUN":
                    noun += 1
            if verb > 1 or noun > 1:
                continue
            
            # If path starts with a preposition, swap the order
            # eg. in located => located in
            if path[0].dep_ == "prep":
                path = path[::-1] 
                
            # Find the longest span in the path that matches a pattern
            path_string = ' '.join([t.text for t in path])
            path_doc = self.nlp(path_string)
            matches = matcher(path_doc)
            spans = [path_doc[start:end] for _, start, end in matches]
            spans = spacy.util.filter_spans(spans)
            
            if len(spans) > 0:
                relations.append((e1, spans[0], e2))
        return relations


class ReverbNoNlp():
    def __init__(self, vocab):
        self.pattern = [[
            {"POS": "VERB"},
            {"POS": "PART", "OP": "?"},
            {"POS": "ADV", "OP": "?"},
            {"POS": {"IN": ["ADJ", "ADV", "NOUN", "PRON", "DET"]}, "OP": "*"},
            {"POS": {"IN": ["PART", "ADP"]}, "OP": "?"}
        ]]
        self.matcher = Matcher(vocab)
        self.matcher.add("pattern", self.pattern)
        self.ner_pos = {"PROPN", "NOUN", "NUM"}

    def get_relations(self, sentence):
        matches = self.matcher(sentence)
        spans = [sentence[start:end] for _, start, end in matches]
        spans = spacy.util.filter_spans(spans)
        return spans

    def extract_relation(self, sentence):
        relations = []
        ss = sentence.start

        # Get all named entities that has at least one NOURN/PROPN
        ents = [e for e in sentence.ents if {t.pos_ for t in sentence[e.start - ss:e.end - ss]} & self.ner_pos]

        # Stop if there are less than 2 named entities
        if len(ents) < 2:
            return relations

        # Extract all possible relations in the sentence
        spans = self.get_relations(sentence)

        # Find two entities on the either side of the relation to create a relation tuple
        for relation in spans:
            rs = relation.start
            left = []
            right = []
            # Divide the entities based on its orientation w.r.t. the relation
            for e in ents:
                offset = e.start - rs
                if offset < 0:
                    left.append((-offset, e))
                else:
                    right.append((offset, e))
            # Select entities that are closest to the relation on the either side
            if len(left) and len(right):
                sorted_left = [x for _, x in sorted(left)]
                sorted_right = [x for _, x in sorted(right)]
                e1, e2 = sorted_left[0], sorted_right[0]
                relations.append((e1, relation.text.lower(), e2))
        return relations


if __name__ == "__main__":
    reverb = Reverb()
    rows = reverb.extract_relations_from_zip(f"{pre_proc_directory}/warcs-20221210-141217.csv", with_matcher=True)
    reverb.save_file(f"{res_directory}/{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}", rows)
