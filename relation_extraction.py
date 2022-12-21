from typing import Tuple, List

import spacy
from spacy.matcher import Matcher


class ReverbNoNlp:
    def __init__(self, vocab):
        """Initializes ReVerb without internal nlp.
        It directly receives spaCy models output and can extract relations from this.

        :param vocab: The vocabulary used during the NER.
        :type vocab: List
        """
        # Use the following patterns as rules.
        self.pattern = [[
            {"POS": "VERB"},
            {"POS": "PART", "OP": "?"},
            {"POS": "ADV", "OP": "?"},
            {"POS": {"IN": ["ADJ", "ADV", "NOUN", "PRON", "DET"]}, "OP": "*"},
            {"POS": {"IN": ["PART", "ADP"]}, "OP": "?"}
        ]]
        # Initialize matcher using vocabulary.
        self.matcher = Matcher(vocab)
        # Add patterns to matcher.
        self.matcher.add("pattern", self.pattern)
        self.ner_pos = {"PROPN", "NOUN", "NUM"}

    def get_relations(self, sentence: object) -> object:
        """Get the relations as the spaCy Span objects.

        :param sentence: spaCy sentence.
        :type sentence: object
        :return: spaCy Span.
        :rtype: object
        """
        matches = self.matcher(sentence)
        spans = [sentence[start:end] for _, start, end in matches]
        spans = spacy.util.filter_spans(spans)
        return spans

    def extract_relations(self, sentence: object) -> List[Tuple[str, str, str]]:
        """Extract relations triple from spaCy sentence.

        :param sentence: spaCy sentence.
        :type sentence: object
        :return: List of relation triples.
        :rtype: List[Tuple[str, str, str]]
        """
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
                e1, e2 = sorted_left[0].text, sorted_right[0].text
                relations.append((e1, relation.text.lower(), e2))
        return relations

    def extract_spacy_relations(self, text: object, valid_entities: dict) -> List[Tuple[str, str, str]]:
        """Extract relations where both entities have been linked to a Wikipedia URL.

        :param text: spaCy Doc text to get the sentences.
        :type text: object
        :param valid_entities: Dictionary of entity -> Wikipedia URL mappings.
        :type valid_entities: dict
        :return: List of extracted relations that are have valid linked entities.
        :rtype: List[Tuple[str, str, str]]
        """
        formatted_relations = []
        sentences = [s for s in text.sents]
        for sentence in sentences:
            relations = self.extract_relations(sentence)
            for e1, r, e2 in relations:
                # Skip extracted relation if entity 1 or entity 2 hasn't been linked.
                if e1 in valid_entities and e2 in valid_entities:
                    formatted_relations.append((e1, r, e2))
        return formatted_relations
