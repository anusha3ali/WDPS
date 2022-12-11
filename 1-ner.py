import spacy
import spacy_transformers

nlp = spacy.load("en_core_web_trf", disable=["tagger", "attribute_ruler", "lemmatizer"])

def get_entities(nlp, text):
    doc = nlp(text)
    return {ent.text: ent.label_ for ent in doc.ents}