import spacy
import spacy_transformers
import spacy_dbpedia_spotlight
import csv
import time

nlp = spacy.load("en_core_web_trf", disable=["tagger", "attribute_ruler", "lemmatizer"])
nlp.add_pipe('dbpedia_spotlight')

def _load_proc_files_from_csv(file_path):
    with open(file_path, newline='', encoding='UTF-8') as file:
        csv_reader = csv.reader(file, quoting=csv.QUOTE_NONE, escapechar='\\')
        return [row for row in csv_reader]

rows = _load_proc_files_from_csv("pre-proc/warcs-20221210-141217.csv")

for key, _, _, text in rows:
    k = int(key.split('-')[-1])
    text_ents = nlp(text).ents
    for ent in text_ents:
        print(f"ENTITY: {key}\t{ent.text}\t{ent.kb_id_}")
    if k % 50 == 0:
        time.sleep(10)