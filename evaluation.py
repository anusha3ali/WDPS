import spacy
import spacy_transformers
import spacy_dbpedia_spotlight
import csv
import time

# nlp = spacy.load("en_core_web_trf", disable=["tagger", "attribute_ruler", "lemmatizer"])
# nlp.add_pipe('dbpedia_spotlight')

# def _load_proc_files_from_csv(file_path):
#     with open(file_path, newline='', encoding='UTF-8') as file:
#         csv_reader = csv.reader(file, quoting=csv.QUOTE_NONE, escapechar='\\')
#         return [row for row in csv_reader]

# rows = _load_proc_files_from_csv("pre-proc/warcs-20221210-141217.csv")

# for key, _, _, text in rows:
#     k = int(key.split('-')[-1])
#     text_ents = nlp(text).ents
#     for ent in text_ents:
#         print(f"ENTITY: {key}\t{ent.text}\t{ent.kb_id_}")
#     if k % 50 == 0:
#         time.sleep(10)


gold = {}
for line in open('EL-dbpedia-spotlight-2.tsv'):
    line = line.strip().split('\t')
    if len(line) < 3:
        continue
    record_type, string, entity = line
    record = record_type.split(': ')[-1]
    if not entity or len(entity) == 0: 
        continue
    gold[(record, string)] = entity.split('/')[-1]
n_gold = len(gold)

pred = {}
for line in open('popular_page.csv'):
    record = line.split(',')[0]
    entities = line[len(record)+1: ].strip('\'}"\n').split(': ')
    print(entities)
    entities = [entity.split('http://en.wikipedia.org/wiki/')[-1].split("',")[0] for entity in entities[1:]]
    print(entities)
    for e in entities:
        pred[(record, e)] = e
n_predicted = len(pred)


n_correct = sum(int(pred[i]==gold[i]) for i in set(gold) & set(pred) )
print("Evaluation ENTITY LINKING")

print('gold: %s' % n_gold)
print('predicted: %s' % n_predicted)
print('correct: %s' % n_correct)
precision = float(n_correct) / float(n_predicted)
print('precision: %s' % precision )
recall = float(n_correct) / float(n_gold)
print('recall: %s' % recall )
f1 = 2 * ( (precision * recall) / (precision + recall) )
print('f1: %s' % f1 )