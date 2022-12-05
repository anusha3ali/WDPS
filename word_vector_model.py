import csv
import subprocess
import sys
from collections import defaultdict

import spacy
import string

from gensim.models.word2vec import Word2Vec
from gensim.models.phrases import Phrases, Phraser


def load_word_vectors(model_name, word_vectors):
    subprocess.run([
        sys.executable,
        "-m",
        "spacy",
        "init",
        "vectors",
        "en",
        word_vectors,
        model_name
    ])
    print(f"New spaCy model created with word vectors. File: {model_name}")


def create_wordvecs(corpus, model_name):
    print(len(corpus))

    phrases = Phrases(corpus, min_count=30, progress_per=10000)
    print("Made Phrases")

    bigram = Phraser(phrases)
    print("Made Bigrams")

    sentences = phrases[corpus]
    print("Found sentences")
    word_freq = defaultdict(int)

    for sent in sentences:
        for i in sent:
            word_freq[i] += 1

    print(len(word_freq))

    print("Training model now...")
    w2v_model = Word2Vec(min_count=1,
                         window=2,
                         vector_size=10,
                         sample=6e-5,
                         alpha=0.03,
                         min_alpha=0.0007,
                         negative=20)
    w2v_model.build_vocab(sentences, progress_per=10000)
    w2v_model.train(sentences, total_examples=w2v_model.corpus_count, epochs=30, report_delay=1)
    w2v_model.wv.save_word2vec_format(f"data/word_vec_model/{model_name}.txt")


def corpus_from_warcs():
    nlp = spacy.load("en_core_web_sm")

    contents = []
    with open('pre-proc/warcs-20221205-211304.csv', 'r', newline='', encoding='UTF-8') as file:
        reader = csv.reader(file, quoting=csv.QUOTE_NONE, escapechar='\\')

        for row in reader:
            contents.append(row[3])
    print(f"Loaded {len(contents)} rows")

    corpus = " ".join(contents)

    print(f"Character size of corpus is {len(corpus)}")

    # TODO model can't get too long. Default limit of text is 1000000 characters.
    corpus = corpus[:1000000]
    # nlp.max_length = 10000000
    doc = nlp(corpus)

    sentences = []
    for sent in doc.sents:
        sentence = sent.text.translate(str.maketrans('', '', string.punctuation))
        words = sentence.split()
        sentences.append(words)

    print(f"Found {len(sentences)} sentences")

    return sentences


if __name__ == "__main__":
    # create_wordvecs(corpus_from_warcs(), "word_vecs")

    load_word_vectors("data/word_vec_model/sample_model", "data/word_vec_model/word_vecs.txt")
