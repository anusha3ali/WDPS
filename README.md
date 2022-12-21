# Installation

```cmd
pip3 install -r requirements.txt
RUN python3 -m spacy download en_core_web_trf
```

# Run

Run main.py which will run on data/warcs/sample.warc.gz, performing the pre-processing, entity linking and relation extraction.

# Docker quick guide

Build using:

```console
docker build --tag IMAGE_NAME
```

Run using:

```console
docker run --name CONTAINER_NAME IMAGE_NAME
```

Copy file out using

```console
docker cp CONTAINER_NAME:PATH/TO/SRC PATH/TO/DEST
```


# spaCy model
Download spaCy model using:
```console
python -m spacy download en_core_web_trf
```

# Demo

The demo is performed on a subset of the data due to time limitations.
We will also show the output when running on all the data in addition to running the demo on this subset of the data.

- Preprocessing is done over all warc files.
- Entity linking and relation extraction is done for the first 50 warc files with HTML content.

# Pipeline

The pipeline consists of 4 stages:
1. Preprocessing 
1. Entity recognition
1. Entity linking
1. Relation extraction

## Preprocessing

1. Get iterator over warc files.
1. Perform map operation over all individual warc files.
1. If file contains no HTML then skip file.
1. Normalize HTML to unescape unicode characters.
1. Pass HTML to BeautifulSoup.
1. Process title, headers and p tags.
    1. Remove empty words, non-alphanumeric words with length of 1, and words containing a character that is not alphanumeric, '$', '€', ':', '.', ',', or '-'.
    1. Sanitize words to remove unnecessary punctuation or non-alphanumeric characters at the end of the word.
    1. Split the words into sentences based on present punctuation or separation via tags.
    1. Return the text as a single string split into sentences.
1. Return mapped warc files as key-title-headers-text tuples.

## Entity recognition

1. Load spaCy model en_core_web_trf to extract named entities and sentences.
1. Turn key-title-headers-text tuples into text-key pairs, the text will be processed, and the key is passed as context.
1. Give text-key pairs to nlp.pipe.

## Entity linking

1. Take named entity.
1. If named entity is known, return mapping immediately. Otherwise continue
1. Construct SPARQL query.
1. Execute query on http://dbpedia.org/sparql endpoint.
1. If error occured, try again in 15 seconds.
1. Return result if there is any.
1. Store entity mention to Wikipedia link mapping.

## Relation extraction

1. Use ReVerb using the spaCy model vocab.
1. Pass the text into the ReVerb.
1. Loop over all sentences.
1. Per sentence extract possible relations.
1. Return all relations that have a linked entity on both sides of the relation.

# Scalability

There are two methods with which the processed was parallelized.
- nlp.pipe()
- pool.map()

## nlp.pipe

nlp.pipe is used for the named entity recognition which is necessary for the entity linking and relation extraction.


All text is combined with the key in a list of text_context pairs and passed to nlp.pipe using `as_tuples=True`.
spaCy will process the text and pass the key with it. This processing is done in parallel, where spaCy chooses how many processes to use. 
We leave this to spaCy to not run out of memory as too many model instantiations could be overly expensive.

```python
import spacy
import spacy_transformers

nlp = spacy.load("en_core_web_trf", disable=[
    "textcat",
    "tok2vec",
    "parser",
    "lemmatizer"
])
nlp.add_pipe("sentencizer")
    
text_context = [(pre_proc_file[3], pre_proc_file[0]) for pre_proc_file in pre_proc_files]

doc_tuples = nlp.pipe(text_context, as_tuples=True)
```

## pool.map

pool.map is used to parallelize the preprocessing, entity linking, and relation extraction.

This is done using the multiprocessing library where the used pool size is taken as the available CPU count.

```python
import multiprocessing as mp
pool_size = mp.cpu_count()
```

The preprocessing map is a map over individual warc files, split by the split_records iterator.

```python
with mp.Pool(processes=pool_size) as pool:
    processed_files = pool.map(process_payload, split_records(fo))
```

The entity linking and relation extraction are parallelized together over individual rows, where a row is a warc file that contained HTML.
Here each process also receives the Extraction class that is given the available vocabulary. 
This class is used as a cache throughout the processing of the row.

```python
with mp.Pool(processes=pool_size) as pool:
    extraction = Extraction(vocab)
    results = pool.map(extraction.process_row, doc_tuples)
```


# Performance

All stages combined over all the warc contents take ~80 minutes.
The result is 8602 linked entities and 1862 linked relations.
