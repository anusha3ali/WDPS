#!/bin/sh
echo "Processing webpages ..."
python3 starter_code.py data/warcs/sample.warc.gz > sample_predictions.tsv
echo "Computing the scores ..."
python3 score.py data/gold_annotations.tsv sample_predictions.tsv "RELATION"