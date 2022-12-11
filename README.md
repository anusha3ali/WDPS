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


# Spacy mdoel
Download Spacy model using:
```
python -m spacy download en_core_web_trf
```
