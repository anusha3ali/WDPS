FROM python:3.8-slim-buster

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

# Should pre-proc be copied or the word vec model etc?
COPY . .

CMD [ "python3", "warc.py", "--warc_output", "warc-pre-proc" ]