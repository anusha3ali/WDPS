import argparse
import datetime
import os

from warc import process_warc_zip, save_pre_proc
from relation_extraction import Patty

current_datetime = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")


def create_dirs(*dirs):
    for directory in dirs:
        if not os.path.exists(directory):
            os.makedirs(directory)


def pre_proc_stage(pre_proc_dir, filename):
    if filename is None:
        warc_filename = f'warcs-{current_datetime}'
    else:
        warc_filename = args.filename

    pre_proc = process_warc_zip()

    save_pre_proc(pre_proc, warc_filename)


def entity_linking_stage():
    pass


def relation_extraction_stage(pre_proc_dir, relation_dir):
    patty = Patty()
    rows = patty.extract_relations_from_zip(f"{pre_proc_dir}/warcs-20221210-141217.csv", with_matcher=True)
    filename = f"{relation_dir}/{current_datetime}"  # Dir seems to be included via save file func TODO centralize this behaviour
    patty.save_file(current_datetime, rows)
# TODO Trace this error. Occurred in line 8 of warcs-20221210-141217.csv
# Traceback (most recent call last):
#   File "C:/Users/Tim/Documents/master-computer_science/WDP/WDPS/main.py", line 72, in <module>
#     relation_extraction_stage(args.pre_proc_dir, args.relations_dir)
#   File "C:/Users/Tim/Documents/master-computer_science/WDP/WDPS/main.py", line 34, in relation_extraction_stage
#     rows = patty.extract_relations_from_zip(f"{pre_proc_dir}/warcs-20221210-141217.csv", with_matcher=True)
#   File "C:\Users\Tim\Documents\master-computer_science\WDP\WDPS\relation_extraction.py", line 31, in extract_relations_from_zip
#     for row in csv_reader:
#   File "C:\Users\Tim\AppData\Local\Programs\Python\Python38\lib\encodings\cp1252.py", line 23, in decode
#     return codecs.charmap_decode(input,self.errors,decoding_table)[0]
# UnicodeDecodeError: 'charmap' codec can't decode byte 0x9d in position 2935: character maps to <undefined>


if __name__ == "__main__":
    parser = argparse.ArgumentParser("wdp")
    parser.add_argument(
        "--warc_pre_proc_out",
        dest="pre_proc_filename",
        required=False,
        help="A file name for the preprocessed warc zip.",
        type=str
    )
    parser.add_argument(
        "--pre_proc_dir",
        dest="pre_proc_dir",
        required=False,
        default="pre-proc",
        help="Directory the preprocessed file gets stored.",
        type=str
    )
    parser.add_argument(
        "--relations_dir",
        dest="relations_dir",
        required=False,
        default="relations",
        help="Directory the relations file gets stored.",
        type=str
    )
    args = parser.parse_args()

    # TODO maybe allow turning on/off certain steps

    create_dirs(args.pre_proc_dir, args.relations_dir)

    # pre_proc_stage(args.pre_proc_dir, args.pre_proc_filename)

    entity_linking_stage()

    relation_extraction_stage(args.pre_proc_dir, args.relations_dir)
