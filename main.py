import argparse
import csv
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

    save_pre_proc(pre_proc_dir, pre_proc, warc_filename)

    return pre_proc


def entity_linking_stage():
    pass


def relation_extraction_stage(pre_proc_dir, relation_dir):
    patty = Patty()
    rows = patty.extract_relations_from_zip(f"{pre_proc_dir}/warcs-20221210-141217.csv", with_matcher=True)
    filename = f"{relation_dir}/{current_datetime}"  # Dir seems to be included via save file func TODO centralize this behaviour
    patty.save_file(current_datetime, rows)


def _load_proc_files_from_csv(file_path):
    with open(file_path, newline='', encoding='UTF-8') as file:
        csv_reader = csv.reader(file, quoting=csv.QUOTE_NONE, escapechar='\\')
        return [row for row in csv_reader]


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
    parser.add_argument(
        "--pre-proc-off",
        dest="skip_pre_proc",
        action='store_false',
        help="Include flag to skip preprocessing."
    )
    args = parser.parse_args()

    # TODO maybe allow turning on/off certain steps

    create_dirs(args.pre_proc_dir, args.relations_dir)

    if not args.skip_pre_proc:
        pre_proc_files = pre_proc_stage(args.pre_proc_dir, args.pre_proc_filename)
    else:
        pre_proc_files = _load_proc_files_from_csv("pre-proc/warcs-20221210-141217.csv")

    entity_linking_stage()
    print(pre_proc_files[0])

    relation_extraction_stage(args.pre_proc_dir, args.relations_dir)
