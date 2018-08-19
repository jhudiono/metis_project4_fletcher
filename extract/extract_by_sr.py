import json
import logging
from collections import defaultdict
from tqdm import tqdm

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

CHUNK = 102400  # in bytes
GB = 45097156608

RAW_DATA_INPUT = "data/raw_data/RC_2017-06"
jloads = json.loads

def extract(subreddits, overwrite=False):
    rawfile = open(RAW_DATA_INPUT, "r")

    data_by_subreddit = {}
    for s in subreddits:
        data_by_subreddit[s] = []
    lines = rawfile.readlines(CHUNK)

    update_value = CHUNK
    pbar = tqdm(total=GB)

    if overwrite:
        for sr in subreddits:
            remove_file(make_filepath(sr))

    while lines:
        [process(l, data_by_subreddit) for l in lines]
        pbar.update(update_value)
        if check_len(data_by_subreddit):
            logging.info("Saving progress...")
            flush_to_file(data_by_subreddit)
        lines = rawfile.readlines(CHUNK)

    rawfile.close()

    flush_to_file(data_by_subreddit)

def check_len(data_by_subreddit):
    return sum([len(posts) for posts in data_by_subreddit.values()]) > 50000

def process(line, data_by_subreddit):
    try:
        jline = jloads(line)
        subreddit = jline['subreddit']
        if subreddit in data_by_subreddit:
            data_by_subreddit[subreddit].append(jline)
    except:
        logging.info("Unable to parse", line)

def flush_to_file(data_by_subreddit):
    for subreddit, posts in data_by_subreddit.items():
        # posts = [json1, json2, json3...]
        filepath = make_filepath(subreddit)
        logging.info("Create/append to file at {}".format(filepath))
        with open(filepath, "a+", encoding='utf-8') as outfile:
            [outfile.write('{}\n'.format(str(p))) for p in posts]
        data_by_subreddit[subreddit].clear()

def make_filepath(subreddit):
    return "data/raw_data/{}.json".format(subreddit)

def remove_file(filename):
    try:
        os.remove(filename)
        logging.info("Removed {}".format(filename))
    except:
        pass
