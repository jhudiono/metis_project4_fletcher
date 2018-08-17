import json
import logging
import pandas as pd
from langdetect import detect
from tqdm import tqdm

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

CHUNK = 102400  # in bytes
GB = 45097156608

RAW_DATA_INPUT = "data/raw_data/RC_2017-06"
BOT_LIST = []
PHRASE_LIST = []
jloads = json.loads

def extract(maxlen=None, filters=None):
    BOT_LIST = load_filter("data/filtering/reddit_bots.txt")
    PHRASE_LIST = load_filter("data/filtering/filter_text.txt")

    rawfile = open(RAW_DATA_INPUT, "r")

    data = []
    line = rawfile.readline()
    dappend = data.append
    lines = rawfile.readlines(CHUNK)

    update_value = 1
    pbar = tqdm(total=GB)
    if maxlen:
        pbar = tqdm(total=maxlen)

    while lines:
        [process(l, dappend) for l in lines]
        pbar.update(update_value)
        if maxlen and len(data) > maxlen:
            break
        lines = rawfile.readlines(CHUNK)

    rawfile.close()

    keys = get_keys(data)
    return pd.DataFrame(data=data, columns=keys)

def process(line, dappend):
    try:
        jline = jloads(line)
        if pass_filters(jline, filters=None):
            dappend(jline)
    except:
        logging.info("Unable to parse", line)

SUBREDDIT_LIST = ['AskReddit', 'hockey', 'nba', 'Overwatch', 'politics']
# author not in bots
# text not contain phrases, min length, in english, no links
def pass_filters(jline, filters):
    if filters:
        # TODO
        return True
    else:
        text = jline['body']
        author = jline['author']
        subreddit = jline['subreddit']
        # default filters
        if subreddit not in SUBREDDIT_LIST:
            return False
        if author in BOT_LIST:
            return False
        if "http://" in text:  # Remove links
            return False
        if "https://" in text:  # Remove links
            return False
        if len(text.split(" ")) < 10:  # Remove posts too short
            return False
        try:
            if detect(text) != 'en':  # Remove non-english
                return False
        except:
            logging.info("Failed to detect language.")
            logging.debug("This text broke language detection:")
            logging.debug(text)
            return False
        for p in PHRASE_LIST:  # Remove comments with this content
            if p in text:
                return False
    return True

def get_keys(data):
    if len(data) == 0:
        return None
    return list(data[0].keys())

def load_filter(filepath):
    arr = []
    with open(filepath, "r") as ffile:
        arr = ffile.readlines()
    return [b.replace("\n", "") for b in arr]
