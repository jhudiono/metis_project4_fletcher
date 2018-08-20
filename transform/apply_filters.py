# input = dataframe of raw data, possibly restricted to several subreddits.

import logging
import pandas as pd
from glob import glob
from langdetect import detect
from tqdm import tqdm, tqdm_pandas
from textblob import TextBlob

tqdm.pandas()

BLACKLIST = {}

def load_filters(types=["author", "body"], log_level=logging.INFO):
    """Load and store blacklisted text from data/filtering/*.

    Keyword arguments:
    types -- names of folders in data/filtering that should be included
    """
    logging.basicConfig()
    logging.getLogger().setLevel(log_level)
    filter_path = "data/filtering/{}/*.txt"
    for t in types:
        BLACKLIST[t] = []
        paths = glob(filter_path.format(t))
        for path in paths:
            with open(path, "r") as ffile:
                lines = ffile.readlines()
            BLACKLIST[t].extend([l.replace("\n", "") for l in lines])
    logging.debug(BLACKLIST)

def filter(observations, log_level=logging.INFO):
    """Return a Pandas dataframe of the relevant data.

    Keyword arguments:
    observations -- dataframe of raw data, possibly restricted to several subreddits
    """
    logging.basicConfig()
    logging.getLogger().setLevel(log_level)
    # TODO: generalize this
    # apply author filters
    observations = observations[~observations['author'].isin(BLACKLIST['author'])]
    # apply body filters
    observations = observations[observations['body'].progress_apply(__filter_body__)]
    return observations

def __filter_body__(text):
    # remove if: less than 10 words, contains blacklisted phrase, not English
    text = text.lower().replace("\n", " ")
    if len(text.split(" ")) < 10:
        return False
    for bp in BLACKLIST:
        if bp.lower() in text:
            return False
    try:
        if detect(text) != 'en':
            return False
    except Exception as err:
        logging.error(err)
        logging.debug(text)
        return False
    return True



