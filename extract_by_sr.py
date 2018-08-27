import json
import logging
from collections import defaultdict
from sqlalchemy import create_engine
from tqdm import tqdm

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

PSQL = 'postgres://{}@localhost:5432/reddit_topics'

CHUNK = 102400  # in bytes
GB = 45097156608

RAW_DATA_INPUT = "data/raw_data/RC_2017-06"
jloads = json.loads

def extract(subreddits, overwrite=False):
    """
    Args:
        subreddits--list of subreddits
        overwrite--when True, overwrite files if they exist. Otherwise append.
    """
    rawfile = open(RAW_DATA_INPUT, "r")

    data_by_subreddit = {}
    for s in subreddits:
        data_by_subreddit[s] = []
    lines = rawfile.readlines(CHUNK)

    update_value = CHUNK
    pbar = tqdm(total=GB)

    if overwrite:
        for sr in subreddits:
            __remove_file__(__make_filepath__(sr))

    while lines:
        [__process__(l, data_by_subreddit) for l in lines]
        pbar.update(update_value)
        if __check_len__(data_by_subreddit):
            logging.info("Saving progress...")
            flush_to_file(data_by_subreddit)
        lines = rawfile.readlines(CHUNK)

    rawfile.close()

    flush_to_file(data_by_subreddit)

def __check_len__(data_by_subreddit):
    return sum([len(posts) for posts in data_by_subreddit.values()]) > 50000

def __process__(line, data_by_subreddit):
    try:
        jline = jloads(line)
        subreddit = jline['subreddit']
        if subreddit in data_by_subreddit:
            data_by_subreddit[subreddit].append(jline)
    except:
        logging.info("Unable to parse", line)

def flush_to_file(data_by_subreddit):
    """Write json/dict object to file.

    Should look like {"subreddit1": [post1, post2, post3...], "subreddit2": [post1], ...}
    """
    jdump = json.dump
    for subreddit, posts in data_by_subreddit.items():
        # posts = [json1, json2, json3...]
        filepath = __make_filepath__(subreddit)
        logging.info("Create/append to file at {}".format(filepath))
        with open(filepath, "a+", encoding='utf-8') as outfile:
            [__flush_line__(p, outfile, jdump) for p in posts]

        data_by_subreddit[subreddit].clear()

def __flush_line__(post, outfile, jdump):
    jdump(post, outfile)
    outfile.write('\n')

def __make_filepath__(subreddit):
    return "data/raw_data/{}.json".format(subreddit)

def __remove_file__(filename):
    try:
        os.remove(filename)
        logging.info("Removed {}".format(filename))
    except:
        pass

def get_sql_connect(cred_path="../credentials/localhost/jessica.txt"):
    cred = ""
    with open(cred_path) as cfile:
        cred = cfile.read().strip("\n")
    cnx = create_engine(PSQL.format(cred), isolation_level='AUTOCOMMIT')
    return cnx

def write_to_sql(df, table_name, cnx):
    logging.info('Sample row:')
    logging.info(str(df.sample(1)))
    df.to_sql(table_name, cnx, if_exists='replace', index=False, chunksize=1000)
    logging.info('Done!')
