import json
import logging
import pandas as pd

from glob import glob
from sqlalchemy import create_engine
from tqdm import tqdm

PSQL = 'postgres://{}@localhost:5432/reddit_topics'
CHUNK = 102400  # in bytes

COLUMNS = ['author', 'body', 'id', 'score', 'subreddit', 'subreddit_id', 'gilded']

def load_filters(filter_name):
    blacklist = []
    paths = glob("data/filtering/{}/*.txt".format(filter_name))
    logging.info("Blacklsits:" + " ".join(paths))
    for path in paths:
        with open(path, "r") as ffile:
            lines = ffile.readlines()
        blacklist.extend([l.replace("\n", "") for l in lines])
    return blacklist

def load_AskReddit():
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    cnx = get_sql_connect()

    filepaths = glob("data/raw_data/AskReddit/*")
    data = []
    dappend = data.append
    jloads = json.loads

    pbar = tqdm(total=len(filepaths)*50000)
    for filepath in filepaths:
        rawfile = open(filepath, "r")
        lines = rawfile.readlines(CHUNK)
        while lines:
            for l in lines:
                try:
                    jline = jloads(l)
                    dappend(jline)
                except Exception as err:
                    logging.error(err)
                    logging.debug(l)
                pbar.update(1)
            lines = rawfile.readlines(CHUNK)
        rawfile.close()
        if len(data) > 50000:
            write_to_sql(__make_dataframe__(data), "ask_reddit_posts", cnx)
            data.clear()
        logging.info("Finished with " + filepath)

    if len(data) == 0:
         return None
    observations = __make_dataframe__(data)
    logging.info("Observations: {} rows, {} columns".format(
        observations.shape[0], observations.shape[1]))
    return observations

def __make_dataframe__(data):
    return pd.DataFrame(data=data, columns=COLUMNS)

def get_sql_connect(cred_path="credentials/localhost/jessica.txt"):
    cred = ""
    with open(cred_path) as cfile:
        cred = cfile.read().strip("\n")
    cnx = create_engine(PSQL.format(cred), isolation_level='AUTOCOMMIT')
    return cnx

def write_to_sql(df, table_name, cnx):
    logging.info('Sample row:')
    logging.info(str(df.sample(1)))
    df.to_sql(table_name, cnx, if_exists='append', index=False, chunksize=1000)

def load_data_from_psql(subreddits, table_name, maxlen=None, log_level=logging.INFO):
    QUERY = "SELECT author, body, subreddit from " + table_name + " where subreddit in {}"

    cred = ""
    with open("credentials/localhost/jessica.txt") as credfile:
        cred = credfile.read().strip("\n")

    cnx = create_engine(PSQL.format(cred), isolation_level='AUTOCOMMIT')
    read_query = QUERY.format("('"+"','".join(subreddits)+"') {}")
    if maxlen:
        read_query = read_query.format("LIMIT " + maxlen + ";")
    else:
        read_query = read_query.format(";")
    return pd.read_sql_query(read_query, cnx)

def load_data_from_json(subreddits, log_level=logging.INFO):
    logging.basicConfig()
    logging.getLogger().setLevel(log_level)

    data = []
    dappend = data.append
    jloads = json.loads
    for subreddit in subreddits:
        rawfile = open("data/raw_data/{}.json".format(subreddit), "r")
        lines = rawfile.readlines(CHUNK)
        while lines:
            for l in lines:
                try:
                    jline = jloads(l)
                    dappend(jline)
                except Exception as err:
                    logging.error(err)
                    logging.debug(l)
            lines = rawfile.readlines(CHUNK)
        rawfile.close()
    if len(data) == 0:
        return None
    observations = pd.DataFrame(data=data, columns=list(data[0].keys()))
    logging.info("Observations: {} rows, {} columns".format(
        observations.shape[0], observations.shape[1]))
    return observations
