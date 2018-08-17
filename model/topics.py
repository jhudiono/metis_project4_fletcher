import pandas as pd
import logging

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import TruncatedSVD


INPUT_PATH = "data/pickle/{}.pkl"

def go(maxlen=30000, min_df=2, num_topics=20):
    #logging.basicConfig()
    #logging.getLogger().setLevel(logging.DEBUG)

    observations = pd.read_pickle(INPUT_PATH.format("sampled_by_subreddit"))
    #logging.info(str(observations.shape))

    documents = observations['body'][:maxlen]
    vectorizer = CountVectorizer(min_df=min_df, stop_words = 'english')
    dtm = vectorizer.fit_transform(documents.values)

    lsa = TruncatedSVD(num_topics, algorithm = 'arpack')
    dtm_lsa = lsa.fit_transform(dtm.asfptype())

    topic_word_dist = pd.DataFrame(lsa.components_.round(5),
            index = ['t{}'.format(t) for t in range(num_topics)],
            columns = vectorizer.get_feature_names())
    return dtm_lsa, topic_word_dist
