import logging
import nltk
import os

from gensim.models import Phrases
from gensim.models.phrases import Phraser
from gensim.utils import simple_preprocess
from glob import glob
from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer

from load import load_filters

def _remove_stopwords_(texts, stop_words):
    return [[word for word in simple_preprocess(str(doc)) if word not in stop_words] for doc in texts]

def _preprocess_(text, stop_words):
    wnl = WordNetLemmatizer()
    text = text.replace("'", "")
    text = text.replace('"', "")
    words = simple_preprocess(text, deacc=True)
    words = [wnl.lemmatize(word, pos='v') for word in words]
    words = [word for word in words if word not in stop_words]
    return words

# TODO: find proper place for nested functions
# TODO: generalize to ngrams > 3
def _make_ngrams_(data_words, ngrams=2, threshold=100):
    bigram = Phrases(data_words, min_count=5, threshold=threshold)  # higher threshold fewer phrases.
    trigram = Phrases(bigram[data_words], threshold=threshold)

    bigram_mod = Phraser(bigram)
    trigram_mod = Phraser(trigram)

    def make_bigrams(texts):
        return [bigram_mod[doc] for doc in texts]

    def make_trigrams(texts):
        return [trigram_mod[bigram_mod[doc]] for doc in texts]

    if ngrams == 2:
        return make_bigrams(data_words)
    elif ngrams == 3:
        return make_trigrams(data_words)
    else:
        logging.info("ngrams > 3 not supported, using trigrams instead")
        return make_trigrams(data_words)

def transform(observations, ngrams=2, threshold=100, stop_words=None):
    if not stop_words:
        if 'stopwords' not in os.listdir(nltk.data.find("corpora")):
            nltk.download('stopwords')
        stop_words = stopwords.words('english')

    author_blacklist = load_filters("author")
    logging.info("Removing bots and suspicious authors")
    observations = observations[~observations['author'].isin(author_blacklist)]

    # After this point, 
    logging.info("Preprocessing")
    preprocess_body = observations['body'].progress_apply(lambda text: _preprocess_(text, stop_words))
    data_words = list(preprocess_body.values)

    data_words_nostops = _remove_stopwords_(data_words, stop_words)


    if ngrams > 1:
        observations['process_body'] = _make_ngrams_(data_words_nostops, ngrams, threshold)
        return observations
    else:
        observations['process_body'] = data_words_nostops
        return observations
