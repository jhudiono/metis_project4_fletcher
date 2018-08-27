# Workflow
I've included AskWomen.json and AskMen.json. See below for instructions on extraction for other subreddits.

Modeling:
* [main.ipynb](https://github.com/jhudiono/r-AskWomen-Topic-Extraction/blob/master/main.ipynb)

Analyzing results:
* [find_sample_posts.ipynb](https://github.com/jhudiono/r-AskWomen-Topic-Extraction/blob/master/find_sample_posts.ipynb)

Results saved to ```./output/<optional tag><timestamp>```. Each folder corresponds to a model for that number of topics and contains the model Pickle file, topics keywords text file, and pyLDAvis html. 
  
#### TODO
* Add better config options for data source, model parameters, and choosing model type.
* Make .py versions of the Jupyter notebooks.
* Make an end-to-end script to run the workflow.

# Extracting data
## Raw data
Get June 2017 data [here](https://www.reddit.com/r/datasets/comments/6mvrb5/reddit_june_2017_comments_are_now_available/) and unzip.
Warning: this file is very big.

Save file at ```./data/raw_data```.

## Separate subreddit from total posts.
This creates a json file with only the relevant posts.
```
import extract_by_sr
extract_by_sr.extract(["AskWomen", "AskMen"])  # subreddit names
```
## (Optional) Save in PostgreSQL.
You need to have a text file with a valid username and password in ```./credentials/<hostname>```.
  
#### TODO
* PostgreSQL setup instructions.
 
```
import load
data = load.load_data_from_json(["AskWomen"])
load.write_to_sql(data, "preprocessed_posts")
```
