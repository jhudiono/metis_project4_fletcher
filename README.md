# Workflow
I've included AskWomen.json and AskMen.json. See below for instructions on extraction for other subreddits.

# Extracting data
## Separate subreddit from total posts.
This creates a json file with only the relevant posts.
```
import extract_by_sr
extract_by_sr.extract(["AskWomen", "AskMen"])  # subreddit names
```
## (Optional) Save in PostgreSQL.
```
import load
data = load.load_data_from_json(["AskWomen"])
load.write_to_sql(data, "preprocessed_posts")
```
