# Import packages
import polars as pl
import os
import openai
import json
from datetime import datetime


# Polars options
pl.Config.set_tbl_rows(50)
pl.Config.set_tbl_cols(20)
pl.Config.set_fmt_str_lengths(100)

# Openai credentials
os.environ['OPENAI_API_KEY'] = '' # <-- Insert API KEY Here
openai.api_key = os.getenv('OPENAI_API_KEY')

# Import data
df = pl.read_csv('gdelt_api\output20230707.csv')
# Rows - 16829

# Check for dupes
# Yes, there are dupes
# Rows - 496
list_duped_names = (df  
                        .with_columns(pl.col('title').str.to_lowercase())
                        .groupby('title')
                        .count()
                        .filter(pl.col('count') > 1)
                        .select('title')
                        .to_series()
                    )

# Filter dupes
# Rows - 16315
df_deduped = df.with_columns(pl.col('title').str.to_lowercase()).groupby('title').first()

# Test Sample
# Covers 1 whole week
sample_news = (df_deduped
                .with_columns([
                    pl.col('seendate')
                    .str.strptime(pl.Date, format="%Y%m%dT%H%M%SZ")
                    .alias('date')
                    , pl.col('seendate')
                    .str.strptime(pl.Date, format="%Y%m%dT%H%M%SZ")
                    .dt.weekday().cast(pl.Int64).alias('weekday')    
                  ])
                .select(['title', 'date', 'weekday', 'domain', 'language', 'sourcecountry'])
                .sort('date', descending=True)
                .filter(pl.col('date') < datetime(2023, 7, 7))
                .head(600)
                .filter(pl.col('date') > datetime(2023, 6, 29))
                .with_row_count()
                .with_columns(pl.col('row_nr').cast(pl.Int64))
              )

# Sample
sample_news.groupby(['date']).count().sort('date', descending=True)
sample_news.groupby(['weekday']).count().sort('weekday')

# Output dataframe
openai_response = pl.DataFrame()
# Define batchsize
batch_size = 30

for frame in sample_news.iter_slices(n_rows=batch_size):

  # Send a query
  response = openai.ChatCompletion.create(
      model="gpt-3.5-turbo",
      messages=[{"role": "user", "content": 
                f"""Data id: {frame['row_nr'].to_list()}
                Title: {frame['title'].to_list()}
                Both inputs are list.
                Return the id number
                , sentiment score of the title (Sentiment shuold be connected to the economy/market. Sentiment score can ONLY be postive negative or neutral! )
                , the most import word for the sentiment
                , the country(can only have 1 value) it relates to, if not available use "Other"
                , the region it relates - options are US, China, UK, Americas, Asia, Europe, Africa, Other
                and the tone of the title is in range from 1 to -1 with intervals of .1(always return a float).
                Data format of the completion is a list of dictionaries. For each id return a dictionary.
                Keys values are -  'id', 'sentiment', 'word', 'country', 'region', 'tone'.
                Return should do not contain any other characters other than the list of dictionaries!
                String values should be writen with double quotes!"""}],
      temperature=0,
      max_tokens = 2000,
      n=1
    )
  
  content_len = len(json.loads(response['choices'][0]['message']['content']))

  for j in range(content_len):
    # Read the response
    loop_df = pl.DataFrame(json.loads(response['choices'][0]['message']['content'])[j])
    # Stack the dataset
    openai_response = openai_response.vstack(loop_df)

# Join the openai responses
output = sample_news.join(openai_response, how='inner', left_on='row_nr', right_on='id')

# Output to parquet
# Parquet saves the columns format
output.write_parquet('process_data_openai\output.parquet')