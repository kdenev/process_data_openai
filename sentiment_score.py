import polars as pl
import pandas as pd
import os
import openai
from datetime import datetime
import json
import time

# Polars options
pl.Config.set_tbl_rows(50)
pl.Config.set_tbl_cols(20)
pl.Config.set_fmt_str_lengths(100)

# Openai credentials
os.environ['OPENAI_API_KEY'] = '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'
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

for i in sample_news['row_nr']:

  # Send a query

  response = openai.ChatCompletion.create(
      model="gpt-3.5-turbo",
      messages=[{"role": "user", "content": 
                f"""Data id: {sample_news['row_nr'][i]}
                Title: {sample_news['title'][i]}
                Return the id number
                , sentiment score of the title (Sentiment score can be postive negative or neutral.)
                , the most import word for the sentiment
                , the country it relates to(if not available use "Other")
                , the region it relates - options are US, China, UK, Americas, Asia, Europe, Africa, Other
                and the tone of the title is in range from 1 to -1 with intervals of .1(always return a float).
                Data format of the response is a dictionary.
                Keys values are -  'id', 'sentiment', 'word', 'country', 'region', 'tone'.
                Return should do not contain any other characters other than the dictionary!
                String values should be writen with double quotes!"""}],
      max_tokens=200,
      temperature=0
    )
  
  # Read the response
  loop_df = pl.DataFrame(json.loads(response['choices'][0]['message']['content']))
  # time.sleep(1)
  openai_response = openai_response.vstack(loop_df)

output = sample_news.join(openai_response, how='inner', left_on='row_nr', right_on='id')