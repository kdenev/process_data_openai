import polars as pl
import pandas as pd
import os
import openai
from datetime import datetime
import json

pl.Config.set_tbl_rows(20)
pl.Config.set_fmt_str_lengths(100)

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

sample_news = (df_deduped
                .with_columns([
                    pl.col('seendate')
                    .str.strptime(pl.Date, format="%Y%m%dT%H%M%SZ")
                    .alias('date')
                    , pl.col('seendate')
                    .str.strptime(pl.Date, format="%Y%m%dT%H%M%SZ")
                    .dt.weekday().alias('weekday')    
                  ])
                .select(['title', 'date', 'weekday', 'domain', 'language', 'sourcecountry'])
                .sort('date', descending=True)
                .filter(pl.col('date') < datetime(2023, 7, 7))
                .head(2300)
                .with_row_count()
              )

# Sample
sample_news.groupby(['date']).count().sort('date', descending=True)
sample_news.groupby(['weekday']).count().sort('weekday')

# Openai credentials
openai.organization = "org-WUwBe3JxtbS5s9X8zhF4DY3K"
openai.api_key = "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"

# Output dataframe
openai_response = pl.DataFrame()

for i in sample_news['row_nr'][:10]:

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
              , the region it relates - options are US, Americas, Asia, Europe, UK, Other
              and the tone of the title is in range from 1 to -1 with intervals of .1(always return a float).
              Data format of the response is a dictionary.
              Keys values are -  'id', 'sentiment', 'word', 'country', 'region', 'tone' """}],
    max_tokens=100,
    temperature=0
  )

  # Read the response
  loop_df = pl.DataFrame(json.loads(response['choices'][0]['message']['content']))

  openai_response = openai_response.vstack(loop_df)

sample_news[7]