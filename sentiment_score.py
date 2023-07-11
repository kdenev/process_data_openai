import polars as pl
import os
import openai

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
sample_news = df_deduped.head(10).select('title').with_row_count()

# Openai credentials
openai.organization = "org-WUwBe3JxtbS5s9X8zhF4DY3K"
openai.api_key = "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"
# Send a query
response = openai.ChatCompletion.create(
  model="gpt-3.5-turbo",
  messages=[{"role": "user", "content": f"""Data id: {sample_news['row_nr'][8]}
             Title: {sample_news['title'][8]}
            Return the id number with sentiment score of the Title, should be a tuple. 
            Sentiment score can be postive negative or neutral."""}],
  max_tokens=100,
  temperature=0
)

# Read the response
response['choices'][0]['message']['content']

# Explore the results
# list(response)
# for i in list(response):
#     print(i)
#     print(response[i])

