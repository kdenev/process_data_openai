<img align="center" width="1000" height="400"
    src="https://images.indianexpress.com/2023/01/openai-logo-featured.jpg?w=640"></img>
    
    
<h1 align="center">OpenAI Sentiment Analysis</h1>
<p align="center">Use OpenAI model to extract sentiment from news title.</p>

<p align="left">Extracting the sentiment of news titles(~600 in this case). This process focuses on the dataset creation. The input generated for this classification process is produced by this <a href=https://github.com/kdenev/gdelt_api>code</a> The resulting dataset has 12 columns : 'row_nr', 'title', 'date', 'weekday', 'domain', 'language', 'sourcecountry', 'sentiment', 'word', 'country', 'region', 'tone'. Sentiment can be either positive, negative or neutral.</p>

<p>You will need to insert your own API key to use the code.</p>

```python
# Openai credentials
os.environ['OPENAI_API_KEY'] = '' # <-- Insert API KEY Here
openai.api_key = os.getenv('OPENAI_API_KEY')
```
<p>Promt to openai asks for more than one piece of info: sentiment, most import word for the sentiment, country and region it relates to and the tone of the article(more or less quantified version of the sentiment) </p>

```python
# Openai promt
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
```
