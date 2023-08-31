import polars as pl
from yahooquery import Ticker

df = pl.read_parquet("process_data_openai\output.parquet")
(   df
    .groupby('date')
    .agg([
        pl.count().alias('n')
        , pl.sum('tone')
    ])
    .sort('date')
    )
ticker = Ticker("^GSPC")
price_info = pl.DataFrame(ticker.history(start="2023-06-30", end="2023-07-08").reset_index()[['symbol', 'date', 'adjclose']])
price_info = price_info.with_columns([
                pl.col('adjclose').shift(-1).alias('nextadjclose')
            ])