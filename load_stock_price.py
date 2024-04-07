"""
This module pulls, clean and saves CRSP daily stock data from WRDS.
"""

from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path

import numpy as np
import pandas as pd
import wrds

"""
connection to WRDS
"""
db = wrds.Connection()


"""
Pulls daily CRSP stock data from a specified start date to end date.
Left join with dsfhdr to get ticker symbol.
"""
def pull_CRSP_daily_file(start_date, end_date):
   
    # pull one extra month of data for cleaning the data
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    start_date = start_date - relativedelta(months=1)
    start_date = start_date.strftime("%Y-%m-%d")

    query = f"""
    SELECT 
        date,
        dsf.permno, dsf.permco, 
        prc, bid, ask, shrout, cfacpr, cfacshr,
        ret, retx, htsymbol, hcomnam
    FROM crsp.dsf AS dsf
    LEFT JOIN 
        crsp.dsfhdr as dsfhdr
    ON 
        dsf.permno = dsfhdr.permno 
    WHERE 
        dsf.date BETWEEN '{start_date}' AND '{end_date}'
    """

    df = db.raw_sql(
        query, date_cols=["date"]
    )

    df = df.loc[:, ~df.columns.duplicated()]
    df["shrout"] = df["shrout"] * 1000

    return df


"""
Adjust prc with a negative sign to be positive

A negative sign means prc is a bid/ask average instead of a closing price

# Prc is the closing price or the negative bid/ask average for a trading day. 
# If the closing price is not available on any given trading day, 
# the number in the price field has a negative sign to indicate that it is a bid/ask average and not an actual closing price. 
# Please note that in this field the negative sign is a symbol and that the value of the bid/ask average is not negative.
# If neither closing price nor bid/ask average is available on a date, prc is set to zero. 
"""
def clean_prc_to_positive(df):
    
    df['prc'] = np.abs(df['prc'])
    return df


"""
Drop the stocks that have prices less than $20 during our research time period
"""
def clean_price_20(df):
    df = df.groupby('htsymbol').filter(lambda x: x['prc'].min() >= 20)
    return df


"""
Merge stock prices dataset with option metircs ticker symbol
"""
def merge_ticker(df):
    ## Shrey version to get all tickers in the optionmetrics dataset

    ## Getting equity tickers in OptionMetrics dataset
    table_name = f"optionm.securd"  # Generate table name dynamically
    query = f"""
    SELECT
        *
    FROM 
        {table_name} a
    WHERE
        a.issue_type = '0'
    """

    df_ticker = db.raw_sql(query, date_cols=['date'])
    tickers = df_ticker['ticker'].unique()

    df = df[df['htsymbol'].isin(tickers)]
    return df


"""
Future work: filtering
after getting ER dates, only wants the stock prices before or after ER
"""



if __name__ == "__main__":
    # Pull CRSP daily stock data
    df_dsf = pull_CRSP_daily_file(start_date="2010-01-01", end_date="2023-02-28")
    
    # change to postive prc
    df_dsf = clean_prc_to_positive(df_dsf)

    # future work: after getting ER dates, drop stocks less than $20
    # # drop stocks with prices less than $20
    # df_dsf = clean_price_20(df_dsf)


    # merge with optionmetrics ticker
    df_dsf = merge_ticker(df_dsf)

    # future work: filter stock prices before or after ER dates

    # save the dataset
    df_dsf[['date', 'permno', 'prc','bid','ask', 'htsymbol', 'hcomnam' ]].to_csv('data/stock_price.csv', index=False)
   