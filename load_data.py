import pandas as pd
from pandas.tseries.offsets import MonthEnd, YearEnd

import numpy as np
import wrds

import config
from pathlib import Path
import wrds



db = wrds.Connection()

spx_options = {} 
for year in range(2010, 2024):  # Loop from 2001 to 2023
    table_name = f"optionm.opprcd{year}"  # Generate table name dynamically
    query = f"""
    SELECT
        date, symbol, cp_flag, volume, strike_price, exdate, open_interest, impl_volatility, best_bid, best_offer, delta, gamma, vega, theta
    FROM 
        {table_name} a 
    WHERE
        a.secid = '108105' AND #change this and add the symbol query
        a.exdate - a.date <= 100 AND
        a.exdate - a.date >= 10 AND 
        a.volume > 0 
    """
    spx_options[year] = db.raw_sql(query, date_cols=['date'])