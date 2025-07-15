import csv
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup

API_KEY = "SY84DJ9SR1DBFCTS"  # Replace with your actual key

symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "KO", "PEP", "JPM", "V"]

all_data = []
for symbol in symbols:
    print(f"Fetching data for {symbol}...")
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}"
    response = requests.get(url)
    data = response.json()

    try:
        time_series = data["Time Series (Daily)"]
        rows = []
        for date, values in time_series.items():
            rows.append({
                "Date": date,
                "Symbol": symbol,
                "Open": values["1. open"],
                "High": values["2. high"],
                "Low": values["3. low"],
                "Close": values["4. close"],
                "Volume": values["5. volume"]
            })
        new_df = pd.DataFrame(rows)
        new_df["Date"] = pd.to_datetime(new_df["Date"])
        new_df = new_df.sort_values("Date")
        
        file_name = f"{symbol}_data.csv"

        try:
            existing_df = pd.read_csv(file_name)
            existing_df["Date"] = pd.to_datetime(existing_df["Date"])
            combined_df = pd.concat([existing_df, new_df])
            combined_df = combined_df.drop_duplicates(subset=["Date"])
        except FileNotFoundError:
            combined_df = new_df

        combined_df.to_csv(file_name, index=False)
        print(f"Saved {symbol} data to {file_name}")

    except KeyError:
        print(f"Error fetching data for {symbol}. Response: {data}")

# Save all data to a single CSV file
master_df = pd.concat([pd.read_csv(f"{s}_data.csv") for s in symbols])
master_df.to_csv("stock_data.csv", index=False)
