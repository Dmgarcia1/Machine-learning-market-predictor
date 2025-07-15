import csv
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
# This script fetches daily stock data for a list of symbols from the Alpha Vantage API,
# processes the data, and saves it to CSV files.
API_KEY = "SY84DJ9SR1DBFCTS"  

symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "KO", "PEP", "JPM", "V"]

all_data = []
for symbol in symbols:
    print(f"Fetching data for {symbol}...")
    # Fetch daily stock data from Alpha Vantage API
    # The API returns data in JSON format, which we will convert to a DataFrame
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}"
    # Sends a GET request to the URL to fetch stock data
    # The response is expected to be in JSON format
    response = requests.get(url)
    #sends a GET request to the URL and parses the response as JSON
    data = response.json()

    
    try:
        #tries to access the "Time Series (Daily)" key in the JSON response
        # If the key exists, it means the data was fetched successfully
        time_series = data["Time Series (Daily)"]
        #initialize empty list to store rows of data fpr each day
        rows = []
        # Loop through each date in the time series data
        # The key metrics are extracted for each date
        #Appends each row as a dictionary to the rows list
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
        
        # Convert the list of rows into a DataFrame
        # This DataFrame will have columns for Date, Symbol, Open, High, Low,
        new_df = pd.DataFrame(rows)
        new_df["Date"] = pd.to_datetime(new_df["Date"])
        #sorted chronologically by date
        new_df = new_df.sort_values("Date")
        
        # Save the DataFrame to a CSV file named after the stock symbol
        # If the file already exists, it will append the new data to it
        file_name = f"{symbol}_data.csv"

        # Attempts to read the existing CSV file
        try:
            existing_df = pd.read_csv(file_name)
            #reads the rows by date to compare with the new data
            existing_df["Date"] = pd.to_datetime(existing_df["Date"])
            # Combines the existing data with the new data
            combined_df = pd.concat([existing_df, new_df])
            #removes duplicate entries based on the "Date" column
            combined_df = combined_df.drop_duplicates(subset=["Date"])
        
        #if the file doesn't exist yet just use the new data
        except FileNotFoundError:
            combined_df = new_df

        # Save the combined DataFrame back to the CSV file
        combined_df.to_csv(file_name, index=False)
        print(f"Saved {symbol} data to {file_name}")

    #catches errors if the "Time Series (Daily)" key is not found in the response
    except KeyError:
        print(f"Error fetching data for {symbol}. Response: {data}")

# Save all data to a single CSV file
#loops through each symbol's CSV file and reads it into a DataFrame (concatenates them into a single DataFrame)
master_df = pd.concat([pd.read_csv(f"{s}_data.csv") for s in symbols])
#saves to new CSV file named "stock_data.csv"
master_df.to_csv("stock_data.csv", index=False)
