import psxdata
import pandas as pd
from datetime import datetime

# 1. Define the PSX tickers you want to track (No suffixes needed for psxdata)
# Example: Systems Limited (SYS), Hub Power (HUBC), Engro (ENGRO)
portfolio_tickers = ["SYS", "HUBC", "ENGRO"]

def fetch_psx_data(tickers):
    all_data = []
    
    # We want data starting from the beginning of the current month
    start_date = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    
    for ticker in tickers:
        print(f"Fetching data for {ticker}...")
        try:
            # psxdata.stocks() returns OHLCV data directly as a DataFrame
            df = psxdata.stocks(ticker, start=start_date)
            
            if not df.empty:
                df['Ticker'] = ticker
                df['Ingestion_Date'] = datetime.now().strftime("%Y-%m-%d")
                all_data.append(df)
        except Exception as e:
            print(f"Could not fetch {ticker}: {e}")
            
    if all_data:
        # Combine all tickers into one DataFrame
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Save as a raw CSV file
        filename = f"bronze_psx_data_{datetime.now().strftime('%Y%m%d')}.csv"
        final_df.to_csv(filename, index=False)
        print(f"Successfully saved {filename}")
    else:
        print("No data found for the given tickers.")

if __name__ == "__main__":
    fetch_psx_data(portfolio_tickers)