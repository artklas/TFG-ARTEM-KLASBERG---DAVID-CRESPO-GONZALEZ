import pandas as pd
import yfinance as yf
import time
from datetime import datetime, timedelta

def load_sp500_tickers():
    try:
        table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
        return table['Symbol'].tolist()
    except Exception as e:
        print(f"Could not load S&P 500 tickers: {e}")
        return []

def create_financial_dataset(tickers):
    all_data = []
    # Keywords to assign data company multiplier to based on industry of the company.
    data_industry_keywords = [
        "financial data", 
        "stock exchanges",
        "data solutions", 
        "research and consulting", 
        "investment and capital markets", 
        "banking technology",
        "information technology services",
        "credit services",
        "internet content & information",
        "information"
    ]

    total_tickers = len(tickers)
    print(f"Processing {total_tickers} tickers...")
    
    for i, ticker in enumerate(tickers):
        try:
            # Progress indicator
            if i % 10 == 0:
                print(f"Processing {i+1}/{total_tickers} ({ticker})...")
                
            stock = yf.Ticker(ticker)
            info = stock.info
            company_name = info.get('longName', '')
            industry = info.get('industry', '')
            sector = info.get('sector', '')

            # Determine if data company
            industry_lower = industry.lower() if industry else ""
            is_data_company = any(keyword in industry_lower for keyword in data_industry_keywords)
            multiplier = 0.45 if is_data_company else 0.1625

            balance_sheet = stock.balance_sheet
            if balance_sheet.empty:
                continue

            current_shares = info.get('sharesOutstanding', None)

            for column in balance_sheet.columns:
                report_date = column
                year = pd.to_datetime(report_date).year

                if hasattr(report_date, 'tz') and report_date.tz is not None:
                    report_date = report_date.tz_localize(None)

                start_date = (report_date - timedelta(days=5)).strftime('%Y-%m-%d')
                end_date = (report_date + timedelta(days=5)).strftime('%Y-%m-%d')

                try:
                    historical_prices = stock.history(start=start_date, end=end_date)
                    avg_price = historical_prices['Close'].mean() if not historical_prices.empty else None
                    historical_market_cap = avg_price * current_shares if avg_price and current_shares else None
                except:
                    historical_market_cap = None

                year_data = balance_sheet[column]
                goodwill = year_data.get('Goodwill')
                intangible_assets = None
                other_intangible_assets = None

                #Check for other possible field names, because it sometimes doesn't work.
                possible_fields = [
                    'Other Intangible Assets', 'Other_Intangible_Assets', 'Intangible Assets',
                    'IntangibleAssets', 'IntangibleAssetsNetExcludingGoodwill',
                    'OtherIntangibleAssets', 'OtherNonCurrentAssets'
                ]

                for field in possible_fields:
                    if field in year_data:
                        intangible_assets = year_data[field]
                        break

                if 'GoodwillAndOtherIntangibleAssets' in year_data:
                    combined = year_data['GoodwillAndOtherIntangibleAssets']
                    if goodwill is not None:
                        other_intangible_assets = combined - goodwill
                else:
                    if intangible_assets is not None and goodwill is not None and intangible_assets > goodwill:
                        other_intangible_assets = intangible_assets - goodwill
                    else:
                        other_intangible_assets = intangible_assets

                total_equity = (
                    year_data.get('Total Equity Gross Minority Interest') or
                    year_data.get('TotalEquityGrossMinorityInterest') or
                    year_data.get('StockholdersEquity') or
                    year_data.get('TotalStockholdersEquity')
                )

                tangible_book_value = total_equity
                if tangible_book_value is not None:
                    if goodwill is not None:
                        tangible_book_value -= goodwill
                    if other_intangible_assets is not None:
                        tangible_book_value -= other_intangible_assets

                new_row = {
                    'Ticker': ticker,
                    'Company Name': company_name,
                    'Industry': industry,
                    'Sector': sector,
                    'Year': year,
                    'Market Cap': historical_market_cap,
                    'Tangible Book Value': tangible_book_value,
                    'Goodwill': goodwill,
                    'Other Intangible Assets': other_intangible_assets,
                    'Multiplier': multiplier
                }

                all_data.append(new_row)
            
            # sleep to avoid too many requests
            time.sleep(1)

        except Exception as e:
            print(f"Error processing ticker {ticker}: {e}")
            continue

    return pd.DataFrame(all_data)

if __name__ == "__main__":
    tickers = load_sp500_tickers()
    print(f"Loaded {len(tickers)} tickers")
    
    # Generate dataset
    data = create_financial_dataset(tickers)

    # Save dataset with timestamp to be able to rerun multiple times without deleting.
    if not data.empty:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"sp500_financial_dataset_{timestamp}.csv"
        data.to_csv(filename, index=False)
        print(f"Dataset saved to {filename}")
    else:
        print("No data collected.")