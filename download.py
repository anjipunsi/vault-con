import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import csv
from sqlalchemy import create_engine, Integer
from sqlalchemy.sql import text
import time
import pymysql 

# Function to check if a column exists in a table
def column_exists(connection, table_name, column_name):
    result = connection.execute(text(f"""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        AND column_name = '{column_name}';
    """))
    return result.scalar() > 0

def fetch_data(url, retries=3, delay=5):
    for attempt in range(retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            if response.status_code == 429:
                print(f"Rate limit hit for URL {url}. Retrying after {delay} seconds...")
                time.sleep(delay)
                continue

            return response.text
        except requests.RequestException as e:
            print(f"Error fetching data from URL {url}: {e}")
            time.sleep(delay)
    raise Exception(f"Failed to fetch data from URL {url} after {retries} attempts.")

def main():
    try:
        df_symbols = pd.read_csv('C:/Users/Guest Users/Downloads/vaul+concourse/ind_nifty50list.csv')
        
        print("Column names in CSV file:", df_symbols.columns)
        
        if 'Symbol' in df_symbols.columns and 'Company Name' in df_symbols.columns:
            symbols = df_symbols['Symbol'].tolist()
            company_names = df_symbols['Company Name'].tolist()
        else:
            print("Error: The 'Symbol' or 'Company Name' columns do not exist in the CSV file.")
            return
        
        symbol_to_name = dict(zip(symbols, company_names))
        
        for symbol in symbols:
            urls = [
                f'https://screener.in/company/{symbol}/consolidated/',
                f'https://screener.in/company/{symbol}/'
            ]
            
            data_fetched = False
            for url in urls:
                try:
                    html_content = fetch_data(url)
                    soup = bs(html_content, 'html.parser')

                    profit_loss_section = soup.find('section', id="profit-loss")
                    if not profit_loss_section:
                        print(f"Failed to find the profit-loss section for symbol {symbol} at URL {url}.")
                        continue
                    
                    table = profit_loss_section.find("table")
                    if not table:
                        print(f"Failed to find the table in the profit-loss section for symbol {symbol} at URL {url}.")
                        continue
                    
                    table_data = []
                    for row in table.find_all('tr'):
                        row_data = [cell.text.strip() for cell in row.find_all(['th', 'td'])]
                        table_data.append(row_data)
                    
                    csv_filename = f"table_data_{symbol}.csv"
                    with open(csv_filename, 'w', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerows(table_data)
                    
                    df_table = pd.DataFrame(table_data)
                    
                    if df_table.empty:
                        print(f"No data found for symbol {symbol}.")
                        continue
                    
                    df_table.columns = df_table.iloc[0]
                    df_table = df_table[1:]
                    df_table.reset_index(drop=True, inplace=True)

                    df_table.insert(0, 'id', range(1, len(df_table) + 1))
                    df_table.insert(1, 'Narration', df_table.iloc[:, 1])
                    df_table = df_table.drop(df_table.columns[2], axis=1)

                    df_table.insert(2, 'company_name', symbol_to_name.get(symbol, 'Unknown Company'))

                    columns = ['id'] + [col for col in df_table.columns if col != 'id']
                    df_table = df_table[columns]

                    company_name = symbol_to_name.get(symbol, 'Unknown Company')
                    print(f"Company: {company_name}")
                    print(df_table.head())

                    print("Columns before melting:", df_table.columns)
                    print(df_table.head())

                    if 'TTM' in df_table.columns:
                        df_ttm = df_table[['Narration', 'TTM']].copy()
                        df_ttm = df_ttm.reset_index(drop=True)
                        df_table = df_table.drop(columns=['TTM'])
                    else:
                        print(f"'TTM' column not found for symbol {symbol}. Skipping TTM extraction.")
                        df_ttm = pd.DataFrame(columns=['Narration', 'TTM'])

                    df_table = df_table.drop(df_table.columns[0], axis=1)

                    df_melted = pd.melt(df_table, id_vars=['Narration', 'company_name'], var_name='Year', value_name='Value')
                    df_melted = df_melted.sort_values(by=['Narration', 'Year']).reset_index(drop=True)

                    unique_narrations = df_table['Narration'].unique()
                    narration_to_id = {narration: idx + 1 for idx, narration in enumerate(unique_narrations)}

                    df_table['ttm_id'] = df_table['Narration'].map(narration_to_id)
                    df_melted['ttm_id'] = df_melted['Narration'].map(narration_to_id)
                    df_ttm['id'] = df_ttm['Narration'].map(narration_to_id)

                    print("Melted DataFrame:")
                    print(df_melted.head(20))

                    print("TTM DataFrame:")
                    print(df_ttm.head(20))

                    db_user = "root"
                    db_password = "root"
                    db_host = "127.0.0.1"
                    db_name = "db"
                    db_port = "3333"  # Change to '3306' 
                    
                    engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
                    
                    df_melted.to_sql('profit_loss_50_companies', con=engine, if_exists='append', index=False, dtype={'ttm_id': Integer})

                    df_ttm.to_sql('ttm_50_companies', con=engine, if_exists='append', index=False, dtype={'id': Integer})

                    with engine.connect() as connection:
                        if not column_exists(connection, 'profit_loss_50_companies', 'ttm_id'):
                            try:
                                alter_table_profit_loss_sql = """
                                    ALTER TABLE profit_loss_50_companies
                                    ADD COLUMN ttm_id INT;
                                """
                                connection.execute(text(alter_table_profit_loss_sql))
                            except Exception as e:
                                print(f"Error adding column to profit_loss_50_companies: {e}")

                        if not column_exists(connection, 'ttm_50_companies', 'id'):
                            try:
                                alter_table_ttm_sql = """
                                    ALTER TABLE ttm_50_companies
                                    ADD COLUMN id INT;
                                """
                                connection.execute(text(alter_table_ttm_sql))
                            except Exception as e:
                                print(f"Error adding column to ttm_50_companies: {e}")

                        update_ttm_id_sql = """
                            UPDATE profit_loss_50_companies pl
                            JOIN ttm_50_companies ttm
                            ON pl.Narration = ttm.Narration
                            SET pl.ttm_id = ttm.id;
                        """
                        connection.execute(text(update_ttm_id_sql))
                        
                        if not column_exists(connection, 'profit_loss_50_companies', 'id'):
                            try:
                                alter_table_profit_loss_add_id_sql = """
                                    ALTER TABLE profit_loss_50_companies
                                    ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;
                                """
                                connection.execute(text(alter_table_profit_loss_add_id_sql))
                            except Exception as e:
                                print(f"Error adding 'id' column to profit_loss_50_companies: {e}")

                    print("Data loaded successfully into MySQL database!")
                    data_fetched = True
                    break

                except Exception as e:
                    print(f"An error occurred for symbol {symbol} at URL {url}: {e}")

            if not data_fetched:
                print(f"Data for symbol {symbol} could not be fetched from any URL.")

    except FileNotFoundError:
        print("Error: The file 'ind_nifty50list.csv' does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()



