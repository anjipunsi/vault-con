

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
        df_symbols = pd.read_csv('C:\Users\Guest Users\Downloads\vaul+concourse\ind_nifty50list.csv')
        
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
                    db_password = "test"
                    db_host = "192.168.3.174"
                    db_name = "connect_test"
                    db_port = "3307"  # Change to '3306' 
                    
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







































































































# import requests
# from bs4 import BeautifulSoup as bs
# import pandas as pd
# import csv
# from sqlalchemy import create_engine, Integer, Column, Table, MetaData, String

# # URL of the webpage to scrape
# url = 'https://screener.in/company/RELIANCE/consolidated/'

# try:
#     # Fetch the webpage content
#     response = requests.get(url)
#     response.raise_for_status()  # Raise an HTTPError for bad responses

#     # Parse the content with BeautifulSoup
#     soup = bs(response.text, 'html.parser')

#     # Find the section containing the profit-loss data
#     profit_loss_section = soup.find('section', id="profit-loss")
#     if not profit_loss_section:
#         raise ValueError("Failed to find the profit-loss section in the webpage.")
    
#     # Find the table within the section
#     table = profit_loss_section.find("table")
#     if not table:
#         raise ValueError("Failed to find the table in the profit-loss section.")
    
#     # Extract table data
#     table_data = []
#     for row in table.find_all('tr'):
#         row_data = [cell.text.strip() for cell in row.find_all(['th', 'td'])]
#         table_data.append(row_data)
    
#     # Write table data to a CSV file
#     with open("table_data.csv", 'w', newline='') as file:
#         writer = csv.writer(file)
#         writer.writerows(table_data)
    
#     # Convert table data to a Pandas DataFrame
#     df_table = pd.DataFrame(table_data)
    
#     # Set the first row as the header and drop it from data
#     df_table.columns = df_table.iloc[0]
#     df_table = df_table[1:]
#     df_table.reset_index(drop=True, inplace=True)

#     # Ensure 'Narration' is set correctly
#     df_table.insert(0, 'id', range(1, len(df_table) + 1))
#     df_table.insert(1, 'Narration', df_table.iloc[:, 1])
#     df_table = df_table.drop(df_table.columns[2], axis=1)

#     # Extract and create a new DataFrame for Narration and TTM
#     df_ttm = df_table[['id', 'Narration', 'TTM']].copy()
#     df_ttm = df_ttm.reset_index(drop=True)

#     # Drop the TTM column before melting
#     df_table = df_table.drop(columns=['TTM'])

#     # Reshape the DataFrame including 'id'
#     df_melted = pd.melt(df_table, id_vars=['id', 'Narration'], var_name='Year', value_name='Value')
#     df_melted = df_melted.sort_values(by=['id', 'Narration', 'Year']).reset_index(drop=True)

#     # Remove 'id' column for insertion into profit_loss_data
#     df_melted_no_id = df_melted.drop(columns=['id'])

#     # Print data to debug
#     print("Melted DataFrame (no id):")
#     print(df_melted_no_id.head(20))

#     print("TTM DataFrame:")
#     print(df_ttm.head(20))

#     # Database connection details
#     db_user = "root"
#     db_password = "test"
#     db_host = "localhost"
#     db_name = "connect_test"
#     db_port = "3307"  # Change to '3306' if needed
    
#     # Create SQLAlchemy engine
#     engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

#     # Define metadata
#     metadata = MetaData()

#     # Define the profit_loss_data table
#     profit_loss_data = Table(
#         'profit_loss_data', metadata,
#         Column('id', Integer, primary_key=True, autoincrement=True),
#         Column('Narration', String(255)),
#         Column('Year', String(255)),
#         Column('Value', String(255))
#     )

#     # Define the ttm_data table
#     ttm_data = Table(
#         'ttm_data', metadata,
#         Column('id', Integer, primary_key=True, autoincrement=True),
#         Column('Narration', String(255)),
#         Column('TTM', String(255))
#     )

#     # Create tables
#     metadata.create_all(engine)

#     # Insert data into profit_loss_data table
#     df_melted_no_id.to_sql('profit_loss_data', engine, if_exists='append', index=False)

#     # Insert data into ttm_data table
#     df_ttm.to_sql('ttm_data', engine, if_exists='append', index=False)

# except Exception as e:
#     print(f"An error occurred: {e}")


































































# # original code

# import requests
# from bs4 import BeautifulSoup as bs
# import pandas as pd
# import csv
# from sqlalchemy import create_engine

# # URL of the webpage to scrape
# url = 'https://screener.in/company/RELIANCE/consolidated/'

# try:
#     # Get the webpage content
#     webpage = requests.get(url)
#     webpage.raise_for_status()  # Raise an HTTPError for bad responses
#     soup = bs(webpage.text, 'html.parser')

#     # Find the section containing the profit-loss data
#     data = soup.find('section', id="profit-loss")
#     if not data:
#         raise ValueError("Failed to find the profit-loss section in the webpage.")
    
#     tdata = data.find("table")
#     if not tdata:
#         raise ValueError("Failed to find the table in the profit-loss section.")
    
#     # Extract table data
#     table_data = []
#     for row in tdata.find_all('tr'):
#         row_data = [cell.text.strip() for cell in row.find_all(['th', 'td'])]
#         table_data.append(row_data)
    
#     # Write the table data to a CSV file
#     with open("table_data.csv", 'w', newline='') as file:
#         writer = csv.writer(file)
#         writer.writerows(table_data)

#     # Convert the table data to a Pandas DataFrame
#     df_table = pd.DataFrame(table_data)
   
    


#     # Debug the DataFrame
#     print("Initial DataFrame:")
#     print(df_table.head(5))

#     # Check for blank or invalid column names
#     print("Column names before setting header:")
#     print(df_table.columns.tolist())

#     # Fix column names: The first row should be the header
#     df_table.columns = df_table.iloc[0]  # Set the first row as the header
#     df_table = df_table[1:]  # Remove the header row from the data

#     if not df_table.empty:
#         df_table.columns = ['Narration'] + df_table.columns[1:].tolist()

    

#     # Drop any columns with blank names or invalid data
#     df_table = df_table.loc[:, df_table.columns.notna()]
#     df_table.columns = df_table.columns.str.strip()

#     # Reset index
#     # df_table.reset_index(drop=True, inplace=True)

#     # Check DataFrame columns and data after cleanup
#     print("DataFrame columns after cleanup:")
#     print(df_table.columns)
#     print(df_table.head(5))

#     # Ensure all column names are non-blank and unique
#     if df_table.columns.duplicated().any():
#         raise ValueError("Duplicate column names found in DataFrame.")
    
#     if df_table.columns.isnull().any() or (df_table.columns == '').any():
#         raise ValueError("Blank column names found in DataFrame.")


#     # Ensure percentage columns are handled correctly
#     if 'OPM %' in df_table.columns:
#         df_table['OPM %'] = df_table['OPM %'].str.replace('%', '').astype(float) / 100
#     if 'Tax %' in df_table.columns:
#         df_table['Tax %'] = df_table['Tax %'].str.replace('%', '').astype(float) / 100
#     if 'Dividend Payout %' in df_table.columns:
#         df_table['Dividend Payout %'] = df_table['Dividend Payout %'].str.replace('%', '').astype(float) / 100

#     # Print DataFrame and column names before database upload
#     print("DataFrame before loading to database:")
#     print(df_table.head(5))

#     # Database connection details
#     db_user = "root"
#     db_password = "test"
#     db_host = "localhost"
#     db_name = "connect_test"
#     db_port = "3307"  # Change to '3306' if running inside Docker

#     # Create SQLAlchemy engine
#     engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    
#     # Load the DataFrame into the MySQL database
#     df_table.to_sql('profit_loss_data', engine, if_exists='replace', index=False)

#     print("Data loaded successfully into MySQL database!")
# except requests.RequestException as e:
#     print(f"Error fetching data from URL: {e}")
# except ValueError as e:
#     print(f"Value Error: {e}")
# except Exception as e:
#     print(f"An error occurred: {e}")





























# import requests
# from bs4 import BeautifulSoup as bs
# import pandas as pd
# import csv
# import psycopg2
# from sqlalchemy import create_engine
 
# url = 'https://screener.in/company/RELIANCE/consolidated/'
# webpage = requests.get(url)
# soup = bs(webpage.text, 'html.parser')
 
# data = soup.find('section', id="profit-loss")
# tdata = data.find("table")
 
# table_data = []
# for row in tdata.find_all('tr'):
#     row_data = []
#     for cell in row.find_all(['th', 'td']):
#         row_data.append(cell.text.strip())
#     table_data.append(row_data)
 
# with open("table_data.csv", 'w', newline='') as file:
#     writer = csv.writer(file)
#     writer.writerows(table_data)
 
# df_table = pd.DataFrame(table_data)
# df_table.iloc[0, 0] = 'Section'
# df_table.columns = df_table.iloc[0]
# df_table = df_table[1:]
 
# # Ensure only valid numeric data is processed with eval
# def safe_eval(val):
#     try:
#         return eval(val)
#     except:
#         return val
 
# for i in df_table.iloc[:, 1:].columns:
#     df_table[i] = df_table[i].str.replace(',', '').str.replace('%', '/100').apply(safe_eval)
 
# db_host = "192.168.3.174"
# db_name = "concourse"
# db_user = "concourse_user"
# db_password = "concourse_pass"
# db_port = "5432"
 
# engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
 
# # Load the DataFrame into the PostgreSQL database
# df_table.to_sql('profit_loss_data', engine, if_exists='replace', index=False)
 
# print("Data loaded successfully into PostgreSQL database!")
