import csv
import requests
from bs4 import BeautifulSoup
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv


def scrape_books(url):
    with open('books.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Title", "Rating", "Price", "Availability"])

        for i in range(1, 51):  # 50 pages to scrape
            response = requests.get(url + f'/catalogue/page-{i}.html')
            soup = BeautifulSoup(response.text, 'html.parser')
            books = soup.find_all('article', class_='product_pod')

            for book in books:
                title = book.find('h3').find('a').get('title')
                rating = book.find('p').get('class')[1]
                price = book.find('p', class_='price_color').text
                availability = book.find('p', class_='instock availability').text.strip()
                writer.writerow([title, rating, price, availability])


def transform_data():
    # Loading the data
    df = pd.read_csv('books.csv')

    # Defining a mapping for ratings
    rating_mapping = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}

    # Applying transformations
    df['Rating'] = df['Rating'].map(rating_mapping)  # Converting ratings to numerical scale
    df['Price'] = df['Price'].str.replace('Â£', '').astype(float)  # Removing currency symbol and convert price to float
    df['Availability'] = df['Availability'] == 'In stock'  # Converting availability to boolean

    # Save the transformed data
    df.to_csv('books_transformed.csv', index=False)


def store_data_in_snowflake(warehouse, database, schema, stage, file_format, table, file_path):
    # Loading variables from .env
    load_dotenv()
    username = os.getenv('SNOWFLAKE_USERNAME')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    account = os.getenv('SNOWFLAKE_ACCOUNT')

    # Establishing a connection to Snowflake
    conn = snowflake.connector.connect(
        user=username,
        password=password,
        account=account
    )

    # Creating a cursor object
    cur = conn.cursor()

    # Creating a new warehouse and setting it as the current warehouse for the session
    cur.execute(f"CREATE WAREHOUSE IF NOT EXISTS {warehouse}")
    cur.execute(f"USE WAREHOUSE {warehouse}")

    # Creating a new database and schema
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    cur.execute(f"USE DATABASE {database}")

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    cur.execute(f"USE SCHEMA {schema}")

    # Using the new database and schema
    cur.execute(f"USE {database}.{schema}")

    # Creating a new stage
    cur.execute(f"CREATE STAGE IF NOT EXISTS {stage}")

    # Putting the file into the stage
    cur.execute(f"PUT file://{file_path} @{stage}")

    # Creating a file format
    cur.execute(f"""
        CREATE OR REPLACE FILE FORMAT {file_format} 
        TYPE = 'CSV' 
        FIELD_DELIMITER = ',' 
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
    """)

    # Creating a new table for the books data
    cur.execute(f"""
        CREATE OR REPLACE TABLE {table} (
            Title STRING,
            Rating NUMBER,
            Price FLOAT,
            Availability BOOLEAN
        )
    """)

    # Copying the data from the file into the table
    cur.execute(f"""
        COPY INTO {table} 
        FROM @{stage} 
        FILE_FORMAT = {file_format}
    """)

    # Closing the connection
    conn.close()


#Scraping the books
scrape_books('https://books.toscrape.com')

# Transforming the data
transform_data()

# Storing the data
store_data_in_snowflake(
    warehouse='books_warehouse',
    database='books_database',
    schema='books_schema',
    stage='books_stage',
    file_format='books_file_format',
    table='books_table',
    file_path=r'C:\Users\UtkarshTripathi\Downloads\Coding_Challenge_Week3\books_transformed.csv'
)
