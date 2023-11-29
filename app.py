import streamlit as st
import pandas as pd
import numpy as np
import snowflake.connector
# from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt


def load_data(warehouse, database, schema, table):
    # Load variables from .env
    # load_dotenv()
    username = st.secrets["SNOWFLAKE_USERNAME"]
    password = st.secrets["SNOWFLAKE_PASSWORD"]
    account = st.secrets["SNOWFLAKE_ACCOUNT"]

    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(
        user=username,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

    # Create a cursor object
    cur = conn.cursor()

    # Execute a query to get the data
    cur.execute(f"SELECT * FROM {table}")

    # Fetch the data
    data = cur.fetchall()

    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(data, columns=['Title', 'Rating', 'Price', 'Availability'])

    # Close the connection
    conn.close()

    return df


# Load the data
df = load_data(warehouse = 'books_warehouse',
    database = 'books_database',
    schema = 'books_schema',
    table = 'books_table')

# Create a separate dataframe for the part that gets affected by filter
filter_df = df.copy()

#Title
st.markdown("<h1 style='text-align: center; color: darkgray;'>Books Dashboard</h1>", unsafe_allow_html=True)

# KPIs
st.markdown("""
    <style>
        .big-font {
            font-size:30px !important;
        }
        .small-font {
            font-size:20px !important;
        }
        .box {
            background-color: #FF4B4B;  # Change the background color to orange
            padding: 10px;
            border-radius: 10px;  # Add rounded corners
            margin: 5px;
            height: 200px;  # Set a fixed height for the boxes
            overflow: hidden;  # Hide the overflow
            text-align: center;  # Center the text
        }
    </style>
    """, unsafe_allow_html=True)

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown('<div class="box"><h3 class="small-font">Total Number of Books</h3><h1 class="big-font">' + str(df.shape[0]) + '</h1></div>', unsafe_allow_html=True)
with col2:
    st.markdown('<div class="box"><h3 class="small-font">Average Book Price</h3><h1 class="big-font">£' + str(round(df['Price'].mean(), 2)) + '</h1></div>', unsafe_allow_html=True)
with col3:
    most_expensive_book = df.loc[df['Price'].idxmax(), 'Title']
    st.markdown('<div class="box"><h3 class="small-font">Most Expensive Book</h3><h1 class="big-font">' + most_expensive_book + '</h1></div>', unsafe_allow_html=True)

# Add a horizontal line and some space
st.markdown("---")

# Sidebar for sorting and filtering options
st.sidebar.header('Sorting and Filtering Options')

# Sorting
sort_options = ['Title (A-Z)', 'Title (Z-A)', 'Rating (Low-High)', 'Rating (High-Low)', 'Price (Low-High)', 'Price (High-Low)']
sort_option = st.sidebar.selectbox('Sort by:', sort_options)

if 'Title' in sort_option:
    filter_df['Title'] = filter_df['Title'].str.capitalize()
    filter_df = filter_df.sort_values('Title', ascending=('A-Z' in sort_option))
elif 'Rating' in sort_option:
    filter_df = filter_df.sort_values('Rating', ascending=('Low-High' in sort_option))
elif 'Price' in sort_option:
    filter_df = filter_df.sort_values('Price', ascending=('Low-High' in sort_option))

# Filtering
price_range = st.sidebar.slider('Price range', min_value=float(filter_df['Price'].min()), max_value=float(filter_df['Price'].max()), value=(float(filter_df['Price'].min()), float(filter_df['Price'].max())))
filter_df = filter_df[(filter_df['Price'] >= price_range[0]) & (filter_df['Price'] <= price_range[1])]

# Display the total results count
st.sidebar.write(f"Total results: {len(filter_df)}")

# Add some space before the table
st.markdown('\n', unsafe_allow_html=True)

# Add a heading for the table
st.header('Sorted and Filtered Books Data')

# Display the data in a table
st.dataframe(filter_df)

# Add a horizontal line and some space
st.markdown("---")

# Create two columns for the plots
col1, col2 = st.columns(2)

# Histogram for Price
with col1:
    st.subheader('Histogram for Price')
    fig, ax = plt.subplots(figsize=(5, 5))
    bins = np.arange(10, df['Price'].max() + 10, 10)
    ax.hist(df['Price'], bins=bins, color='blue', alpha=0.7, rwidth=0.85)
    plt.xlabel('Price')
    plt.ylabel('Frequency')
    plt.xticks(bins)
    st.pyplot(fig)

# Pie Chart for Ratings
with col2:
    st.subheader('Pie Chart for Ratings')
    ratings = df['Rating'].value_counts()
    fig, ax = plt.subplots(figsize=(4, 4))
    ax.pie(ratings, labels=ratings.index, startangle=90, counterclock=False, autopct='%1.1f%%', radius = 0.8)
    st.pyplot(fig)

# Create a new set of columns for the bar chart
col3, _ = st.columns([5,5])

#Bar Chart for Availability
with col3:
    st.subheader('Bar Chart for Book Availability')
    df['Availability'] = df['Availability'].replace({True: 'Available', False: 'Not Available'})  # Replace boolean values with 'Available' and 'Not Available'
    availability = df['Availability'].value_counts()
    availability = availability.reindex(['Available', 'Unavailable'], fill_value=0)  # Ensure both categories are present
    fig = plt.figure(figsize=(3, 3))
    plt.bar(availability.index, availability, color='green', alpha=0.7)
    plt.xlabel('Availability')
    plt.ylabel('Count')
    st.pyplot(fig)