#https://plotly.com/python/v3/graph-data-from-mysql-database-in-python/

import pandas as pd
import plotly.express as px
from google.cloud.sql.connector import connector
import os

# -----------------------------
# 1. Set Google Cloud credentials (update with your path)
# -----------------------------
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "path_to_your_service_account.json"

# -----------------------------
# 2. Connect to Cloud SQL (update with your instance details)
# -----------------------------
def make_connection():
    return connector.connect(
        "your-instance-connection-name",  # e.g., project:region:instance
        "pymysql",
        user="your-username",
        password="your-password",
        database="your-database"
    )

# -----------------------------
# 3. Fetch data from MySQL
# -----------------------------
conn = make_connection()
query = "SELECT Name, Continent, Population, LifeExpectancy, GNP FROM country"
df = pd.read_sql(query, conn)
conn.close()

# -----------------------------
# 4. Plotly Express Charts
# -----------------------------

# 1. Scatter Plot: GNP vs Life Expectancy
fig1 = px.scatter(
    df,
    x='GNP',
    y='LifeExpectancy',
    hover_data=['Name', 'Continent'],
    title='GNP vs Life Expectancy',
    color='Continent'
)
fig1.show()

# 2. Bar Chart: Population by Continent
continent_pop = df.groupby('Continent')['Population'].sum().reset_index()
fig2 = px.bar(
    continent_pop,
    x='Continent',
    y='Population',
    title='Population by Continent',
    color='Continent'
)
fig2.show()

# 3. Histogram: Life Expectancy Distribution
fig3 = px.histogram(
    df,
    x='LifeExpectancy',
    nbins=20,
    title='Life Expectancy Distribution',
    color='Continent'
)
fig3.show()
