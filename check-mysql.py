import os
import mysql.connector
from dotenv import load_dotenv

def main():
    config = get_dotenv()
    data = fetch_table_data(config, "google_organic_analytics_data")
    print_table_data(data)

def get_dotenv():
    load_dotenv()
    return {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_DATABASE')
    }

def fetch_table_data(config, table_name):
    host = config['host']
    user = config['user']
    password = config['password']
    database = config['database']
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    cursor = connection.cursor()
    query = "SELECT * FROM " + table_name
    cursor.execute(query)
    rows = cursor.fetchall()
    connection.close()
    return rows

def print_table_data(data):
    for row in data:
        print(row)

if __name__ == "__main__":
    main()    
