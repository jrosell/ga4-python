# google_organic_analytics_data.py
import os
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient as AnalyticsClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, Filter, FilterExpression, RunReportRequest
from dotenv import load_dotenv
from datetime import datetime, timedelta
import mysql.connector
from sqlalchemy import create_engine


def main():
    config = get_dotenv()
    results = get_google_organic_analytics_data(
        config,
        get_date_days_ago(config["last_days"]),
        get_date_days_ago(1)
    )
    save_google_organic_analytics_data(config, results)

def get_dotenv():
    load_dotenv()
    return {
        'google_credentials_file': os.getenv('GOOGLE_CREDENTIALS_FILE'),
        'ga_property_id': os.getenv('GA_PROPERTY_ID'),
        'landing_starts': os.getenv('LANDING_BEGINS_WITH'),
        'last_days': os.getenv('LAST_DAYS'),        
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_DATABASE')
    }

def get_google_organic_analytics_data(config, start_date, end_date):
    credentials_file = config['google_credentials_file']
    property_id = config['ga_property_id']
    landing_starts = config['landing_starts']
    client = AnalyticsClient.from_service_account_file(credentials_file)
    date_dimension = Dimension(name='date')
    landingPage_dimension = Dimension(name='landingPage')    
    metric = Metric(name='sessions')
    dimension_filter = FilterExpression(
        filter=Filter(
            field_name='sessionSourceMedium',
            string_filter=Filter.StringFilter(
                value='google / organic',
                match_type=Filter.StringFilter.MatchType.EXACT
            )
        )
    )
    landing_page_filter = FilterExpression(
        filter=Filter(
            field_name='landingPage',
            string_filter=Filter.StringFilter(
                value=landing_starts,
                match_type=Filter.StringFilter.MatchType.BEGINS_WITH
            )
        )
    )
    request = RunReportRequest(
        property=f'properties/{property_id}',
        dimensions=[date_dimension, landingPage_dimension],
        metrics=[metric],
        dimension_filter=FilterExpression(
            and_group={
                'expressions': [
                    dimension_filter,
                    landing_page_filter,
                ]
            }
        ),
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    )
    response = client.run_report(request)
    rows = []
    for row in response.rows:
        rows.append({            
            'date': convert_ga_date_to_yyyy_mm_dd(row.dimension_values[0].value),
            'landingPage': row.dimension_values[1].value,
            'Sessions': row.metric_values[0].value
        })
    return rows

def save_google_organic_analytics_data(config, data):
    df = pd.DataFrame(data)
    print(df)
    if not os.path.exists('data'):
        os.makedirs('data')
    timestamp = datetime.now().strftime("%Y-%m-%d")
    filename = f"data/{timestamp}_google_organic_analytics_data.csv"
    df.to_csv(filename, index=False)
    engine = create_mysql_connection(config)
    df.to_sql('google_organic_analytics_data', con=engine, if_exists='replace', index=False)
    engine.dispose()

def get_date_days_ago(days_ago):
    days_ago = int(days_ago)
    today = datetime.today()
    target_date = today - timedelta(days=days_ago)
    return target_date.strftime('%Y-%m-%d')

def convert_ga_date_to_yyyy_mm_dd(date_str):
    date_obj = datetime.strptime(date_str, '%Y%m%d')
    return date_obj.strftime('%Y-%m-%d')


def upsert_csv_to_mysql(config, csv_file, table_name):
    connection, cursor = create_mysql_connection(config)
    df = pd.read_csv(csv_file)
    df = clean_column_names(df)
    data = df.to_dict('records')    
    create_table_if_not_exist(cursor, table_name, df)
    insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))}) "
    update_query = f"ON DUPLICATE KEY UPDATE {', '.join([f'{col} = VALUES({col})' for col in df.columns])}"
    for row in data:
        values = tuple(row.values())
        query = insert_query + update_query
        cursor.execute(query, values)
    connection.commit()
    connection.close()

def create_table_if_not_exist(cursor, table_name, df):
    try:
        cursor.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
    except mysql.connector.Error as err:
        if err.errno == mysql.connector.errorcode.ER_NO_SUCH_TABLE:
            create_table_query = create_table_sql(df, table_name)
            cursor.execute(create_table_query)
        else:
            raise err
        
def create_table_sql(df, table_name):
    type_mapping = {
            'int64': 'INT',
            'float64': 'FLOAT',
            'object': 'VARCHAR(255)',
            'datetime64[ns]': 'DATETIME',
            'bool': 'BOOL',
        }
    columns = ', '.join([f'{col} {type_mapping[str(df.dtypes[col])]}' for col in df.columns])
    primary_key = ', '.join(df.columns)
    create_table_query = f"""
        CREATE TABLE {table_name} (
            {columns},
            PRIMARY KEY ({primary_key})
        )
    """
    return create_table_query

def clean_column_names(df):    
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace(r'\W', '')
    return df

def fetch_mysql_data_from(config, start_date, end_date):
    engine = create_mysql_connection(config)
    query = f"SELECT * FROM google_organic_analytics_data WHERE date BETWEEN '{start_date}' AND '{end_date}'"
    df = pd.read_sql_query(query, engine)
    engine.dispose()
    return df

def fetch_csv_data_from(start_date, end_date):
    today_date = datetime.today().strftime('%Y-%m-%d')
    csv_file = f"data/{today_date}_google_organic_analytics_data.csv"
    df = pd.read_csv(csv_file)
    df['date'] = pd.to_datetime(df['date'])
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    df_filtered = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
    return df_filtered



def create_mysql_connection(config):
    host = config['host']
    user = config['user']
    password = config['password']
    database = config['database']    
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")
    return engine

if __name__ == "__main__":
    main()
    