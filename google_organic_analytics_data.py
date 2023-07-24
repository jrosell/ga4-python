# google_organic_analytics_data.py
import os
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient as AnalyticsClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, Filter, FilterExpression, RunReportRequest
from dotenv import load_dotenv
from datetime import datetime, timedelta
import mysql.connector

def main():
    config = get_dotenv()
    results = get_google_organic_analytics_data(
        config,
        get_date_days_ago(config.last_days),
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
        'database': os.getenv('DB_DATABASE'),
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
    upsert_csv_to_mysql(config, filename, "google_organic_analytics_data")

def get_date_days_ago(days_ago):
    today = datetime.today()
    target_date = today - timedelta(days=days_ago)
    return target_date.strftime('%Y-%m-%d')

def convert_ga_date_to_yyyy_mm_dd(date_str):
    date_obj = datetime.strptime(date_str, '%Y%m%d')
    return date_obj.strftime('%Y-%m-%d')



def upsert_csv_to_mysql(config, csv_file, table_name):
    host = config.host;
    user = config.user;
    password = config.password;
    database = config.database;
    df = pd.read_csv(csv_file)
    data = df.to_dict('records')
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    cursor = connection.cursor()
    insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))}) "
    update_query = f"ON DUPLICATE KEY UPDATE {', '.join([f'{col} = VALUES({col})' for col in df.columns])}"
    for row in data:
        values = tuple(row.values())
        query = insert_query + update_query
        cursor.execute(query, values)
    connection.commit()
    connection.close()

if __name__ == "__main__":
    main()
    