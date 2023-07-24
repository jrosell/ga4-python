# google_organic_analytics_data.py
import os
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient as AnalyticsClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, Filter, FilterExpression, RunReportRequest
from dotenv import load_dotenv
from datetime import datetime, timedelta
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
    landing_starts = config['landing_starts']
    sessionSourceMedium_dimension_filter = FilterExpression(
        filter=Filter(
            field_name='sessionSourceMedium',
            string_filter=Filter.StringFilter(
                value='google / organic',
                match_type=Filter.StringFilter.MatchType.EXACT
            )
        )
    )
    landingPage_dimension_filter = FilterExpression(
        filter=Filter(
            field_name='landingPage',
            string_filter=Filter.StringFilter(
                value=landing_starts,
                match_type=Filter.StringFilter.MatchType.BEGINS_WITH
            )
        )
    )
    dimension_filters = FilterExpression(
            and_group={
                'expressions': [
                    sessionSourceMedium_dimension_filter,
                    landingPage_dimension_filter,
                ]
            }
        )
    date_dimension = Dimension(name='date')
    landingPage_dimension = Dimension(name='landingPage')
    dimensions = [date_dimension, landingPage_dimension]
    response = get_google_analytics_data(
        config, 
        start_date, 
        end_date,
          dimensions = dimensions, 
          dimension_filter=dimension_filters
    )
    rows = []
    for row in response.rows:
        rows.append({            
            'date': convert_ga_date_to_yyyy_mm_dd(row.dimension_values[0].value),
            'landingPage': row.dimension_values[1].value,
            'Sessions': row.metric_values[0].value
        })
    return rows

def get_google_analytics_data(config, start_date, end_date, dimensions = None, dimension_filter=None):
    credentials_file = config['google_credentials_file']
    property_id = config['ga_property_id']
    client = AnalyticsClient.from_service_account_file(credentials_file)    
    metric = Metric(name='sessions')
    request = RunReportRequest(
        property=f'properties/{property_id}',
        dimensions=dimensions,
        metrics=[metric],
        dimension_filter=dimension_filter,
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    )
    response = client.run_report(request)   
    return response

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
    