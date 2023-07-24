import unittest
from google_organic_analytics_data import main, get_date_days_ago, fetch_csv_data_from, fetch_mysql_data_from, get_dotenv

class TestGoogleOrganicAnalyticsData(unittest.TestCase):
    def test_fetch_csv_data_from_yesterday(self):
        main()
        yesterday_date = get_date_days_ago(1)        
        data = fetch_csv_data_from(yesterday_date, yesterday_date)
        self.assertGreater(len(data), 0)
    def test_fetch_mysql_data_from_yesterday(self):
        main()
        yesterday_date = get_date_days_ago(1)        
        config = get_dotenv()
        data = fetch_mysql_data_from(config, yesterday_date, yesterday_date)
        self.assertGreater(len(data), 0)

if __name__ == '__main__':
    unittest.main()