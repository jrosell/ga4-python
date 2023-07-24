# ga4-python

Example scripts to extract GA4 data.

## Installation

You can either install the required packages in your environment:
```
pip install google-analytics-data mysql-connector-python pandas python-dotenv
```

Or use a new environment in the current folder:
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Make a copy of .env.example to .env and edit apropietly.

## google_organic_analytics_data.py

Python script to extract daily google / organic GA4 session data. It save results to data folder and to the google_organic_analytics_data table in your configured mysql database.