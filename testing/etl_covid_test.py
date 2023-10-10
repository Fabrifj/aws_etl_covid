import pandas as pd
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta  
import time
from sqlalchemy import create_engine



def lambda_handler(event, context):
    SOURCE = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/"

    #Set db variables
    COVID_HOST='coviddb.ci5us37i3fra.us-east-2.rds.amazonaws.com'
    COVID_PORT=3306
    COVID_USER='admin_db'
    COVID_PASSWORD='p#%tG3*$8SU&YmAg'
    COVID_DB='coviddb'
    # Start the timer
    start_time = time.time()

    # Select the date
    selected_date = datetime.now()
    searched_date = (selected_date - relativedelta(years=3)).strftime('%m-%d-%Y') 
    searched_date_plus =  (selected_date - relativedelta(years=3) + timedelta(days=1)).strftime('%m-%d-%Y')
    print(searched_date+ "  "+ searched_date_plus)

    
    try:
        print("Conectado a las base de datos")
        # Connection string
        DATABASE_URL = f"mysql+pymysql://{COVID_USER}:{COVID_PASSWORD}@{COVID_HOST}:{COVID_PORT}/{COVID_DB}"
        engine = create_engine(DATABASE_URL)

        # Download the file from GitHub
        url = SOURCE + str(searched_date) + ".csv"
        print(url)
        df = pd.read_csv(url)
        print(f"Shape DataFrame: {df.shape}")
        print(f"Colums: {df.columns}")

        # Drop Admin2 and FIPS columns
        if 'Admin2' in df.columns:
            df.drop('Admin2', axis=1, inplace=True)
        if 'FIPS' in df.columns:
            df.drop('FIPS', axis=1, inplace=True)

        # Rewrite the columns names 
        old_columns = ['Province/State','Country/Region','Last Update','Lat','Long_']
        new_columns = ['Province_State','Country_Region','Last_Update','Latitude','Longitude']

        for index,old_column in enumerate(old_columns):
            if  old_column in df.columns:
                df.rename(columns={old_column: new_columns[index]}, inplace=True)
        
        df_clean = df
        # Clean date anomalies !! What happend if there is nul date ? 
        df_clean['Last_Update'] = pd.to_datetime(df['Last_Update'], errors='coerce')

        df_clean['Date_Part'] = df_clean['Last_Update'].dt.strftime('%m-%d-%Y')

        # Filter the DataFrame by the specific date
        df_filtered = df_clean[(df_clean['Date_Part'] == searched_date) | (df_clean['Date_Part'] == searched_date_plus)]

        # Optionally, you can drop the temporary 'Date_Part' column
        df_filtered.drop(columns=['Date_Part'], inplace=True)

        df_clean = df_filtered.copy()

        # Create day column
        df_clean['Day'] = df_clean['Last_Update'].apply(lambda x: x.strftime('%A'))

        # Remove inconsistent data
        df_clean = df_clean[df_clean['Province_State'] != 'Unknown']
        df_clean['Province_State'].fillna(df_clean['Country_Region'], inplace=True)

        # Remove location anomalies
        df_clean = df_clean[df_clean['Latitude'].notnull()]

        print(df_clean.shape)
        df_clean.to_sql('test_covid', engine, if_exists='append', index=False)
        
        # Measure elapsed time
        elapsed_time = time.time() - start_time
        
        return {
            'statusCode': 200,
            'body': f'Data cleaning successfully completed. Elapsed time: {elapsed_time:.2f} seconds.'
        }

    except pd.errors.EmptyDataError:
        return {
            'statusCode': 500,
            'body': 'The CSV file is empty.'
        }

    except pd.errors.ParserError:
        return {
            'statusCode': 500,
            'body': 'Error parsing the CSV file.'
        }

    except Exception as e:
        # A generic error catch for any other type of error
        return {
            'statusCode': 5001,
            'body': f'Unexpected error: {e}'
        }
        
    finally:
        # Closing database connection if it exists.
        if engine:
            engine.dispose()
            print("MySQL connection is closed.")




