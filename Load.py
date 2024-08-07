import psycopg2
from dotenv import load_dotenv
import os
import Extract
import Transform
import pandas as pd

# Load environment variables from .env file
dotenv_path = "C:/Users/asus/Documents/Learning Materials/ETL/secrets.env"
load_dotenv(dotenv_path)

db_endpoint = os.getenv('db_endpoint')
db_name = os.getenv('db_name')

db_user = os.getenv('db_user')
db_password = os.getenv('db_password')


def load_to_rds(transformed_df, db_endpoint, db_name, db_user, db_password):
    try:
        connection = psycopg2.connect(
            host=db_endpoint,
            database=db_name,
            user=db_user,
            password=db_password,
            port=5432
        )
        cursor = connection.cursor()

        select_query = "SELECT 1 FROM artist_count WHERE id = %s"
        insert_query = """
           INSERT INTO artist_count (id, timestamp, artist_name, count)
           VALUES (%s, %s, %s, %s)
           """

        for index, row in transformed_df.iterrows():
            cursor.execute(select_query, (row['ID'],))
            exists = cursor.fetchone()
            if not exists:
                cursor.execute(insert_query, (row['ID'], row['timestamp'], row['artist_name'], row['count']))

        connection.commit()
        cursor.close()
    except psycopg2.DatabaseError as e:
        print(f"DatabaseError: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'connection' in locals() and connection:
            connection.close()

if __name__ == "__main__":
    # Extract data
    Extract.initiate_data_fetch()
    extracted_df = Extract.return_dataframe()
    print("Extracted DataFrame:")
    print(extracted_df)

    # Perform data quality checks
    try:
        if Transform.Data_Quality(extracted_df):
            print("Data quality check passed")
            # Transform the data
            Transformed_df = Transform.Transform_df(extracted_df)
            print("Transformed DataFrame:")
            print(Transformed_df)

            # Load the transformed data to RDS
            load_to_rds(Transformed_df, db_endpoint, db_name, db_user, db_password)
            print('Everything Ran Properly')
        else:
            print('Data quality check failed: DataFrame failed the quality checks')
    except Exception as e:
        print(f"Data quality check failed: {e}")
