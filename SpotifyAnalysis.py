# Jupyter Notebook example
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2

# Database connection details
db_endpoint = 'spotify.c12qoyccgbhf.us-east-2.rds.amazonaws.com'
db_name = 'spotify'
db_user = 'sarvsalvi'
db_password = 'Keepgoingham1#'
db_port = 5432

# Query the data
def query_data():
    try:
        connection = psycopg2.connect(
            host=db_endpoint,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
        print("Connection to the 'spotify' database successful")
        query = "SELECT * FROM artist_count;"  # Adjust your query as needed
        df = pd.read_sql_query(query, connection)
        connection.close()
        return df
    except psycopg2.OperationalError as e:
        print(f"OperationalError: {e}")
    except psycopg2.InterfaceError as e:
        print(f"InterfaceError: {e}")
    except psycopg2.DatabaseError as e:
        print(f"DatabaseError: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'connection' in locals() and connection:
            connection.close()

# Query the data
df = query_data()

# Display the data
print(df.head())

# Plot the data
plt.figure(figsize=(10, 6))
sns.barplot(data=df, x='artist_name', y='count')
plt.xticks(rotation=90)
plt.title('Number of Songs Played by Each Artist')
plt.xlabel('Artist Name')
plt.ylabel('Count')
plt.tight_layout()
plt.savefig('artist_count.png')  # Save the plot as an image
plt.show()
