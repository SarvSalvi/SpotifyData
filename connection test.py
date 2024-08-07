import psycopg2
from psycopg2 import sql

# Database connection details
db_endpoint = 'spotify.c12qoyccgbhf.us-east-2.rds.amazonaws.com'
db_name = 'spotify'
db_user = 'sarvsalvi'
db_password = 'Keepgoingham1#'
db_port = 5432


# Function to check if the database exists
def check_database_exists(cursor, db_name):
    cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [db_name])
    return cursor.fetchone() is not None


# Function to create the database
def create_database():
    try:
        # Connect to the default 'postgres' database
        connection = psycopg2.connect(
            host=db_endpoint,
            database='postgres',
            user=db_user,
            password=db_password,
            port=db_port
        )
        connection.autocommit = True
        cursor = connection.cursor()

        # Check if the database exists
        if check_database_exists(cursor, db_name):
            print(f"Database '{db_name}' already exists")
        else:
            # Create the new 'spotify' database
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
            print(f"Database '{db_name}' created successfully")

        cursor.close()
    except psycopg2.Error as e:
        print(f"Error while creating database: {e}")
    finally:
        if 'connection' in locals() and connection:
            connection.close()


# Function to test connection to the new database
def test_connection():
    try:
        connection = psycopg2.connect(
            host=db_endpoint,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
        print("Connection to the 'spotify' database successful")
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")
        cursor.close()
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


# Main execution
if __name__ == "__main__":
    # Step 1: Create the database
    create_database()

    # Step 2: Test connection to the new database
    test_connection()
