import Extract
import pandas as pd 

# Set of Data Quality Checks Needed to Perform Before Loading
def Data_Quality(df):
    try:
        # Check whether the DataFrame is empty
        if df.empty:
            print('No Songs Extracted')
            return False

        # Enforcing Primary keys since we don't need duplicates
        if pd.Series(df['played_at']).is_unique:
            pass
        else:
            raise Exception("Primary Key Exception: Data might contain duplicates")

        # Checking for nulls in the DataFrame
        if df.isnull().values.any():
            raise Exception("Null values found")

        print("Data quality check passed")
        return True

    except Exception as e:
        print(f"Data quality check failed: {e}")
        return False


# Writing some Transformation Queries to get the count of artist
def Transform_df(load_df):

    # Applying transformation logic
    Transformed_df = load_df.groupby(['timestamp', 'song_name', 'artist_name'], as_index=False).count()
    Transformed_df.rename(columns={'played_at': 'count'}, inplace=True)

    # Creating a Primary Key based on Timestamp and artist name
    Transformed_df["ID"] = Transformed_df['timestamp'].astype(str) + "-" + Transformed_df["artist_name"]

    return Transformed_df[['ID','timestamp','song_name', 'artist_name']]



