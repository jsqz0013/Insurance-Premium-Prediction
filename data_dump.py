import pymongo
import pandas as pd
import json

client = pymongo.MongoClient("mongodb+srv://Rais:rais123@cluster0.aulercz.mongodb.net/?retryWrites=true&w=majority")

DATA_FILE_PATH=r"C:\DS Projects\Insurance-Premium-Prediction\insurance.csv"
DATABASE_NAME="INSURANCE"
COLLECTION_NAME="INSURANCE_PROJECT"

if __name__ == "__main__":
    df = pd.read_csv(DATA_FILE_PATH)
    print(f"Rows and columns: {df.shape}")
# Conversion of Dataframe to JSON for MongoDB Insertion
    #Dropping Index
    df.reset_index(drop=True, inplace=True)
    #Transposing current dataframe MongoDB Takes data in KVP in JSON FORMAT
    json_record = list(json.loads(df.T.to_json()).values())
    print(json_record[0])

    # Inserting Data into MongoDB Database
    client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)