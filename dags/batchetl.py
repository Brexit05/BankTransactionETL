import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import boto3
from config import PG_URL, AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET_NAME

class BatchETL:
    def __init__(self, url=PG_URL):
        self.engine = create_engine(url)
        self.last_run_file = "last_run.txt"

    def get_last_run_time(self) -> str:
        try:
            with open(self.last_run_file, "r") as f:
                return f.read().strip()
        except FileNotFoundError:
            return None

    def update_last_run_time(self):
        now = datetime.isoformat()
        with open(self.last_run_file, "w") as f:
            f.write(now)
        return now

    def extract(self) -> pd.DataFrame:
        last_run = self.get_last_run_time()
        query = "SELECT * FROM transactions"
        if last_run:
            query += f" WHERE timestamp > '{last_run}'"
        
        df = pd.read_sql(query, self.engine)
        print(f"Extracted {len(df)} new records since {last_run}")
        return df

    def transform(self) -> pd.DataFrame:
        sql = """
        SELECT 
            account_id,
            COUNT(*) AS transaction_count,
            SUM(amount) AS total_amount,
            MAX(timestamp) AS last_transaction_time
        FROM transactions
        GROUP BY account_id;
        """
        df = pd.read_sql(sql, self.engine)
        print(f"Transformed data: {len(df)} aggregated accounts")
        return df

    def load_to_csv(self, df: pd.DataFrame, path: str = "batch_transactions.csv"):
        if df.empty:
            print("No data to load")
            return
        df.to_csv(path, index=False)
        print(f"Saved batch to {path}")
        return path

    def upload_to_s3(self, file_path: str, s3_key: str = None):
        s3_key = s3_key or file_path
        s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
        s3.upload_file(file_path, S3_BUCKET_NAME, s3_key)
        print(f"Uploaded {file_path} to s3://{S3_BUCKET_NAME}/{s3_key}")

    def run(self):
        df_new = self.extract()
        if df_new.empty:
            print("No new records to process")
            return
        
        df_transformed = self.transform()
        file_path = self.load_to_csv(df_transformed)
        self.upload_to_s3(file_path)
        self.update_last_run_time()
        print("ETL batch completed")

    def close(self):
        self.engine.dispose()
