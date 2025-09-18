from dotenv import load_dotenv
import os 

load_dotenv()

POSTGRES_USER=os.getenv('POSTGRES_USER')
POSTGRES_DB=os.getenv('POSTGRES_DB')
POSTGRES_PASSWORD=os.getenv('POSTGRES_PASSWORD')

PG_URL=f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@bank-db:5432/{POSTGRES_DB}"

AWS_ACCESS_KEY=os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY=os.getenv('AWS_SECRET_KEY')
S3_BUCKET_NAME=os.getenv('S3_BUCKET_NAME')