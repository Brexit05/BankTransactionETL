from kafka import KafkaConsumer
import json
from sqlalchemy import create_engine, text
from config import PG_URL
import logging
import time

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[logging.FileHandler('consumer.log'), logging.StreamHandler()])
logger = logging.getLogger(__name__)


class BankCollect:
    """Collects data from Kafka and stores in Postgres."""
    def __init__(self, bootstrap_servers: list[str] = ['kafka:9092'],
                 topic: list[str] = ['bank_records'], url=PG_URL):
        self.topic = topic
        self.url = url
        self.consumer = KafkaConsumer(*self.topic,
                                      bootstrap_servers=bootstrap_servers,
                                      value_deserializer=lambda c: json.loads(c.decode('utf-8')),
                                      key_deserializer=lambda c: c.decode('utf-8'))
        logger.info("Establishing connection to postgres.")
        self.engine = create_engine(self.url)
        logger.info("Connection established.")

    def data_insert(self, records: list[dict]):
        """Batch insert into transactions table."""
        if not records:
            return
        try:
            for r in records:
                r['fraud'] = False
            query = text("""
                INSERT INTO transactions (account, account_id, action, amount, location, timestamp, fraud)
                VALUES (:account, :account_id, :action, :amount, :location, :timestamp, :fraud);
            """)
            with self.engine.begin() as conn:
                conn.execute(query, records)
            logger.info(f"Inserted {len(records)} transactions.")
        except Exception as e:
            logger.error(f"Error inserting transactions: {e}")

    def insert_fraud(self, records: list[dict]):
        """Batch insert into fraud_alerts table."""
        if not records:
            return
        try:
            for r in records:
                r['fraud'] = True
                logger.warning(f"Suspicious activity in account: {r['account']} at {r['timestamp']} ({r['location']})")

            query = text("""
                INSERT INTO fraud_alerts (account, account_id, action, amount, location, timestamp, fraud)
                VALUES (:account, :account_id, :action, :amount, :location, :timestamp, :fraud);
            """)
            with self.engine.begin() as conn:
                conn.execute(query, records)
            logger.info(f"Inserted {len(records)} fraud alerts.")
        except Exception as e:
            logger.error(f"Error inserting fraud alerts: {e}")

    def consume_data(self, interval: int = 30, batch_size: int = 50):
        """Consume Kafka messages in batches and insert into DB."""
        transactions_batch = []
        fraud_batch = []
        start_time = time.time()
        try:
            for message in self.consumer:
                data = message.value
                transactions_batch.append(data)
                amount = data.get("amount",0)
                if data.get('rapid_repeats', False) or (isinstance(amount, (int, float)) and amount >= 3000):
                    fraud_batch.append(data)

                if (len(transactions_batch) >= batch_size) or (time.time() - start_time >= interval):
                    self.data_insert(transactions_batch)
                    self.insert_fraud(fraud_batch)
                    transactions_batch.clear()
                    fraud_batch.clear()
                    start_time = time.time()
        except KeyboardInterrupt as e:
            logger.error("Keyboard interruption.")
        except Exception as e:
            logger.error(f"Unknwon error occured in the consume_data function: {e}")

    def close(self):
        self.consumer.close()


if __name__ == "__main__":
    collector = BankCollect()
    try:
        collector.consume_data()
    except Exception as e:
        logger.error(f"Unknown error in running the script: {e}")
    finally:
        collector.close()
