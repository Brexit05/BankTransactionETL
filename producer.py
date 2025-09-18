import time
import random
from datetime import datetime
import faker
from kafka import KafkaProducer
import json
import logging


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers = [logging.StreamHandler(), logging.FileHandler('bank_record.log')])
logger = logging.getLogger(__name__)

fake = faker.Faker()

class BankRecord:
    """Generates fake bank transaction records and sends them to a Kafka topic."""
    def __init__(self, bootstrap_servers: list[str]=['kafka:9092'], topic:str='bank_records'):
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                      value_serializer=lambda b: json.dumps(b).encode('utf-8'),
                                      key_serializer=lambda b: b.encode('utf-8'),
                                      retries=5,
                                      retry_backoff_ms=1000)
        self.names= [fake.name() for _ in range(300)]


    def get_amount(self, action):
        """Simulates realistic transaction amounts based on the action type."""
        if action == 'deposit':
            return max(1000, int(random.gauss(50000,15000)))
        elif action == 'withdraw':
            return max(1000, int(random.gauss(20000,5000)))
        elif action == 'transfer':
            return max(100, int(random.gauss(20000,7000)))
        else:
            return None
    def generate_record(self):
        """Generates a single fake bank transaction record."""
        alphabets = [
        "A","B","C","D","E","F","G","H","I","J","K","L","M",
        "N","O","P","Q","R","S","T","U","V","W","X","Y","Z"
            ]
        states = [
        "Abia", "Adamawa", "Akwa Ibom", "Anambra", "Bauchi", "Bayelsa", "Benue", "Borno",
        "Cross River", "Delta", "Ebonyi", "Edo", "Ekiti", "Enugu", "Gombe", "Imo",
        "Jigawa", "Kaduna", "Kano", "Katsina", "Kebbi", "Kogi", "Kwara", "Lagos",
        "Nasarawa", "Niger", "Ogun", "Ondo", "Osun", "Oyo", "Plateau", "Rivers",
        "Sokoto", "Taraba", "Yobe", "Zamfara", "FCT"
            ]
        try:
            events = ['deposit', 'withdraw', 'transfer','check_balance']
            action = random.choices(events, weights=[0.1,0.5,0.1,0.3], k=1)[0]
            amount=self.get_amount(action)
            account = random.choice(self.names)
            account_id = f"{''.join(random.choice(alphabets) for _ in range(3))}{random.randint(1000000,99999999)}"
            location = random.choice(states)
            timestamp = datetime.now().isoformat()
            
            self.data = {'account': account,
                    'account_id': account_id,
                    'action': action,
                    'amount': amount,
                    'location': location,
                    'timestamp': timestamp
                    }
            if random.random() < 0.1 and action in ['withdraw','transfer']:
                fraud_type = random.choice(["large_amount", "rapid_repeats"])
                if fraud_type=='large_amount':
                     self.data['amount'] = random.randint(30000,100000)
                elif fraud_type=='rapid_repeats':
                    self.data['rapid_repeats'] = True

            return self.data 
        except Exception as e:
            logger.error(f"Error generating record: {e}")
            return None
    def send_to_kafka(self, record, interval: float = 0.5):
        """Sends the generated record to the specified Kafka topic."""
        try:
            key = record['account_id']
            self.producer.send(topic=self.topic,value=record,key=key)
            time.sleep(interval)
        except Exception as e:
            logger.error(f"Something happened in your send_to_kafka fucntion: {e}")

if __name__ == "__main__":
    bank = BankRecord()
    try:
        while True:
            logger.info("Generating and sending a new bank record...")
            record = bank.generate_record()
            bank.send_to_kafka(record)
            logger.info(f"Record sent: {record}")
    except KeyboardInterrupt:
        logger.info("Producer stopped manually (Ctrl+C).")
    except Exception as e:
            logger.error(f"Error occurred: {e}")
    finally:
            bank.producer.flush()
            bank.producer.close()
            logger.info("Done and dusted.")



