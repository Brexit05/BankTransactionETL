CREATE TABLE IF NOT EXISTS transactions (
     id SERIAL PRIMARY KEY,
    account VARCHAR(100) NOT NULL,
    account_id VARCHAR(20) NOT NULL,
    action VARCHAR(20) NOT NULL,
    amount INT ,
    location VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    fraud BOOLEAN DEFAULT FALSE NOT NULL
);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    id SERIAL PRIMARY KEY,
    account VARCHAR(100) NOT NULL,
    account_id VARCHAR(20) NOT NULL,
    action VARCHAR(20) NOT NULL,
    amount INT ,
    location VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    fraud BOOLEAN DEFAULT FALSE NOT NULL
);



CREATE INDEX IF NOT EXISTS trans_action_index ON transactions(action);
CREATE INDEX IF NOT EXISTS trans_fraud_index ON transactions(fraud);
CREATE INDEX IF NOT EXISTS trans_timestamp_index ON transactions(timestamp);

CREATE INDEX IF NOT EXISTS fraud_action_index ON fraud_alerts(action);
CREATE INDEX IF NOT EXISTS fraud_timestamp_index ON fraud_alerts(timestamp);



