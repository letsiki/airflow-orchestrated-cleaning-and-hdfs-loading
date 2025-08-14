-- Create subscribers table
CREATE TABLE IF NOT EXISTS subscribers (
   row_key VARCHAR(128),
   sub_id VARCHAR(7) PRIMARY KEY,
   act_dt DATE
);

-- Create unmatched_transactions table  
CREATE TABLE IF NOT EXISTS unmatched_transactions (
   timestamp TIMESTAMPTZ,
   subscriber_id VARCHAR(7),
   amount DECIMAL(10,4),
   channel VARCHAR(20)
);