-- isolate transactions with a registered subscriber
SELECT
    timestamp,
    subscriber_id,
    amount,
    channel
FROM
    cleansed_data_transactions
WHERE subscriber_id NOT IN (
    SELECT DISTINCT sub_id
    FROM cleansed_subscribers
    )