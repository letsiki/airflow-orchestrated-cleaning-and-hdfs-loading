-- We use inner join as we want to exclude unmatching sub_id's
SELECT 
    cdt.timestamp,
    cs.row_key as sub_row_key,
    cs.sub_id,
    cdt.amount,
    cdt.channel,
    date_format(cs.act_dt, 'yyyyMMdd') as activation_date
FROM cleansed_data_transactions cdt
JOIN cleansed_subscribers cs on cs.sub_id = cdt.subscriber_id