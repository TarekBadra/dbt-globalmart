SELECT
    customerid,
    segment,
    country,
    sum(orderprofit) as profit
FROM 
    {{ ref('stg_orders') }}
GROUP BY
    customerid,
    segment,
    country    
