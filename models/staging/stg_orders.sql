SELECT
    --from raw orders
    o.orderid,
    o.orderdate,
    o.shipdate,
    o.shipmode,
    o.ordersellingprice - o.ordercost as orderprofit,
    o.ordercost,
    o.ordersellingprice,
    --from raw customer
    c.customerid,
    c.customername,
    c.segment,
    c.country,
    --from raw product
    p.category,
    p.productid,
    p.productname,
    p.subcategory,
    {{ markup('ordersellingprice', 'ordercost') }} as markup
FROM 
    {{ ref('raw_orders') }} as o
LEFT JOIN 
    {{ ref('raw_customer') }} as c
ON  o.customerid = c.customerid
LEFT JOIN
    {{ ref('raw_product') }} as p
ON  o.productid = p.productid

{{limit_data_in_dev('orderdate')}}
