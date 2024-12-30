--  36 in stream 0 using template query36.tpl
-- start query 37 in stream 0 using template query37.tpl
select  i_item_id
       ,i_item_desc
       ,i_current_price
 from item, inventory, date_dim, catalog_sales
 where i_current_price between 39 and 39 + 30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
AND d_date BETWEEN CAST('1998-07-16' AS DATE)
    AND (CAST('1998-07-16' AS DATE) + INTERVAL '60 days')
 and i_manufact_id in (839,875,898,819)
 and inv_quantity_on_hand between 100 and 500
 and cs_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
 limit 100;

-- 