select
 i_item_id
 ,i_item_desc
 ,s_store_id
 ,s_store_name
 ,sum(ss_net_profit) as store_sales_profit
 ,sum(sr_net_loss) as store_returns_loss
 ,sum(cs_net_profit) as catalog_sales_profit
 from
 store_sales
 ,store_returns
 ,catalog_sales
 ,store
 ,item
 where
 -- CHANGE 1: store_sales filtered by date before the joins (replaces d1)
 ss_sold_date_sk in (select d_date_sk from date_dim
                     where d_moy = 4 and d_year = 2000)
 and i_item_sk = ss_item_sk
 and s_store_sk = ss_store_sk
 and ss_customer_sk = sr_customer_sk
 and ss_item_sk = sr_item_sk
 and ss_ticket_number = sr_ticket_number
 -- CHANGE 2: store_returns filtered by date before the joins (replaces d2)
 and sr_returned_date_sk in (select d_date_sk from date_dim
                             where d_moy between 4 and 10 and d_year = 2000)
 and sr_customer_sk = cs_bill_customer_sk
 and sr_item_sk = cs_item_sk
 -- CHANGE 3: catalog_sales filtered by date before the joins (replaces d3)
 and cs_sold_date_sk in (select d_date_sk from date_dim
                         where d_moy between 4 and 10 and d_year = 2000)
 group by
 i_item_id
 ,i_item_desc
 ,s_store_id
 ,s_store_name
 order by
 i_item_id
 ,i_item_desc
 ,s_store_id
 ,s_store_name
 limit 100;