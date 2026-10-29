
select
     i_item_id
    ,i_item_desc
    ,s_store_id
    ,s_store_name
    ,sum(ss_quantity)        as store_sales_quantity
    ,sum(sr_return_quantity) as store_returns_quantity
    ,sum(cs_quantity)        as catalog_sales_quantity
 from
    store_sales
   ,store_returns
   ,catalog_sales
   ,store
   ,item
 where
 ss_sold_date_sk in (select d_date_sk from date_dim   -- CHANGE 1: store_sales filtered to April 1999 before the joins (replaces d1)
                     where d_moy = 4 and d_year = 1999)
 and i_item_sk              = ss_item_sk
 and s_store_sk             = ss_store_sk
 and ss_customer_sk         = sr_customer_sk
 and ss_item_sk             = sr_item_sk
 and ss_ticket_number       = sr_ticket_number
 and sr_returned_date_sk in (select d_date_sk from date_dim    -- CHANGE 2: store_returns filtered to April to July 1999 before the joins (replaces d2)
                             where d_moy between 4 and 7 and d_year = 1999)
 and sr_customer_sk         = cs_bill_customer_sk
 and sr_item_sk             = cs_item_sk
 and cs_sold_date_sk in (select d_date_sk from date_dim     -- CHANGE 3: catalog_sales filtered to 1999-2001 before the joins (replaces d3)
                         where d_year in (1999, 2000, 2001))
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