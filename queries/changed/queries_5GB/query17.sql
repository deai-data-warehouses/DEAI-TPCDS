
select  i_item_id
       ,i_item_desc
       ,s_state
       ,count(ss_quantity) as store_sales_quantitycount
       ,avg(ss_quantity) as store_sales_quantityave
       ,stddev_samp(ss_quantity) as store_sales_quantitystdev
       ,stddev_samp(ss_quantity)/avg(ss_quantity) as store_sales_quantitycov
       ,count(sr_return_quantity) as store_returns_quantitycount
       ,avg(sr_return_quantity) as store_returns_quantityave
       ,stddev_samp(sr_return_quantity) as store_returns_quantitystdev
       ,stddev_samp(sr_return_quantity)/avg(sr_return_quantity) as store_returns_quantitycov
       ,count(cs_quantity) as catalog_sales_quantitycount
       ,avg(cs_quantity) as catalog_sales_quantityave
       ,stddev_samp(cs_quantity) as catalog_sales_quantitystdev
       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitycov
 from store_sales
     ,store_returns
     ,catalog_sales
     ,store
     ,item
 where
   -- CHANGE 1: store_sales filtered to 1998Q1 before the joins (replaces d1)
   ss_sold_date_sk in (select d_date_sk from date_dim
                       where d_quarter_name = '1998Q1')
   and i_item_sk = ss_item_sk
   and s_store_sk = ss_store_sk
   and ss_customer_sk = sr_customer_sk
   and ss_item_sk = sr_item_sk
   and ss_ticket_number = sr_ticket_number
   -- CHANGE 2: store_returns filtered to 1998Q1-Q3 before the joins (replaces d2)
   and sr_returned_date_sk in (select d_date_sk from date_dim
                               where d_quarter_name in ('1998Q1','1998Q2','1998Q3'))
   and sr_customer_sk = cs_bill_customer_sk
   and sr_item_sk = cs_item_sk
   -- CHANGE 3: catalog_sales filtered to 1998Q1-Q3 before the joins (replaces d3)
   and cs_sold_date_sk in (select d_date_sk from date_dim
                           where d_quarter_name in ('1998Q1','1998Q2','1998Q3'))
 group by i_item_id
         ,i_item_desc
         ,s_state
 order by i_item_id
         ,i_item_desc
         ,s_state
limit 100;