
select  rpad(i_category, 50, ' ') as i_category         -- CHANGE 3: Final RPAD
       ,rpad(i_class, 50, ' ') as i_class
       ,rpad(i_brand, 50, ' ') as i_brand
       ,rpad(i_product_name, 50, ' ') as i_product_name
       ,d_year
       ,d_qoy
       ,d_moy
       ,rpad(s_store_id, 16, ' ') as s_store_id
       ,sumsales
       ,rk
from (select i_category
            ,i_class
            ,i_brand
            ,i_product_name
            ,d_year
            ,d_qoy
            ,d_moy
            ,s_store_id
            ,sumsales
            ,rank() over (partition by i_category order by sumsales desc) rk
      from (select i_category
                  ,i_class
                  ,i_brand
                  ,i_product_name
                  ,d_year
                  ,d_qoy
                  ,d_moy
                  ,s_store_id
                  ,sum(sales_value) sumsales            -- CHANGE 2: Summing the pre-computed value
            from ( 
                  -- CHANGE 1: Injected subquery for TRIM and Math Pushdown
                  select trim(i_category) as i_category
                        ,trim(i_class) as i_class
                        ,trim(i_brand) as i_brand
                        ,trim(i_product_name) as i_product_name
                        ,d_year
                        ,d_qoy
                        ,d_moy
                        ,trim(s_store_id) as s_store_id
                        ,coalesce(ss_sales_price*ss_quantity,0) as sales_value
                  from store_sales
                      ,date_dim
                      ,store
                      ,item
                  where  ss_sold_date_sk=d_date_sk
                     and ss_item_sk=i_item_sk
                     and ss_store_sk = s_store_sk
                     and d_month_seq between 1212 and 1212+11
            ) prep
       group by  rollup(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,s_store_id))dw1) dw2
where rk <= 100
order by i_category
        ,i_class
        ,i_brand
        ,i_product_name
        ,d_year
        ,d_qoy
        ,d_moy
        ,s_store_id
        ,sumsales
        ,rk
limit 100;