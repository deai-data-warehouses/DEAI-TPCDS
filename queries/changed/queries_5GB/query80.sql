with date_window as (
       select d_date_sk
       from date_dim
       where d_date between cast('1998-08-04' as date)
                  and (cast('1998-08-04' as date) +  30 days)
       ),

items_more_than_50 as (
       select i_item_sk
       from item
       where i_current_price > 50
),

ss_more_than_50_in_window as (
       select *
       from store_sales
              join date_window on ss_sold_date_sk = d_date_sk
              join items_more_than_50 on ss_item_sk = i_item_sk
),

ssr as
 (select  s_store_id as store_id,
          sum(ss_ext_sales_price) as sales,
          sum(coalesce(sr_return_amt, 0)) as returns,
          sum(ss_net_profit - coalesce(sr_net_loss, 0)) as profit
  from ss_more_than_50_in_window left outer join store_returns on
         (ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number),
     -- store la necesito para el store_id
     store,
     -- promotion la necesito para el filtro de "not promoted on television"
     promotion
 where ss_store_sk = s_store_sk
       and ss_promo_sk = p_promo_sk
       -- not promoted on television
       and p_channel_tv = 'N'
 group by s_store_id) ,

cs_more_than_50_in_window as (
       select *
       from catalog_sales
              join date_window on cs_sold_date_sk = d_date_sk
              join items_more_than_50 on cs_item_sk = i_item_sk
),

 csr as
 (select  cp_catalog_page_id as catalog_page_id,
          sum(cs_ext_sales_price) as sales,
          sum(coalesce(cr_return_amount, 0)) as returns,
          sum(cs_net_profit - coalesce(cr_net_loss, 0)) as profit
  from cs_more_than_50_in_window left outer join catalog_returns on
         (cs_item_sk = cr_item_sk and cs_order_number = cr_order_number),
     catalog_page,
     promotion
 where cs_catalog_page_sk = cp_catalog_page_sk
       and cs_promo_sk = p_promo_sk
       and p_channel_tv = 'N'
group by cp_catalog_page_id),

ws_more_than_50_in_window as (
       select *
       from web_sales
              join date_window on ws_sold_date_sk = d_date_sk
              join items_more_than_50 on ws_item_sk = i_item_sk
),

 wsr as
 (select  web_site_id,
          sum(ws_ext_sales_price) as sales,
          sum(coalesce(wr_return_amt, 0)) as returns,
          sum(ws_net_profit - coalesce(wr_net_loss, 0)) as profit
  from ws_more_than_50_in_window left outer join web_returns on
         (ws_item_sk = wr_item_sk and ws_order_number = wr_order_number),
     web_site,
     promotion
 where ws_web_site_sk = web_site_sk
       and ws_promo_sk = p_promo_sk
       and p_channel_tv = 'N'
group by web_site_id)

  select  channel
        , id
        , sum(sales) as sales
        , sum(returns) as returns
        , sum(profit) as profit
 from
 (select 'store channel' as channel
        , 'store' || store_id as id
        , sales
        , returns
        , profit
 from   ssr
 union all
 select 'catalog channel' as channel
        , 'catalog_page' || catalog_page_id as id
        , sales
        , returns
        , profit
 from  csr
 union all
 select 'web channel' as channel
        , 'web_site' || web_site_id as id
        , sales
        , returns
        , profit
 from   wsr
 ) x
 group by rollup (channel, id)
 order by channel
         ,id

 limit 100;