select  ss_customer_sk
            ,sum(act_sales) sumsales
      from (select ss_item_sk
                  ,ss_ticket_number
                  ,ss_customer_sk
                  ,case when sr_return_quantity is not null then (ss_quantity-sr_return_quantity)*ss_sales_price
                                                            else (ss_quantity*ss_sales_price) end act_sales
            from store_sales
                join (select sr.sr_item_sk, sr.sr_ticket_number, sr.sr_return_quantity
                      from store_returns sr
                          join reason r on sr.sr_reason_sk = r.r_reason_sk    -- CHANGE: join reason here, so only matching returns reach the shuffle with store_sales
                      where r.r_reason_desc = 'Did not like the warranty') sr
                on (sr_item_sk = ss_item_sk
                    and sr_ticket_number = ss_ticket_number)) t
      group by ss_customer_sk
      order by sumsales, ss_customer_sk
limit 100;