with main as (
select distinct customer_id, trx_header_id

from `mynrma-mm-mk-dw-sandbox-5984.omc.send_level_summary` s

-- inner join  
--     `mynrma-mm-mk-dw-sandbox-5984.omc.campaign_info` i
--     on i.campaign_id=s.campaign_id

inner join 
    gms.s_contact AS con
    on con.row_id=s.customer_id


INNER JOIN
    gms.s_addr_per as address
    ON address.row_id = con.pr_per_addr_id

inner join                                                          -- purchase battery during promotion period
    `mynrma-mm-da-dw-sandbox-7f27.ams.return_feed_header` battery      
    on battery.MEMBER_NUMBER=con.csn
    and partner='NRMA Batteries'
    and date(time_stamp) between '2021-11-01' and '2021-12-10'

where s.campaign_id in ('63547562','63554262','63547642')             -- receive mkt communication related to promotion
and address.state in ('ACT','NSW')
AND DATE_DIFF(CURRENT_DATE('Australia/Sydney'), DATE(con.birth_dt), YEAR) - IF(
        EXTRACT(MONTH FROM DATE(con.birth_dt))*100 + EXTRACT(DAY FROM DATE(con.birth_dt)) >
        EXTRACT(MONTH FROM CURRENT_DATE('Australia/Sydney'))*100 + EXTRACT(DAY FROM CURRENT_DATE('Australia/Sydney')),
        1,
        0
    ) >= 18                                                         -- age 18 check
AND COALESCE(con.x_inv_email_1, 'N') = 'N'                          -- email validity check
AND con.email_addr IS NOT NULL
)
select *, rand() as rnd from main
