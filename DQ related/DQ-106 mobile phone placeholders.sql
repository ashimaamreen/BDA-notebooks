select count(*),s_user.login 
 
 
from gms.s_contact as con  
inner join gms.s_user on con.created_by = s_user.row_id 
where 
year(con.x_nrma_join_dt)=2020 
and 
    regexp_replace(con.cell_ph_num, "[^0-9]+", "") = '0000000000' 
--    or regexp_replace(con.cell_ph_num, "[0-9]+", "") = '0400000000' 
--    or  
-- regexp_replace(con.cell_ph_num, "[^0-9]+", "") = '0411111111' 
group by s_user.login 
--year(con.x_nrma_join_dt) desc 