SELECT prod.name, 
       asset.owner_con_id 
FROM gms.s_asset as asset 
INNER JOIN gms.s_prod_int AS prod ON prod.row_id = asset.prod_id 
WHERE prod.name IN('Autoclub Classic' 
                  ,'Autoclub Premium' 
                  ,'Autoclub Plus' 
                  ,'Classic Care' 
                  ,'Free2Go' 
                  ,'Premium Care' 
                  ,'NRMA Blue' 
                  ,'MVB Premium Care' 
                  ,'MVB Classic Care' 
                    ) 
  AND asset.status_cd = 'Active' 
  AND promotion_id IS NULL 
--GROUP BY prod.name 
