spark.read.load('qbr_handback.csv', format = 'csv', header = 'true').createOrReplaceTempView('handback') 
spark.sql(''' 
SELECT 
    `ABN` AS abn, 
    DATE_TRUNC('MONTH', TO_TIMESTAMP(`TRANSACTION START DATE`, 'yyyyMMdd')) AS dt 
 
FROM 
    handback 
''').createOrReplaceTempView('accepted') 
abns = ['11149226568', 
'11161012344', 
'11424260599', 
'11460560421', 
'11738215952', 
'12082174703', 
'12125971851', 
'12133471619', 
'12291798458', 
'12606901624', 
'12624077738', 
'13091164519', 
'14114526975', 
'14117136795', 
'14142819412', 
'14162416104', 
'14263296295', 
'14633936204', 
'14636465088', 
'14803680036', 
'15166183062', 
'15354744689', 
'15636354053', 
'15639153861', 
'16160464158', 
'16168511886', 
'16475009617', 
'16615072538', 
'17062546427', 
'17094004770', 
'17121737102', 
'17163892466', 
'17166765984', 
'17631524895', 
'17635448109', 
'18164419197', 
'19061429883', 
'19629977411', 
'20003833574', 
'20084183102', 
'20603564012', 
'20669679036', 
'21144520427', 
'21156112337', 
'21232129816', 
'21629789500', 
'21833979985', 
'22138352799', 
'22167057963', 
'22169901884', 
'22842918939', 
'23109285074', 
'23544861219', 
'23632169245', 
'24158886714', 
'25165130021', 
'25165423316', 
'25617284705', 
'25619176055', 
'26090094189', 
'26133722782', 
'26166768449', 
'26611227939', 
'27061020399', 
'27150920264', 
'27614271055', 
'27616856672', 
'28630912515', 
'28910976306', 
'28988744595', 
'29113260932', 
'29138778033', 
'29151245957', 
'29161395268', 
'29629276848', 
'29637316833', 
'30132319496', 
'30154744304', 
'30416509331', 
'30624108961', 
'30631127163', 
'30654870850', 
'31061914690', 
'31131667019', 
'31148454502', 
'31364785581', 
'31622350887', 
'32024176567', 
'32108957955', 
'32145876071', 
'32162288280', 
'32389812421', 
'32624202919', 
'32631237008', 
'33056326914', 
'33060820751', 
'33112884921', 
'33118591209', 
'33167105722', 
'33600208213', 
'33614616590', 
'33624037252', 
'34086078640', 
'34113829655', 
'34164320941', 
'34187090581', 
'34605662860', 
'34606059376', 
'35087660580', 
'35605230233', 
'35620214039', 
'35688205396', 
'36063459825', 
'36474863108', 
'36617018621', 
'37616018707', 
'37625076575', 
'37928468313', 
'37997007354', 
'38099046321', 
'38124729406', 
'38193499605', 
'38607438867', 
'38626302029', 
'39001065425', 
'39618212196', 
'40609872031', 
'40614707336', 
'40624266308', 
'40759808689', 
'41247530091', 
'41624337873', 
'41637085600', 
'41669511757', 
'41884814628', 
'42608684644', 
'42632422407', 
'42638499199', 
'42692771420', 
'42909129957', 
'43092742982', 
'43618227615', 
'43634079315', 
'43637664834', 
'44072457217', 
'44131668767', 
'44612396488', 
'45133093044', 
'45176900825', 
'45342162916', 
'45516951990', 
'45626367762', 
'45630264861', 
'45921459898', 
'46110280192', 
'46141162263', 
'46144573259', 
'46225547032', 
'46297381461', 
'46359882837', 
'46456379520', 
'46624825958', 
'46628375762', 
'46637537672', 
'46903704040', 
'47002521404', 
'47110995518', 
'47135615468', 
'47164442942', 
'47302753590', 
'47582272057', 
'48073323389', 
'48153548004', 
'48634369387', 
'49166529522', 
'49607257812', 
'49615380044', 
'49627482713', 
'49634547149', 
'49638143549', 
'49939853770', 
'50005863439', 
'50078326191', 
'50165781902', 
'50606472548', 
'50613076403', 
'50637876718', 
'50669951838', 
'50772944785', 
'51087347171', 
'51135908717', 
'51147045332', 
'51162236784', 
'52882307841', 
'53140803907', 
'53142739893', 
'53577022604', 
'53601597166', 
'53627605467', 
'53635205686', 
'54005018799', 
'54103573066', 
'54146286008', 
'54608584247', 
'54615594360', 
'55101916016', 
'55620216186', 
'55621114476', 
'55852475063', 
'56112436905', 
'56169146221', 
'56609031290', 
'56637177256', 
'57142886084', 
'57631741270', 
'57638040203', 
'58143640555', 
'58627099245', 
'58635091440', 
'59160028026', 
'59430613429', 
'59632565934', 
'60609536898', 
'61108458224', 
'61145624200', 
'61160143055', 
'61354640806', 
'61632942593', 
'61785328644', 
'61903086168', 
'62061168803', 
'62125210184', 
'62605198472', 
'62613553110', 
'62626834393', 
'62634695891', 
'63612767932', 
'64244633855', 
'64292019390', 
'64626905682', 
'65106192661', 
'65124189883', 
'65600636922', 
'65607211692', 
'65611739667', 
'65632947034', 
'65667362163', 
'66002843129', 
'66158483053', 
'66166008368', 
'66538806397', 
'66620894297', 
'66740252712', 
'67116633133', 
'67122449807', 
'67136189038', 
'67167330298', 
'67606809590', 
'68603082757', 
'68632596760', 
'68946076879', 
'69006778150', 
'69153678141', 
'69154177090', 
'69167940696', 
'69350025316', 
'69620186290', 
'69914578538', 
'70003862511', 
'70107167108', 
'70574836620', 
'70617478556', 
'71007108973', 
'71117505585', 
'71160319755', 
'71629547502', 
'72638324277', 
'73161596783', 
'73261496837', 
'73605956250', 
'73624762894', 
'73671929416', 
'73814174353', 
'74498462175', 
'74602717037', 
'74624853112', 
'74633066501', 
'74915561976', 
'75129188692', 
'75359453365', 
'75414482475', 
'75605618866', 
'75606241794', 
'76109725126', 
'76127011250', 
'76299303137', 
'76602592754', 
'77611702375', 
'77623664040', 
'77638532411', 
'78143611474', 
'78157723316', 
'78157934364', 
'78609542817', 
'78611869768', 
'79502567531', 
'79608699449', 
'79644504620', 
'80134361283', 
'80135108977', 
'80158497690', 
'80625191399', 
'80626877852', 
'81137869937', 
'81634702932', 
'81815895045', 
'82169402756', 
'82170530433', 
'82604476404', 
'82613671588', 
'83101570487', 
'83634954359', 
'84115240803', 
'84131246403', 
'84155146786', 
'84168905404', 
'84331942457', 
'84381199664', 
'84609803858', 
'85128982123', 
'85161457058', 
'85311365399', 
'85396990759', 
'85520403704', 
'85610384168', 
'85622420704', 
'85629361691', 
'85634237059', 
'85752970893', 
'86101717926', 
'86491964317', 
'86633188940', 
'87159348222', 
'87258442925', 
'87635049946', 
'88001658699', 
'88003063974', 
'88031656591', 
'88142463312', 
'89003473407', 
'89144667407', 
'89190132199', 
'90013525792', 
'90112563407', 
'90622920129', 
'90679872239', 
'91041537708', 
'91514872581', 
'91607348774', 
'92101034600', 
'92147786374', 
'92399228448', 
'93067281645', 
'93088794905', 
'93090906915', 
'93100737415', 
'93166364021', 
'93601512636', 
'93608483156', 
'93611988026', 
'94080617036', 
'94250419587', 
'94624152594', 
'94630539007', 
'94684162163', 
'95101135933', 
'96128369871', 
'96165889903', 
'96611121610', 
'96617438972', 
'97080536492', 
'97097893653', 
'97130933434', 
'97165799182', 
'97171284356', 
'98164326952', 
'98398094637', 
'98610648729', 
'98965370329', 
'99098093273', 
'99110688367', 
'99130035504', 
'99615251020', 
'99709346020', 
'24151258463', 
'46001903439', 
'77421705616', 
'18828658413', 
'33688603918', 
'54779791755', 
'15610780786', 
'25211412167', 
'30981657536', 
'29936726713', 
'50473964346', 
'85455229581', 
'30002865232', 
'27630257279', 
'39003976289', 
'43634366055', 
'21105772969', 
'22002463376', 
'70613322722', 
'81105891861', 
'67619124382', 
'35615115985', 
'49610654941', 
'60612916953', 
'33114618585', 
'49613819686', 
'26292661279', 
'84160229736', 
'17204880324', 
'93151466554', 
'17130727356', 
'21080648692', 
'39518400562', 
'28000291458', 
'23629768001', 
'93095398824', 
'31191331841', 
'53030174938', 
'59872796334', 
'87155635608', 
'55848680640', 
'25307877599', 
'81079175269', 
'36112391018', 
'97208823158', 
'45176900825', 
'16003084295', 
'86169800015', 
'65152967218', 
'66348265324', 
'28085324929', 
'47100682139', 
'33118037531', 
'14003441496', 
'36119180460', 
'62088196543', 
'95154313201', 
'14069839756', 
'24360153575', 
'15168535313', 
'28055679070', 
'70137201471', 
'95054883146', 
'77537384373', 
'73157676394', 
'82940734258', 
'76050054898', 
'78061449653', 
'39795814055', 
'45161159619', 
'13147631687', 
'98524576326', 
'70092314586', 
'31139653268', 
'22060362236', 
'74634458156', 
'94618862954', 
'68253137615', 
'19152498130', 
'22619256674', 
'36165081167', 
'17052813840', 
'12089146896', 
'27129189233', 
'25792162631', 
'42003324598', 
'21470450792', 
'77158897287', 
'89664212116', 
'59034628691', 
'62311037419', 
'80038199497', 
'83736369902', 
'35168356009', 
'14001034911', 
'17141123631', 
'27102747571', 
'31167856680', 
'31602529377', 
'33133743058', 
'35152921821', 
'35725782786', 
'47145122363', 
'51145228566', 
'52063050377', 
'62480316193', 
'63610665971', 
'71298381351', 
'73003060160', 
'77099716873', 
'90081546536', 
'93091997634', 
'96167579179', 
'97099083084'] 
 
spark.createDataFrame([[a] for a in abns], schema = ['abn']).createOrReplaceTempView('abns') 
 
 
# spark.sql(''' 
# SELECT 
#     acc.vat_regn_num, 
#     DATE_TRUNC('DAY', MIN(acc.x_nrma_join_dt)) AS join_month, 
     
# FROM 
#     gms.s_org_ext AS acc 
     
# LEFT JOIN 
#     gms.s_org_ext_x AS accx 
#     ON accx.par_row_id = acc.row_id 
     
# LEFT JOIN 
#     gms.s_asset AS asset 
#     ON asset.owner_accnt_id = acc.row_id 
     
# LEFT JOIN 
#     gms.s_order AS ord 
#     ON ord.accnt_id = acc.row_id 
     
# WHERE 
#     acc.ou_type_cd LIKE "%Organisation" 
#     AND COALESCE(acc.market_class_cd, '') NOT LIKE "50+%" 
#     AND COALESCE(acc.market_class_cd, '') NOT LIKE "ADM%" 
#     -- AND accx.attrib_25 = 0 
 
# GROUP BY 
#     acc.vat_regn_num 
# ''').createOrReplaceTempView('temp') 
 
spark.sql(''' 
SELECT 
    acc.vat_regn_num AS abn, 
    MAX(ord.order_dt) AS last_order, 
    MAX(acc.x_nrma_join_dt) AS join_dt, 
    DATEDIFF(MAX(acc.x_nrma_join_dt), MAX(ord.order_dt)) 
     
FROM 
    gms.s_org_ext AS acc 
     
LEFT JOIN 
    gms.s_order AS ord 
    ON ord.accnt_id = acc.row_id 
 
LEFT JOIN 
    gms.s_order_type AS ordt 
    ON ordt.row_id = ord.order_type_id 
 
WHERE 
    ( 
        (ordt.name = 'New' AND ord.status_cd NOT IN ('Cancelled', 'Revised')) 
    OR 
        (ordt.name = 'Renew') 
    OR 
        ord.row_id IS NULL 
    ) 
    AND acc.x_nrma_join_dt >= DATE('2018-01-01') 
    AND (ord.order_dt IS NULL OR ord.order_dt < acc.x_nrma_join_dt) 
    AND COALESCE(acc.market_class_cd, '') NOT LIKE 'ADM%' 
    AND COALESCE(acc.market_class_cd, '') NOT LIKE '50+%' 
    AND acc.x_nrma_asset_count < 50 
 
GROUP BY 
    acc.vat_regn_num 
     
HAVING 
    DATEDIFF(MAX(acc.x_nrma_join_dt), MAX(ord.order_dt)) >= 365*3 
    OR DATEDIFF(MAX(acc.x_nrma_join_dt), MAX(ord.order_dt)) < 2 
    OR MAX(ord.order_dt) IS NULL 
''').createOrReplaceTempView('joins') 
 
# spark.sql(''' 
# SELECT * FROM 
# foobar LEFT ANTI JOIN abns ON foobar.abn = abns.abn 
# ''').show(20, False) 
 
# Two of the ones sent to Qantas have cancelled since, the extras are all anomalies and should be fine 
# spark.sql(''' 
# SELECT 
#     DATE_TRUNC('MONTH', foobar.join_dt), 
#     COUNT(DISTINCT foobar.abn), 
#     COUNT(DISTINCT accepted.abn), 
#     COUNT(*) 
     
# FROM 
#     foobar 
 
# LEFT OUTER JOIN 
#     accepted 
#     ON accepted.abn = foobar.abn 
#     AND accepted.dt = DATE_TRUNC('MONTH', foobar.join_dt) 
 
# INNER JOIN 
#     gms.s_org_ext AS acc 
#     ON acc.vat_regn_num = foobar.abn 
 
# INNER JOIN 
#     sandpit.renewal_base AS base 
#     ON base.account_id = acc.row_id 
#     AND DATE_TRUNC('MONTH', base.order_completed_dt) = DATE_TRUNC('MONTH', foobar.join_dt) 
#     AND base.prod_type = 'RSA' 
 
# WHERE 
#     foobar.join_dt >= DATE('2019-01-01') 
 
 
# ''').show(250, False) 
 
 
spark.sql(''' 
SELECT 
    DATE_TRUNC('MONTH', joins.join_dt) AS join_month, 
    joins.abn, 
    accepted.abn AS accepted_abn 
     
FROM 
    joins 
     
LEFT OUTER JOIN 
    accepted 
    ON accepted.abn = joins.abn 
    AND accepted.dt = DATE_TRUNC('MONTH', joins.join_dt) 
''').createOrReplaceTempView('conversions') 
 
 
spark.sql(''' 
SELECT 
    conv.join_month, 
    COUNT(DISTINCT conv.abn), 
    COUNT(DISTINCT conv.accepted_abn), 
    COUNT(*) 
     
FROM 
    conversions AS conv 
 
INNER JOIN 
    gms.s_org_ext AS acc 
    ON acc.vat_regn_num = conv.abn 
 
INNER JOIN 
    sandpit.renewal_base AS base 
    ON base.account_id = acc.row_id 
    AND DATE_TRUNC('MONTH', base.order_completed_dt) = DATE_TRUNC('MONTH', conv.join_month) 
    AND base.prod_type = 'RSA' 
 
WHERE 
    conv.join_month >= DATE('2018-01-01') 
 
GROUP BY 
    conv.join_month     
     
ORDER BY 
    conv.join_month 
''').show(250, False) 
 
 
 
spark.sql(''' 
SELECT 
    conv.abn, 
    conv.accepted_abn, 
    conv.join_month, 
    SUM(CASE WHEN base.prod_type = 'RSA' THEN 1 ELSE 0 END) AS assets, 
    SUM(item_net_price) AS price 
     
FROM 
    conversions AS conv 
 
INNER JOIN 
    gms.s_org_ext AS acc 
    ON acc.vat_regn_num = conv.abn 
 
INNER JOIN 
    sandpit.renewal_base AS base 
    ON base.account_id = acc.row_id 
    AND DATE_TRUNC('MONTH', base.order_completed_dt) = DATE_TRUNC('MONTH', conv.join_month) 
    AND (base.prod_type = 'RSA' OR prod_name = 'BusinessWise Annual Membership Fee') 
 
WHERE 
    conv.join_month >= DATE('2018-01-01') 
 
GROUP BY 
    conv.abn, 
    conv.accepted_abn, 
    conv.join_month 
 
ORDER BY 
    COUNT(*) DESC 
''').createOrReplaceTempView('asset') 
spark.sql(''' 
SELECT 
    join_month AS month, 
    COUNT(DISTINCT abn) AS joined, 
    COUNT(DISTINCT accepted_abn) AS converted, 
    SUM(assets) AS j_asset, 
    SUM(CASE WHEN accepted_abn IS NOT NULL THEN assets ELSE 0 END) AS c_asset, 
    SUM(price) AS j_price, 
    SUM(CASE WHEN accepted_abn IS NOT NULL THEN price ELSE 0 END) AS c_price 
     
FROM 
    asset 
 
WHERE 
    assets < 50 
 
GROUP BY 
    join_month 
     
ORDER BY 
    join_month ASC 
''').show(250, False) 
 
 
 
 
spark.sql(''' 
SELECT 
    conv.abn, 
    conv.accepted_abn, 
    conv.join_month, 
    SUM(CASE WHEN base.prod_type = 'RSA' THEN 1 ELSE 0 END) AS assets, 
    SUM(item_net_price) AS price, 
    prod_name 
     
FROM 
    conversions AS conv 
 
INNER JOIN 
    gms.s_org_ext AS acc 
    ON acc.vat_regn_num = conv.abn 
 
INNER JOIN 
    sandpit.renewal_base AS base 
    ON base.account_id = acc.row_id 
    AND DATE_TRUNC('MONTH', base.order_completed_dt) = DATE_TRUNC('MONTH', conv.join_month) 
    AND (base.prod_type = 'RSA' OR prod_name = 'BusinessWise Annual Membership Fee') 
 
WHERE 
    conv.join_month >= DATE('2018-01-01') 
 
GROUP BY 
    conv.abn, 
    conv.accepted_abn, 
    conv.join_month, 
    prod_name 
 
ORDER BY 
    COUNT(*) DESC 
''').createOrReplaceTempView('asset') 
spark.sql(''' 
 SELECT prod_name, order_type, prod_type FROM sandpit.renewal_base WHERE prod_name = 'BusinessWise Annual Membership Fee' 
''').show(250, False) 
 
spark.sql(''' 
SELECT 
    join_month AS month, 
    prod_name, 
    SUM(assets) AS j_asset 
     
FROM 
    asset 
 
WHERE 
    assets < 50 
 
GROUP BY 
    join_month, 
    prod_name 
     
ORDER BY 
    join_month ASC, 
    prod_name ASC 
''').show(250, False) 
 
spark.sql(''' 
SELECT 
    join_month AS month, 
    COUNT(DISTINCT abn) AS joined, 
    COUNT(DISTINCT accepted_abn) AS converted, 
    SUM(assets) AS j_asset, 
    SUM(CASE WHEN accepted_abn IS NOT NULL THEN assets ELSE 0 END) AS c_asset, 
    SUM(price) AS j_price, 
    SUM(CASE WHEN accepted_abn IS NOT NULL THEN price ELSE 0 END) AS c_price 
     
FROM 
    asset 
 
WHERE 
    assets < 50 
    AND abn IN ('24151258463', 
'46001903439', 
'77421705616', 
'18828658413', 
'33688603918', 
'54779791755', 
'15610780786', 
'25211412167', 
'30981657536', 
'29936726713', 
'50473964346', 
'85455229581', 
'30002865232', 
'27630257279', 
'39003976289', 
'43634366055', 
'21105772969', 
'22002463376', 
'70613322722', 
'81105891861', 
'67619124382', 
'35615115985', 
'49610654941', 
'60612916953', 
'33114618585', 
'49613819686', 
'26292661279', 
'84160229736', 
'17204880324', 
'93151466554', 
'17130727356', 
'21080648692', 
'39518400562', 
'28000291458', 
'23629768001', 
'93095398824', 
'31191331841', 
'53030174938', 
'59872796334', 
'87155635608', 
'55848680640', 
'25307877599', 
'81079175269', 
'36112391018', 
'97208823158', 
'45176900825', 
'16003084295', 
'86169800015', 
'65152967218', 
'66348265324', 
'28085324929', 
'47100682139', 
'33118037531', 
'14003441496', 
'36119180460', 
'62088196543', 
'95154313201', 
'14069839756', 
'24360153575', 
'15168535313', 
'28055679070', 
'70137201471', 
'95054883146', 
'77537384373', 
'73157676394', 
'82940734258', 
'76050054898', 
'78061449653', 
'39795814055', 
'45161159619', 
'13147631687', 
'98524576326', 
'70092314586', 
'31139653268', 
'22060362236', 
'74634458156', 
'94618862954', 
'68253137615', 
'19152498130', 
'22619256674', 
'36165081167', 
'17052813840', 
'12089146896', 
'27129189233', 
'25792162631', 
'42003324598', 
'21470450792', 
'77158897287', 
'89664212116', 
'59034628691', 
'62311037419', 
'80038199497', 
'83736369902', 
'35168356009') 
 
GROUP BY 
    join_month 
     
ORDER BY 
    join_month ASC 
''').show(250, False) 