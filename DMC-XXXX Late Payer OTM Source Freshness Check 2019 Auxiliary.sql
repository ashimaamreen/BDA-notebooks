from __future__ import print_function 
 
tables = ('gms.cx_cvm_churn', 
          'gms.cx_cvm_value', 
          'gms.s_addr_per', 
          'gms.s_asset', 
          'gms.s_contact', 
          'gms.s_contact_x', 
          'gms.s_contact_fnx', 
          'gms.s_order', 
          'gms.s_order_item', 
          'gms.s_order_type', 
          'gms.s_org_ext', 
          'gms.s_org_ext_x', 
          'gms.s_prod_int') 
 
for t in tables: 
    print(t, end = ': ') 
    print(str(spark.sql('SELECT MAX(last_upd) FROM {t}'.format(t = t)).collect()[0]['max(last_upd)'])) 
     
tables = ('avayaoutbound.pim_contact', 
          'avayaoutbound.pim_contact_store', 
          'avayaoutbound.pim_completion_code') 
 
for t in tables: 
    print(t, end = ': ') 
    print(str(spark.sql('SELECT MAX(last_modified_on) FROM {t}'.format(t = t)).collect()[0]['max(last_modified_on)'])) 