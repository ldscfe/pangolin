# -*- coding: utf-8 -*-
#
import upy
'''-----------------------------------------------------------------------------
 Name     : pet_usql
 Purpose  : User-defined database SQL - pet
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2020/12/21  Adam             Create

-----------------------------------------------------------------------------'''
import sys

import pet_usql_oracle
import pet_usql_mysql


#### General
# get_res_s_todolist
__get_res_s_todolist         = '''
   update   bi.s_res
   set      status    = '1',
            stem_from = '%client%:%pid%,%ppid%',
            UT        = '%time_b%',
            UA        = '%ua%'
   where    1=1
   and      status   = '0'
   and      obj_code = '%obj%'
   and      bi_code  = '%db%'
'''

# free_res_s_todolist
__free_res_s_todolist        = '''
   update   bi.s_res
   set      status = '0'
   where    1=1
   and      status  <> '0'
   and      obj_code = '%obj%'
   and      bi_code  = '%db%'
'''

# c_etl_map
__c_etl_map                 = '''
   select   *
   from     c_etl_map
   where    1=1
   and      etl_id = '%etl_id%'
'''
# c_etl_map - target_t
__c_etl_map_target_t         = '''
   select   target_t
   from     v_proc_map
   where    proc_id = '%etl_id%'
'''
# c_etl_map - field_name_l
__c_etl_map_fnl              = '''
   update   c_etl_map
   set      field_name_l   = '%col_s%',
            memo           = 'update fnl from %op_info%.'
   where    etl_id = '%etl_id%'
'''
# s_kv : db
__s_kv_db                    = '''
   select   *
   from     s_kv
   where    1=1
   and      k_name like '%:%_db%%'
   and      k_type = '0001'
'''

# SQL : insert
__sql_insert                 = '''
   INSERT INTO %table_t% %field_t% VALUES %val%
'''

# SQL : Truncate
__all_delete                 = '''
   truncate table %table_t%
'''

#### Variable Lists
####
usql = {
 # General
 'get_res_s_todolist'        : __get_res_s_todolist,
 'free_res_s_todolist'       : __free_res_s_todolist,
 'c_etl_map'                 : __c_etl_map,
 'c_etl_map_target_t'        : __c_etl_map_target_t,
 'c_etl_map_fnl'             : __c_etl_map_fnl,
 's_kv_db'                   : __s_kv_db,
 'sql_insert'                : __sql_insert,
 'all_delete'                : __all_delete,
 #'table_exist'               : usql_sys.__tab_exist,

 # Oracle
 'oracle_get_s_todolist'     : pet_usql_oracle.__get_s_todolist,
 'oracle_tag_s_todolist'     : pet_usql_oracle.__tag_s_todolist,
 'oracle_l_todolist'         : pet_usql_oracle.__l_todolist,
 'oracle_l_etl'              : pet_usql_oracle.__l_etl,
 'oracle_finished_s_todolist': pet_usql_oracle.__finished_s_todolist,
 'oracle_fail_s_todolist'    : pet_usql_oracle.__fail_s_todolist,
 #'oracle_table_exist'        : usql_sys.__oracle_tab_exist,

 # MySQL
 #'mysql_tab_col'             : pet_usql_mysql.__mysql_tab_col,

 'default'                   : None
}

umap = {
    #'mysql_oracle'           : pet_usql_oracle.__mysql_oracle.replace('\n', ' '),

    'default'                : None
}
