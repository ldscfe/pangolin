# -*- coding: utf-8 -*-
#
'''-----------------------------------------------------------------------------
 Name     : pet_usql_oracle
 Purpose  : User-defined Oracle SQL - pet
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2020/12/21  Adam             Create

-----------------------------------------------------------------------------'''
# get_s_todolist
__get_s_todolist             = '''
   select   rowid etl_ky, proc_id etl_id, date_id, time_id
   from     bi.s_todolist
   where    1=1
   and      proc_id                 %_etl_id%
   and      counts                  < 99
   and      nvl(flag, 0)            = '0'
   and      proc_name               = '%proc_name%'
   and      nvl(time_b, sysdate-1)  <  sysdate - 5/24/60
   order by create_time + level_id/24/6 desc
'''

__tag_s_todolist             = '''
   update   bi.s_todolist
   set      flag   = '%pid%,%ppid%',
            time_b = sysdate,
            counts = counts + 1
   where    rowid = '%etl_ky%' and proc_id = '%etl_id%'
'''

__l_todolist                 = '''
   insert into bi.l_todolist
   select   DATE_ID, TIME_ID, PROC_NAME, PROC_ID,
            STEM_FROM||',pangolin:pet' , CRONTAB, CRONTAB_RANGE, LEVEL_ID, CREATE_TIME, TIME_B,
            sysdate    time_e,
            %secs%      secs,
            COUNTS,
            '%result%' result,
            '%op_info%'op_info,
            '%memo%'   memo
   from     bi.s_todolist
   where    rowid = '%etl_ky%' and proc_id = '%etl_id%'
'''

__l_etl                      = '''
   insert into bi.l_etl
   select   DATE_ID, TIME_ID, PROC_NAME, PROC_ID,
            '%proc_desc%' PROC_DESC, TIME_B,
            sysdate     TIME_E,
            %secs%      SECS,
            %rs%        COUNTS,
            '%owner%.%table%@%db%'   SOURCE_TABLE,
            '%table_t%' TARGET_TABLE,
            '%result%'  result,
            '%op_info%' op_info,
            '%memo%'    memo
   from     bi.s_todolist
   where    rowid = '%etl_ky%' and proc_id = '%etl_id%'
'''

__finished_s_todolist        = '''
   delete from bi.s_todolist
   where    rowid = '%etl_ky%' and proc_id = '%etl_id%'
'''

__fail_s_todolist            = '''
   update   bi.s_todolist
   set      flag   = '0',
            time_b = sysdate
   where    rowid = '%etl_ky%' and proc_id = '%etl_id%'
'''

####  Table Column Type
## format: id, name, type, field, col_len, col_len_octet, col_cset
#         1 LOG_NR_  bigint   bigint      19,0  19,0
#         2 TYPE_    varchar  varchar(64) 64    192   utf8

