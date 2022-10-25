#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
#
import upy
HELP = '''
------------------------------------------------------------------------------
  Name     : pangoin - PY
  Purpose  : ETL
  Author   : Adam
  Uses     : sys, time

  Revisions:
  Ver        Date        Author           Description
  ---------  ----------  ---------------  ------------------------------------
  1.0        2019/03/22  Adam             Create
  1.1        2019/04/19  Adam             M01: write log.
  1.2        2019/10/09  LLC              M02: Default Values
  1.21       2021/08/23  LLC              M03: Output DB Define Error Info
  1.5        2021/08/26  LLC              M04: Add passwd pre(from db.ini)

format:
  pangolin app=ppy ds=cbsvp-st s="nhbase.base_app_login_info;{}, 2 REPORT_PARTITION(8), [autopart]" dt=devnh t="ods.z_t_inf_23" bn=1 step=10000 debug=3
  ds                data source(from db.ini)
  dt                data target(from db.ini)
  s                 source SQL - "P1;P2;P3". P1=owner.table@db P2=col1, ..., {}, col9 part_id(8), [autopart] P3=limit 10/ky=1001/...
  t                 target SQL
  pc                pre_condition(SQL, result > 1 --> true)
  pp                pre_process(SQL)
  sleep             [0.1]   check thread status interval(secs)
  bn                [1000]  bulk insert
  step              [1^8]   Row threshold used by monitor, and commit
  debug             [0]     output level, 0-9
  --
  p                 [0]     task parallel, like p=0-7
  line              [1]     check point(n * 10^8)
  interval          [1]     check point(n * 1 mins)
  overall           ['YES'] postgreSQL overall load

------------------------------------------------------------------------------
'''
def etl_py(PD_PARA=None):
    import os
    import sys
    import re
    ##--from uuid import uuid1

    # User Defined
    import ustr
    import usys
    import udb

    import etl_unit

    # Command Parameter
    if not PD_PARA:
        PD_PARA = usys.cp(4, HELP)

    if len(PD_PARA) < 5:
        print(HELP)
        sys.exit(1)

    ustr.DEBUG = PD_PARA.get('debug', 0)

    d_val = {}
    d_val.update(PD_PARA)

    pid, ppid = usys.upid().split(',')
    d_val['uuid'] = usys.uid()
    d_val['pid'] = pid
    d_val['ppid'] = ppid
    d_val['host'] = usys.uhost()
    d_val['osuser'] = usys.uuser()
    d_val['proc_name'] = 'shell:pangolin_py'
    d_val['proc_desc'] = d_val['proc_name'].split(':')[1]
    d_val['ua'] = 'ppy'

    time_b = usys.utime()
    d_val['time_b'] = time_b
    d_val['time_e'] = time_b
    d_val['secs'] = 0

    # default value
    if not d_val.get('line'):
        d_val['line']  = 1
    if not d_val.get('interval'):
        d_val['interval']  = 1
    if not d_val.get('sleep'):
        d_val['sleep']  = 0.1
    if not d_val.get('debug'):
        d_val['debug']  = 0
    if not d_val.get('step_line'):
        d_val['step_line']  = 1000000000
    if not d_val.get('overall'):
        d_val['overall']  = 'NO'


    # ENV
    #os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.UTF8'            #oracle


    parallel      = d_val.get('p')
    #Parallel 0-3 --> 0,1,2,3
    if parallel:
        parallel = ustr.range2str(parallel)
        d_val['p'] = parallel

    #### ETL

    # Get DB connect string(Dict)
    d_db_s = ustr.ini2dict(d_val.get('ds'))
    d_db_t = ustr.ini2dict(d_val.get('dt'))
    
    # M03: Output DB Define Error Info
    if not d_db_s:
        d_val['result']  = 1
        d_val['message'] = 'Error - Source DB Define Error: %s' % d_val.get('ds')
        d_val['memo']    = d_val['message']
        return d_val
    if not d_db_t:
        d_val['result']  = 1
        d_val['message'] = 'Error - Source DB Define Error: %s' % d_val.get('dt')
        d_val['memo']    = d_val['message']
        return d_val
    
    ## source
    d_val['type_s']          = d_db_s.get('type', '').lower()
    s_tmp =  d_val.get('s').split(';')
    # get source db, owner, table
    # db
    s_tmp_1 = (s_tmp[0] + '@').split('@')[1]
    if s_tmp_1:
        d_db_s['db'] = s_tmp_1                             # source db
    d_val['db_s']            = d_db_s.get('db')
    # owner, table
    s_tmp_1 = (s_tmp[0].split('@')[0] + '.').split('.')
    d_val['owner_s']         = s_tmp_1[0]
    d_val['table_s']         = s_tmp_1[1]

    # field
    s_tmp_1 = ustr.getlval(s_tmp, 2)
    if s_tmp_1:
        d_val['auto_ini']    =(s_tmp_1+'[').split('[')[1].strip('] ')
        d_val['field_s']     = s_tmp_1.split('[')[0].strip(',; ')

    # where
    s_tmp_1 = ustr.getlval(s_tmp, 3)
    if s_tmp_1:
        s_tmp_1_1 = s_tmp_1.strip(' ').split(' ')[0].strip(' ').lower()
        if s_tmp_1_1 != 'where':
            d_val['condition']   = s_tmp_1


    ## target
    d_val['type_t']         = d_db_t.get('type').lower()
    s_tmp = d_val.get('t').replace('{', '(').replace('}', ')')
    d_val['field_t'] = ''
    if '(' in s_tmp:
        d_val['field_t'] = s_tmp.split('(')[1].strip(') ')
        s_tmp = s_tmp.split('(')[0]
    s_tmp_1 = s_tmp.split('.')
    d_val['owner_t']        = ustr.getlval(s_tmp_1, 1)
    s_tmp = ''
    if d_val['owner_t']:
        s_tmp = d_val['owner_t'] + '.'
    d_val['table_t']        = s_tmp + ustr.getlval(s_tmp_1, 2)

    ustr.uout("etl - Source : %s" % (d_db_s), 2)

    # DB cursor - Source
    try:
        # Get DB connect string(Dict)
        _pre = ustr.str2num(d_db_s.pop('pre', '2'))        # M01: Add passwd pre(from db.ini)
        conn_s = udb.db_conn(d_db_s, _pre)
        cur_s  = conn_s.cursor()
        d_val['cur_s']       = cur_s
    except Exception as e:
        d_val['result']  = 1
        d_val['message'] = 'Error - Source DB.'
        d_val['memo']    = str(e)
        return d_val
    # DB cursor - Target
    try:
        # Get DB connect string(Dict)
        _pre = ustr.str2num(d_db_t.pop('pre', '2'))        # M01: Add passwd pre(from db.ini)
        conn_t = udb.db_conn(d_db_t, _pre)
        cur_t  = conn_t.cursor()
        d_val['cur_t']       = cur_t
    except Exception as e:
        d_val['result']  = 1
        d_val['message'] = 'Error - Target DB.'
        d_val['memo']    = str(e)
        return d_val


    # other : auto_ini
    s_tmp = d_val.pop('pc', '')
    if s_tmp:
        d_val['pre_condition']  = s_tmp

    s_tmp = d_val.pop('pp', '')
    if s_tmp:
        d_val['pre_process']    = s_tmp

    # other
    #d_para['bn']             = d_val.get('bn')
    #d_para['step']           = d_val.get('step')

    # get auto*.ini, default partition
    s_tmp = d_val.get('auto_ini')
    if s_tmp:
        s_tmp = s_tmp.strip('[] ')
        try:
            fhd = open(s_tmp + '.ini', mode='r')
            d_val['auto_ini'] = fhd.read()
            fhd.close()
        except Exception as e:
            d_val['auto_ini'] = ''
            d_val['memo'] = str(e)
            pass

    ustr.uout("etl - auto_ini : %s" % (s_tmp), 3)

    # etl sql(Create, Select, Insert)

    d_tmp = etl_unit.etl_ins(d_val)
    d_val.update(d_tmp)

    time_e = usys.utime()
    time_b = d_val.get('time_b', time_e)
    d_val['secs'] = round(time_e - time_b, 2)

    d_val['time_b'] = usys.utime(time_b)
    d_val['time_e'] = usys.utime(time_e)

    # close db, cursor
    cur_s.close()
    conn_s.close()
    cur_t.close()
    conn_t.close()

    # move some lable
    d_val.pop('cur_s', None)
    d_val.pop('cur_t', None)
    d_val.pop('auto_ini', None)
    d_val.pop('cursor', None)
    d_val.pop('connect', None)


    return d_val

# end etl_py

if __name__ == "__main__":

    d_res = etl_py()
    print(d_res)
