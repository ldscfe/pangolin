#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
import upy
HELP = '''
------------------------------------------------------------------------------
  Name     : pangoin - pet
  Purpose  : ETL from c_etl_map
  Author   : Adam

  Revisions:
  Ver        Date        Author           Description
  ---------  ----------  ---------------  ------------------------------------
  1.0        2020/12/15  Adam             Create
  1.2        2021/03/25  Adam             Add etl_unit.etl_ins(dict)
  1.5        2021/08/26  LLC              M01 : Add passwd pre(from s_kv:db define)
  1.51       2021/08/31  LLC              M02 : Fix (etl_e)DB cursor - Source/Target(etl-3~etl-4)
  1.86       2021/12/12  Adam             M03 : PET - Add parameter - proc_name, etc...
  1.87       2022/05/13  LLC              M04 : condition - replace date_id, etc...

format:
  pangolin app=pet db=devnh etlid=10200001 proc_name=shell:pangolin_et_n bn=1000 step=100000000 debug=3.

Result:
  result           # 0=OK, >0:err

------------------------------------------------------------------------------
'''

# ETL finish
# log=1 : if result= 1, write log.
def etl_e(d_val, log=1):
    import usys
    from etl_unit import exec_sql

    _time_e = usys.utime()
    _time_b = d_val.get('_time_b', _time_e)
    d_val['secs'] = round(_time_e - _time_b, 2)

    d_val['time_e'] = usys.utime(_time_e)

    s_cols = d_val.get('col_s')
    if s_cols:
        import ustr
        s_sql = exec_sql('c_etl_map_fnl', d_val, 1)
        ustr.uout("etl_e : Update Column List : %s" % (s_sql), 2)

    if d_val.get('result', 2) == 0:
        s_sql = exec_sql('l_todolist', d_val, 1)
        s_sql = exec_sql('l_etl', d_val, 1)
        s_sql = exec_sql('finished_s_todolist', d_val, 1)
    else:
        if log==1:
            s_sql = exec_sql('l_etl', d_val, 1)
            if s_sql[0] != 0:
                print("etl_e : ETL Error Write Log : %s" % (s_sql))
            s_sql = exec_sql('fail_s_todolist', d_val, 1)

    return d_val
# END : etl_e


def etl_py(PD_PARA=None):

    import sys
    import time
    import re

    import ustr
    import usys
    import udb
    import umess
    import usql_sys

    import pet_usql
    import etl_unit

    # Command Parameter
    if not PD_PARA:
        PD_PARA = usys.cp(2, HELP)

    if len(PD_PARA) < 2:
        print(HELP)
        sys.exit(1)

    ## DEBUG ##
    ustr.DEBUG = PD_PARA.get('debug', 0)

    ## DEBUG ##
    ustr.uout("etl - INIT: Paras = %s" % (PD_PARA), 2)

    # 参数字典
    d_para = {}
    d_val = {}

    _time_b = usys.utime()
    time_b = usys.utime(_time_b)
    pid, ppid = usys.upid().split(',')
    d_val['_time_b'] = _time_b
    d_val['uuid'] = usys.uid()
    d_val['pid'] = pid
    d_val['ppid'] = ppid
    d_val['client'] = usys.uhost()
    d_val['osuser'] = usys.uuser()
    d_val['proc_name'] = 'shell:pangolin_et'
    d_val['ua'] = 'pet'
    d_val['time_b'] = time_b
    d_val['time_e'] = time_b
    d_val['secs'] = 0

    d_val.update(PD_PARA)                                                      # M03 : Add parameter - proc_name, etc... - Move from d_val={}
    d_val['proc_desc'] = d_val['proc_name'].split(':')[1]
    d_val['op_info']         = '%s|BI|%s|%s|%s' % (time_b, d_val['client'], d_val['osuser'], d_val['proc_desc'])

    # etlid : etl_id, etlid, $1. etl_id=0 auto get etl_id
    etl_id = str(d_val.get('etl_id', d_val.get('etlid', d_val.get(1, '0'))))
    d_val['etl_id'] = etl_id
    if etl_id == '0':
         d_val['_etl_id'] = "<> '%s'" % etl_id
    else:
         d_val['_etl_id'] = "= '%s'" % etl_id

    ## DEBUG ##
    ustr.uout("etl - 1: ETLID = %s" % (etl_id), 2)

    # get [db]
    s_db = PD_PARA.get('db', 'base')

    ## get etl info from db.ini[base]
    d_db = ustr.ini2dict(s_db)
    if not d_db:
        d_val['result']  = 2
        d_val['message'] = 'Error - Default Error.'
        d_val['memo']    = 'Error - Get [%s] from db.ini' % s_db
        return d_val

    # DB cursor
    try:
        # Get DB connect string(Dict)
        conn = udb.db_conn(d_db, 2)
        cur  = conn.cursor()
    except Exception as e:
        d_val['result']  = 2
        d_val['message'] = 'Error - Default Error.'
        d_val['memo']    = 'Error - %s' % str(e)
        return d_val

    d_db['type']   = d_db.get('type', '').lower()          # like : Oracle --> oracle
    d_db['connect'] = conn
    d_db['cursor'] = cur
    d_val.update(d_db)

    ## DEBUG ##
    ustr.uout("etl - 2: DB CUR = %s" % (conn), 4)

    d_val['message'] = 'Other Error.'
    d_val['memo'] = ''
    d_val['rs'] = ''
    d_val['cs'] = ''

    ## get etl_id from db

    # s_res tag
    # get [db type]_get_res_s_todolist OR get_res_s_todolist
    d_val['obj'] = 's_todolist'
    _flag = 9
    i = 1
    while _flag != 0:
        # Table is not exist.
        if _flag == 1:
            conn.rollback()

            d_val['result']  = 1
            d_val['memo']    = 'Error - Table s_res Error.'
            etl_e(d_val, 0)
            return d_val

        # Update s_res error, Resource not released.
        if i > 10:
            conn.rollback()

            d_val['result']  = 1
            d_val['memo']    = 'Error - Update s_res Error.'
            etl_e(d_val, 0)
            return d_val

        l_res_tmp = etl_unit.exec_sql('get_res_s_todolist', d_val, 1, False)
        _flag = l_res_tmp[0]
        ustr.uout("s_res - tag: %s" % (l_res_tmp), 9)
        i = i+1

    ## Get task from s_todolist
    # get [db type]_res_s_todolist OR get_s_todolist
    s_sql = etl_unit.exec_sql('get_s_todolist', d_val)[1]

    d_res = udb.uresl(cur, s_sql, 1)
    if d_res:
        d_val['etl_ky'] = d_res[0].get('ETL_KY', '')
        d_val['etl_id'] = d_res[0].get('ETL_ID', '')
        d_val['date_id'] = d_res[0].get('DATE_ID', '')
        d_val['time_id'] = d_res[0].get('TIME_ID', '')
    else:
        conn.rollback()

        d_val['result']  = 2
        d_val['message'] = 'Info - Not Task.'
        d_val['memo']    = 'Info - Not etl_id（task waiting to be execute）from s_todolist.'
        etl_e(d_val, 0)
        return d_val

    # Tag task in s_todolist
    l_res_tmp = etl_unit.exec_sql('tag_s_todolist', d_val, 1)
    # if l_res_tmp[0] = 2: Error. ****

    # free s_res TAG
    l_res_tmp = etl_unit.exec_sql('free_res_s_todolist', d_val, 1)
    # if l_res_tmp[0] >0 : Error. ****

    # get task info for etl_id
    s_sql = etl_unit.exec_sql('c_etl_map', d_val)[1]
    d_tmp = udb.uresl(d_db['cursor'], s_sql, 1)[0]

    d_val['db']              = d_tmp.get('DB', '')
    d_val['owner']           = d_tmp.get('OWNER', '')
    d_val['table']           = d_tmp.get('TABLE_NAME', '')
    d_val['field']           = d_tmp.get('FIELD_NAME', '')
    d_val['field_l']         = d_tmp.get('FIELD_NAME_L', '')
    d_val['db_t']            = ustr.unvl(d_tmp.get('DB_T'), 'default')
    d_val['owner_t']         = d_tmp.get('OWNER_T', '')
    d_val['table_t']         = d_tmp.get('TABLE_NAME_T', '')
    d_val['field_t']         = d_tmp.get('FIELD_NAME_T', '')
    d_val['condition']       = d_tmp.get('CONDITION', '')
    d_val['pre_condition']   = d_tmp.get('PRE_CONDITION', '')
    d_val['pre_process']     = d_tmp.get('PRE_PROCESS', '')

    d_val['auto_ini']  =(d_val.get('field')+'[').split('[')[1]
    d_val['field']     = d_val.get('field').split('[')[0].strip(',; ')

    # if table_t not define, get it from c_etl_map_target_t(view).
    if d_val['table_t']:
        d_val['table_t'] = d_val['owner_t'] + '.' + d_val['table_t']
    else:
        s_sql = etl_unit.exec_sql('c_etl_map_target_t', d_val)[1]
        d_tmp = udb.uresl(d_db['cursor'], s_sql, 1)[0]
        d_val['table_t'] = d_tmp.get('TARGET_T', '').split('@')[0]

    # variable(%var%) --> value
    d_val['pre_condition'] = ustr.urep(d_val['pre_condition'], d_val)
    d_val['pre_process']   = ustr.urep(d_val['pre_process'],   d_val)
    d_val['condition'] = ustr.urep(d_val['condition'],   d_val)                 # condition - replace date_id, etc...

    # get source db, target db
    d_val['_db']  = d_val['db']
    s_sql = etl_unit.exec_sql('s_kv_db', d_val)[1]
    s_db = udb.uresl(cur, s_sql, 1)[0].get('VALUE')

    d_val['_db']  = d_val['db_t']
    s_sql = etl_unit.exec_sql('s_kv_db', d_val)[1]
    s_db_t = udb.uresl(cur, s_sql, 1)[0].get('VALUE')

    d_db_s = ustr.udict(s_db, type_is='=', type_part=';')
    d_db_t = ustr.udict(s_db_t, type_is='=', type_part=';')

    l_tmp = d_db_s.get('host', 'unknow').split('//')
    d_db_s['host'] = l_tmp[len(l_tmp)-1]

    ## DEBUG ##
    ustr.uout("etl - 3: Source DB String = %s" % (d_db_s), 2)
    ustr.uout("etl - 3: Target DB String = %s" % (d_db_t), 2)

    # DB cursor - Source
    try:
        # Get DB connect string(Dict)
        _pre = ustr.str2num(d_db_s.pop('pre', '0'))        # M01: Add passwd pre(from s_kv:db define)
        conn_s = udb.db_conn(d_db_s, _pre)
        cur_s  = conn_s.cursor()
    except Exception as e:
        d_val['result']  = 1
        d_val['message'] = str(e)
        d_val['memo']    = 'Error - Source DB Connect Error.'
        etl_e(d_val, 1)
        return d_val
    # DB cursor - Target
    try:
        # Get DB connect string(Dict)
        _pre = ustr.str2num(d_db_t.pop('pre', '0'))        # M01: Add passwd pre(from s_kv:db define)
        conn_t = udb.db_conn(d_db_t, _pre)
        cur_t  = conn_t.cursor()
    except Exception as e:
        d_val['result']  = 1
        d_val['message'] = str(e)
        d_val['memo']    = 'Error - Target DB Connect Error.'
        etl_e(d_val, 1)
        return d_val


    ## DEBUG ##
    ustr.uout("etl - 4: Source DB Cursor = %s" % (cur_s), 4)
    ustr.uout("etl - 4: Target DB Cursor = %s" % (cur_t), 4)


    ## source_t --> target_t, target not exist, create.
    # source: type_s, cur_s, owner_s, table_s, cols_s
    # target: type_t, cur_t, owner_t, table_t, cols_t
    # other : auto_ini
    d_para.clear()
    d_para['type_s']         = d_db_s.get('type')
    d_para['cur_s']          = cur_s
    d_para['owner_s']        = d_val.get('owner')
    d_para['table_s']        = d_val.get('table')
    d_para['field_s']        = d_val.get('field')
    d_para['field_s_l']      = d_val.get('field_l')
    d_para['type_t']         = d_db_t.get('type')
    d_para['cur_t']          = cur_t
    d_para['owner_t']        = d_val.get('owner_t')
    d_para['table_t']        = d_val.get('table_t')
    d_para['field_t']        = d_val.get('field_t')
    d_para['pre_condition']  = d_val.get('pre_condition', '')
    d_para['condition']      = d_val.get('condition', '')
    d_para['pre_process']    = d_val.get('pre_process', '')
    # other
    d_para['bn']             = d_val.get('bn')
    d_para['step']           = d_val.get('step')
    d_para['auto_ini']       = ''

    # get auto*.ini, default partition
    s_tmp = d_val['auto_ini']
    if s_tmp:
        s_tmp = s_tmp.strip('[] ')
        ustr.uout('auto_ini : %s' % s_tmp, 3)
        try:
            fhd = open(s_tmp + '.ini', mode='r')
            d_para['auto_ini'] = fhd.read()
            fhd.close()
        except Exception as e:
            pass

    # etl sql(Create, Select, Insert)
    d_tmp = etl_unit.etl_ins(d_para)
    d_val.update(d_tmp)

    etl_e(d_val, 1)

    # close db, cursor
    cur.close()
    conn.close()
    cur_s.close()
    conn_s.close()
    cur_t.close()
    conn_t.close()

    # move some lable
    d_val.pop('_etl_id', None)
    d_val.pop('_db', None)
    d_val.pop('_time_b', None)
    d_val.pop('etl_ky', None)
    d_val.pop('passwd', None)
    d_val.pop('col_s', None)
    d_val.pop('field_l', None)
    d_val.pop('auto_ini', None)
    d_val.pop('cursor', None)
    d_val.pop('connect', None)

    #d_val['status'] = 0

    return d_val


## INIT
if __name__ == "__main__":

    res = etl_py()
    print(res)
