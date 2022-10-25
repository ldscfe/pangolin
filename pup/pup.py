#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
HELP = '''
------------------------------------------------------------------------------
  Name     : upload csv/tsv to GP
  Purpose  :
  Author   : LLC

  Revisions:
  Ver        Date        Author           Description
  ---------  ----------  ---------------  ------------------------------------
  1.0        2021/07/09  LLC              Create

1. set s_res:s_todolist, status=1, stem_from=pgp:pid,ppid (NOT COMMIT)
2. get proc_id, date_id, filename(table), path(db), _tmp(owner) from s_todolist, key=pangolin_gp
3. set s_todolist time_b=%time%,flag=pid,ppid (%proc_id%)
4. rollback (1)
4. upload csv to GP
5. sucess : move s_todolist time_b=%time%,flag=pid,ppid to l_todolist (%proc_id%)
            write l_etl (%proc_id%)
            set s_res:s_todolist, status=0, stem_from=pgp:pid,ppid (COMMIT)   # ststus=0 redundant design
   Error  : set s_todolist time_b=%time%,flag=0 (%proc_id%)
            write l_etl (%proc_id%)
------------------------------------------------------------------------------
'''

# ETL finish
# log=1 : if result=-1, write log.
def etl_e(d_res, log=1):
    import usys
    from etl_unit import exec_sql

    _time_e = usys.utime()
    _time_b = d_res.get('_time_b', _time_e)
    d_res['secs'] = round(_time_e - _time_b, 2)
    
    d_res['time_e'] = usys.utime(_time_e)

    s_cols = d_res.get('col_s')
    if s_cols:
        import ustr
        s_sql = exec_sql('c_etl_map_fnl', d_res, 1)
        ustr.uout("etl_e : Update Column List : %s" % (s_sql), 2)
    
    if d_res.get('result', 2) == 0:
        s_sql = exec_sql('l_todolist', d_res, 1)
        s_sql = exec_sql('l_etl', d_res, 1)
        s_sql = exec_sql('finished_s_todolist', d_res, 1)
    else:
        if log==1:
            s_sql = exec_sql('l_etl', d_res, 1)
            if s_sql[0] != 0:
                print("etl_e : ETL Error Write Log : %s" % (s_sql))
            s_sql = exec_sql('fail_s_todolist', d_res, 1)

    return d_res
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
    import etl_unit


    # Command Parameter
    if not PD_PARA:
        PD_PARA = usys.cp(0, HELP)

    ## DEBUG ##
    ustr.DEBUG = PD_PARA.get('debug', 0)

    ## DEBUG ##
    ustr.uout("etl - INIT: Paras = %s" % (PD_PARA), 2)

    # 
    d_res = {}
    d_res.update(PD_PARA)


    pid, ppid = usys.upid().split(',')
    d_res['uuid'] = usys.uid()
    d_res['pid'] = pid
    d_res['ppid'] = ppid
    d_res['client'] = usys.uhost()
    d_res['osuser'] = usys.uuser()
    d_res['proc_name'] = 'shell:pangolin_up'
    d_res['proc_desc'] = d_res['proc_name'].split(':')[1]
    d_res['ua'] = 'pup'

    _time_b = usys.utime()
    d_res['_time_b'] = _time_b
    time_b = usys.utime(_time_b)
    d_res['time_b'] = time_b
    d_res['time_e'] = time_b
    d_res['secs'] = 0

    d_res['op_info']         = '%s|BI|%s|%s|%s' % (time_b, d_res['client'], d_res['osuser'], d_res['proc_desc'])

    # etlid : etl_id, etlid, $1. etl_id=0 auto get etl_id
    etl_id = str(d_res.get('etl_id', d_res.get('etlid', d_res.get(1, '0'))))
    d_res['etl_id'] = etl_id
    if etl_id == '0':
         d_res['_etl_id'] = "<> '%s'" % etl_id
    else:
         d_res['_etl_id'] = "= '%s'" % etl_id

    ## DEBUG ##
    ustr.uout("ulgp - 10: ETLID = %s" % (etl_id), 2)

    # get [db]
    s_db = PD_PARA.get('db', 'base')

    ## get etl info from db.ini[base]
    d_db = ustr.ini2dict(s_db)
    if not d_db:
        d_res['result']  = 2
        d_res['message'] = 'Error - Default Error.'
        d_res['memo']    = 'Error - Get [%s] from db.ini' % s_db
        return d_res

    ## DEBUG ##
    ustr.uout("ulgp - 20: d_db = %s" % (d_db), 2)

    # DB cursor
    try:
        # Get DB connect string(Dict)
        conn = udb.db_conn(d_db, 2)
        cur  = conn.cursor()
    except Exception as e:
        d_res['result']  = 2
        d_res['message'] = 'Error - Default Error.'
        d_res['memo']    = 'Error - %s' % str(e)
        return v

    d_db['type']   = d_db.get('type', '').lower()          # like : Oracle --> oracle
    d_db['connect'] = conn
    d_db['cursor'] = cur
    d_res.update(d_db)

    ## DEBUG ##
    ustr.uout("ulgp - 30: DB CUR = %s" % (conn), 4)

    d_res['message'] = 'Other Error.'
    d_res['memo'] = ''
    d_res['rs'] = ''
    d_res['cs'] = ''


    ## get etl_id from db

    # s_res tag
    # get [db type]_get_res_s_todolist OR get_res_s_todolist
    d_res['obj'] = 's_todolist'
    _flag = 9
    i = 1
    while _flag != 0:
        # Table is not exist.
        if _flag == 1:
            conn.rollback()
            d_res['result']  = 1
            d_res['memo']    = 'Error - Table s_res Error.'
            etl_e(d_res, 0)
            return d_res

        # Update s_res error, Resource not released.
        if i > 10:
            conn.rollback()

            d_res['result']  = 1
            d_res['memo']    = 'Error - Update s_res Error.'
            etl_e(d_res, 0)
            return d_res

        l_res_tmp = etl_unit.exec_sql('get_res_s_todolist1', d_res, 1, False)
        if l_res_tmp:
            _flag = l_res_tmp[0]
        else:
            _flag = 1
        
        i = i+1

    ustr.uout("ulgp - 40: s_res - tag: %s" % (l_res_tmp), 3)
    
    ## Get task from s_todolist
    # get [db type]_res_s_todolist OR get_s_todolist
    s_sql = etl_unit.exec_sql('get_s_todolist', d_res)[1]

    d_res = udb.uresl(cur, s_sql, 1)
    if d_res:
        d_res['etl_ky'] = d_res[0].get('ETL_KY', '')
        d_res['etl_id'] = d_res[0].get('ETL_ID', '')
        d_res['date_id'] = d_res[0].get('DATE_ID', '')
        d_res['time_id'] = d_res[0].get('TIME_ID', '')
    else:
        conn.rollback()

        d_res['result']  = 2
        d_res['message'] = 'Info - Not Task.'
        d_res['memo']    = 'Info - Not etl_idtask waiting to be executefrom s_todolist.'
        etl_e(d_res, 0)
        return d_res

    ustr.uout("ulgp - 50: s_todolist - tag: %s" % (l_res_tmp), 3)

    # Tag task in s_todolist
    l_res_tmp = etl_unit.exec_sql('tag_s_todolist', d_res, 1)
    # if l_res_tmp[0] = 2: Error. ****

    # free s_res TAG
    l_res_tmp = etl_unit.exec_sql('free_res_s_todolist', d_res, 1)
    # if l_res_tmp[0] >0 : Error. ****

    # get task info for etl_id
    s_sql = etl_unit.exec_sql('c_etl_map', d_res)[1]
    d_tmp = udb.uresl(d_db['cursor'], s_sql, 1)[0]

    d_res['db']              = d_tmp.get('DB', '')
    d_res['owner']           = d_tmp.get('OWNER', '')
    d_res['table']           = d_tmp.get('TABLE_NAME', '')
    d_res['field']           = d_tmp.get('FIELD_NAME', '')
    d_res['field_l']         = d_tmp.get('FIELD_NAME_L', '')
    d_res['db_t']            = ustr.unvl(d_tmp.get('DB_T'), 'default')
    d_res['owner_t']         = d_tmp.get('OWNER_T', '')
    d_res['table_t']         = ''
    d_res['field_t']         = d_tmp.get('FIELD_NAME_T', '')
    d_res['condition']       = d_tmp.get('CONDITION', '')
    d_res['pre_condition']   = d_tmp.get('PRE_CONDITION', '')
    d_res['pre_process']     = d_tmp.get('PRE_PROCESS', '')

    d_res['auto_ini']  =(d_res.get('field')+'[').split('[')[1]
    d_res['field']     = d_res.get('field').split('[')[0].strip(',; ')
    
    return d_res

## INIT
if __name__ == "__main__":

    res = etl_py()
    print(res)
