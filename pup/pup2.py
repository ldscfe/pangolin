#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
import upy
HELP = '''
------------------------------------------------------------------------------
  Name     : Load Data From CSV Using db.ini
  Purpose  :
  Author   : Adam

  Revisions:
  Ver        Date        Author           Description
  ---------  ----------  ---------------  ------------------------------------
  20220825  Adam        1.0   - Create
  20220916  Adam        1.91  - M01: Change if (Source is/not Exist)
  20220920  Adam        1.92  - M02: Fix Load CSV to DB(Greenplum)

format:
  pangolin app=pup2 ds=fcbs71 s="/owdb/2022091509*.csv.gz" dt=cbsgp188 t="ods.gio_ads_track_click_2 debug=3
  ds                source file (from db.ini)
  dt                data target (from db.ini)
  s                 source file (regular expression: *)
  t                 target Table
  pc                pre_condition (SQL, result > 1 --> true)
  pp                pre_process (SQL)

  pr                [2] passwd PRE
  rv                [0] Result Verify. using .inf, default 0=not verify
  debug             [0] output level, 0-9
  --
  p                 [0] task parallel, like p=0-7
  -- s,t Variable:
  yesterday         yyyymmdd, now-1
  now-N             yyyymmdd
------------------------------------------------------------------------------
'''
def etl_py(G_VAR=None):
    import os
    import ustr
    import usys
    import etl_unit
    import usql_sys
    import re

    # Command Parameter
    if not G_VAR or len(G_VAR) < 5:
        G_VAR = usys.cp(5, HELP)
    
    ## s,t Variable replace
    strTmp = G_VAR.get('s', '').lower()
    while 1:
        s1 = strTmp.split('%')
        if len(s1) < 2:
            break;
        s2 = s1[1].split('-')
        s2.append('0')
        if s2[0] == 'yesterday':
            t1 = usys.utime(usys.utime_offset((int(s2[1])+1)*1*24*3600))[:8]
            strTmp = re.sub('%yesterday.*?%', t1, strTmp)
        elif s2[0] == 'now':
            t1 = usys.utime(usys.utime_offset((int(s2[1])+0)*1*24*3600))[:8]
            strTmp = re.sub('%now.*?%', t1, strTmp)
        else:
            break
    G_VAR['s'] = strTmp
    #print(strTmp)

    #os._exit(1)


    ## DEBUG ##
    #ustr.DEBUG = G_VAR.get('debug', 0)

    d_res = {'proc_name': 'shell:pangolin_up', 'app': 'pup2', 'result': 0, 'message': None, 'memo': None, 'rs': None, 'secs': 0}
    d_res.update(G_VAR)
    d_res.update(etl_unit.etl_init(d_res))
    d_res.update(etl_unit.etl_db(d_res))
    ustr.uout("Source - G_VAR: %s" % (G_VAR), 3)
    ustr.uout("Source - d_res: %s" % (d_res), 3)

    
    # INIT
    PRE_CONDITION = d_res.get('pc', None)    # SQL
    PRE_PROCESS   = d_res.get('pp', None)    # SQL
    PRE           = d_res.get('pr', 2)       # passwd PRE, default 2
    RESULT_V      = d_res.get('rv', 0)       # result verify using .inf, default 0=not verify


    # Source is Exist, OR exit.
    s_fn = d_res.get('ds').get('db')+d_res.get('ds').get('table_name')
    ## DEBUG ##
    ustr.uout("Source - Path: %s" % (s_fn), 2)

    res = os.popen('ls %s' % s_fn, "r").read()
   
    # OLD: if not os.access(s_fn, os.R_OK):
    if not res:
        time_e = usys.utime()
        d_res['secs'] = round(time_e - d_res['time_e'], 2)
        d_res['time_e'] = usys.utime(time_e)
        d_res['result'] = 1
        d_res['message'] = 'Error - Source is Not Exist.'

        return d_res
    l_file = res.strip().split('\n')
    #print(l_file)
    #os._exit(1)

    s_format = d_res.get('ds').get('format', 'csv')


    # Target is Exist, OR exit.

    # pre_condition

    # Load CSV to DB
    if 'greenplum' in d_res.get('dt').get('type').lower():
        
        cmd = usql_sys.usql.get('greenplum_load_data_' + s_format)
        #print('CMD: %s, format: %s' % (cmd, s_format))
        if not cmd:
            d_res['result'] = 1
            d_res['message'] = 'Error - Load Data Not Define: %s.' % ('greenplum_load_data_' + s_format)
            return d_res

        cmd = cmd.strip(' \n\r')

        # cmd = cmd.replace('%FN%', s_fn)
        cmd = cmd.replace('%DB%', d_res.get('dt').get('db'))
        cmd = cmd.replace('%HOST%', d_res.get('dt').get('host'))
        cmd = cmd.replace('%USER%', d_res.get('dt').get('user'))
        cmd = cmd.replace('%TB%', d_res.get('dt').get('table_name'))
        cmd = cmd.replace('%HEADER%', d_res.get('ds').get('header', 'false'))
        cmd = cmd.replace('%ENCODING%', d_res.get('ds').get('encoding', 'utf8'))
        passwd = d_res.get('dt').get('passwd')
        passwd = re.sub('\n', '',  os.popen("./crypt bidb " + passwd + " U", "r").read())[PRE:]
        cmd = cmd.replace('%PASSWD%', passwd)
        ## DEBUG ##
        # M02 Dev Version
        #cmd = 'sudo runuser -l gpadmin -c "%s"' % cmd
        #print(cmd)
        #os._exit(1)
        ustr.uout("Greenplum - Load: %s" % (cmd), 3)

        CMD_TEMPLATE = cmd
        d_1 = {'result_l': [], 'rs_l': [], 'inf_v': [], 'file_err': []}
        n_rs_total = 0
        result_total = 0
        message_total = None
        for s_fn in l_file:
            _res = get_inf_rs(s_fn)
            _rs = _res.pop('_rs', None)

            # set inf exist flag 0, or 1=not
            if _rs:
                d_1['inf_v'].append(0)
            else:
                d_1['inf_v'].append(1)
            
            # get file from list
            cmd = CMD_TEMPLATE.replace('%FN%', s_fn)
            ustr.uout("Greenplum - CMD: %s" % (cmd), 3)
            res = os.popen(cmd, "r").read()
        
            ## RES: 
            # OK: COPY 10000
            # ER: ERROR: value too long for ...
            # _rs(.inf) VS load: n_rs
            
            if not 'error' in res.lower():
                n_rs = ustr.str2num(res.split(' ')[1], 0)
                n_rs_total = n_rs_total + n_rs
                if not _rs or _rs == n_rs:
                    d_1['result_l'].append(0)
                    d_1['rs_l'].append(n_rs)
                else:
                    if RESULT_V:
                        d_1['result_l'].append(1)
                        d_1['rs_l'].append(n_rs)
                        d_1['file_err'].append(ustr.getlval(s_fn.split('/'), 0))
                        result_total = result_total + 1
                        if not message_total:
                            message_total = 'Warning - Load Data Count Exceptions(%s - %s).' % (_rs, n_rs)
                    else:
                        d_1['result_l'].append(0)
                        d_1['rs_l'].append(n_rs)
            else:
                ##?? if 'result_v' == 1: _rs <> n_rs exit.
                result_total = result_total + 1
                d_1['result_l'].append(1)
                d_1['rs_l'].append(None)
                d_1['file_err'].append(ustr.getlval(s_fn.split('/'), 0))
                # d_1['message_l'].append('Error - Load Data Error. %s' % res.replace('\n', ' ').replace('\r', ' ').replace('"', ' ').replace("'", ' '))
                if not message_total:
                    message_total = 'Error - Load Data Error. %s' % res.replace('\n', ' ').replace('\r', ' ').replace('"', ' ').replace("'", ' ')

        # end for s_fn in l_file:

        d_res['result'] = result_total
        d_res['message'] = message_total
        d_res['rs'] = n_rs_total
        d_res.update(d_1)

    elif 'clickhouse' in d_res.get('dt').get('type').lower():

        cmd = usql_sys.usql.get('greenplum_load_data_csv_gz')
        cmd = cmd.strip(' \n\r')

        cmd = cmd.replace('%FN%', d_res.get('ds').get('db')+d_res.get('ds').get('table_name'))
        cmd = cmd.replace('%DB%', d_res.get('dt').get('db'))
        cmd = cmd.replace('%TB%', d_res.get('dt').get('table_name'))
        cmd = cmd.replace('%ENCODING%', d_res.get('ds').get('encoding'))
        print(cmd)

        # os cmd
        #res = os.popen(cmd, "r").read()
    else:
        print('other')


    time_e = usys.utime()
    d_res['secs'] = round(time_e - d_res['time_e'], 2)
    d_res['time_e'] = usys.utime(time_e)

    _ds = d_res.get('ds')
    _ds.pop('passwd', None)
    _ds.pop('connect', None)
    _ds.pop('cursor', None)
    d_res['ds'] = _ds
    _dt = d_res.get('dt')
    _dt.pop('passwd', None)
    _dt.pop('connect', None)
    _dt.pop('cursor', None)
    d_res['dt'] = _dt

    return d_res

# get inf from filename( demo.csv.gz --> demo.inf)
def get_inf_rs(s_fn):
    import os
    import ustr
    
    s_fn_f1 = s_fn.split('.')[0] + '.inf'

    _d_res = {'_rs': None, 'memo': 'Source INF is Not Exist.'}
    if os.access(s_fn_f1, os.R_OK):
        s_tmp = os.popen('cat ' + s_fn_f1, "r").read()
        _d_res['_rs'] = ustr.str2num(s_tmp.split('=')[1], 0)
    
    return _d_res

## INIT
if __name__ == "__main__":

    res = etl_up()

    print(res)
