# -*- coding: utf-8 -*-
#
import upy
'''-----------------------------------------------------------------------------
 Name     : etl_ins
 Purpose  : ETL function - Create, Select, Insert
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2021/03/21  Adam             Create
 1.2        2021/05/19  Adam             etl_ins_many_err : bulk-null problem
 1.5        2021/05/21  Adam             memo : ustr.ure(memo)
 1.6        2021/06/04  Adam             M01 : Source Cols VS Target Cols, dev..ing
 1.7        2021/07/23  LLC              M02 : all-delete --> all_delete Fix
 1.8        2021/08/10  Adam             M03 : field_s_l
 1.81       2021/08/20  Adam             M00 : Add Oracle --> MySQL
 1.83       2021/09/11  LLC              M04 : pre_condition Add cur_s
 1.85       2021/12/12  Adam             M05 : Support utf8 special characters, bn=1. values :s --> N'%s'
 1.90       2022/08/25  Adam             Add etl_init, etl_db


 Function:
   exec_sql
   etl_ins
     etl_ins_many_err

-----------------------------------------------------------------------------'''
## INIT
def etl_init(__VAR=None):
    import usys

    d_res = {}
    pid, ppid = usys.upid().split(',')
    d_res['uuid'] = usys.uid()
    d_res['pid'] = pid
    d_res['ppid'] = ppid
    d_res['host'] = usys.uhost()
    d_res['ip'] = usys.uip()
    d_res['osuser'] = usys.uuser()
    d_res['proc_desc'] = __VAR.get('proc_name', 'shell:None').split(':')[1]

    time_b = usys.utime()
    d_res['time_b'] = usys.utime(time_b)
    d_res['time_e'] = time_b
    d_res['secs'] = 0
    d_res['op_info']   = '%s|BI|%s|%s|%s' % (d_res['time_b'], d_res['host'], d_res['osuser'], d_res['proc_desc'])

    return d_res


## DB Connect
def etl_db(__VAR=None):
    import usys
    import udb
    import ustr

    d_res = {}

    # DB cursor
    try:
        # Get Source DB connect (Dict)
        d_1 = {'name': 'ds'}
        _d_1 = ustr.ini2dict(__VAR['ds'])
        d_1.update(_d_1)
        d_1['table_name'] = __VAR.pop('s', '')

        if 'file' in d_1.get('type', '').lower():
            conn = None
            cur  = None
            d_1['connect'] = conn
            d_1['cursor']  = cur
            pass
        else:
            conn = udb.db_conn(d_1, 2)
            cur  = conn.cursor()
            d_1['connect'] = conn
            d_1['cursor']  = cur
        __VAR['ds'] = d_1

        # Get Target DB connect (Dict)
        d_1 = {'name': 'dt'}
        _d_1 = ustr.ini2dict(__VAR['dt'])
        d_1.update(_d_1)
        d_1['table_name'] = __VAR.pop('t', '')

        if 'file' in d_1.get('type', '').lower():
            pass
        else:
            conn = udb.db_conn(d_1, 2)
            cur  = conn.cursor()
            d_1['connect'] = conn
            d_1['cursor']  = cur
        __VAR['dt'] = d_1
    except Exception as e:
        d_res['result']  = 2
        d_res['message'] = 'Error - %s Error.' % d_1.get('name')
        d_res['memo']    = 'Error - %s - %s' % (d_1.get('name'), str(e))
        return d_res

    return d_res


# get SQL : (code, para{})
# exec SQL: (code, para{}, result count)
# exec SQL & not commit: (code, para{}, result count, False)
# para{} = type, connect, cursor
def exec_sql(s_sql_code, d_para, rs=None, commit=True):
    if not s_sql_code:
        return ''
    import udb
    import ustr

    import usql_sys
    import pet_usql
    ustr.uout("exec_sql : %s = %s" % (s_sql_code, d_para), 3)

    s_sql = ''

    db_type = d_para.get('type', '').lower()

    # if s_sql_code is SQL
    if len(s_sql_code.split(' ')) > 1:
        s_sql = s_sql_code
    else:
        s_sql = usql_sys.usql.get(db_type + '_' + s_sql_code, usql_sys.usql.get(s_sql_code))
        if not s_sql:
            s_sql = pet_usql.usql.get(db_type + '_' + s_sql_code, pet_usql.usql.get(s_sql_code))

    return udb.exec_sql(s_sql, d_para, rs, commit)
# END : exec_sql


#
def etl_ins_many_err(cur_t, s_sql_t, l_rs_bulk):
    import ustr
    d_return = {'result':0, 'memo':'OK.'}
    try:
        cur_t.executemany(s_sql_t, ustr.valconv(l_rs_bulk))          # This line for Solve the nausea 0000-00-00 ** date problem
    except Exception as e:
        try:
            # This part for Solve the Fucking bulk-null problem
            for rs1 in l_rs_bulk:
                try:
                    cur_t.execute(s_sql_t, rs1)
                except Exception as e:
                    rs1 = ustr.valconv(rs1, 1)
                    cur_t.execute(s_sql_t, rs1)
        except Exception as e:
            d_return['result'] = 1
            d_return['memo']   = 'FAILED_INSERT: %s' % ustr.ure(str(e))
            ustr.uout( 'Error : %s' % rs1, 1 )

    return d_return
# END : etl_ins_many_err


def etl_ins(d_para):

    import re

    import ustr
    import udb
    import usql_sys

    # Constant
    C_BN   = 1000
    C_STEP = 1000000000

    ## DEBUG ##
    ustr.uout("etl_ins - INIT: Paras = %s" % (d_para), 2)

    ## INIT

    ## source --> target
    cur_s = d_para['cur_s']
    cur_t = d_para['cur_t']

    d_val = {}

    # Create Table is not exist
    _res = udb.csql(d_para)
    ustr.uout("etl_ins : Create Table %s = %s" % (d_para['table_t'], _res), 4)
    if ustr.str2num(_res.get('result')) != 0:
        d_val['result']  = 1
        d_val['message'] = 'Error - Create Table.'
        d_val['memo']    = 'Error - Create Table %s.' % ustr.ure(d_para['table_t'])
        ## pet_unit.etl_e(d_db, d_val)
        return d_val
    else:
        d_val['memo'] = ustr.ure(_res.get('message'))


    ## pre_condition
    # NULL, >0, SQL[0]>0   --> true
    # 0                    --> false
    s_sql_pre_condition = d_para.get('pre_condition', '').strip(' ')
    pre_condition = 0
    if not s_sql_pre_condition or s_sql_pre_condition == '1':
        pre_condition = 1
    elif s_sql_pre_condition == '0':
        pass
    elif ustr.unvl(ustr.str2num(s_sql_pre_condition), 0) >= 1:
        pass
    # SQL
    elif s_sql_pre_condition:
        l_tmp = udb.uresl(cur_s, s_sql_pre_condition, 1)
        if not l_tmp:
            l_tmp = udb.uresl(cur_t, s_sql_pre_condition, 1)                   # M04 : pre_condition Add cur_s

        if l_tmp:
            pre_condition = 1 if ustr.unvl(ustr.str2num(ustr.getdval(l_tmp[0], '0')), 0) > 0 else 0

    # FAILED_PRE_CONDITION
    if pre_condition <= 0:
        d_val['result']  = 1
        d_val['message'] = 'Info - PRE_CONDITION.'
        d_val['memo']  = 'FAILED_PRE_CONDITION: %s' % ustr.ure(s_sql_pre_condition)
        #pet_unit.etl_e(d_db, d_val)
        return d_val


    ## pre_process
    s_sql = d_para.get('pre_process', '')
    if s_sql:
        #s_sql = s_sql.replace('-', '_')                                       # M02 : all-delete --> all_delete Fix
        s_sql = s_sql.replace('all-delete', 'all_delete')
        d_vp = {}
        d_vp['type'] = d_para.get('type_t', '')
        if s_sql == 'all_delete':
            s_sql = exec_sql(s_sql, d_vp)[1]
        # replace table_t, date_id
        d_vp['table_t'] = d_para.get('table_t')
        d_vp['date_id'] = d_para.get('date_id')
        #if s_sql:
        #    s_sql = ustr.urep(s_sql, d_vp)
        # exec SQL
        d_vp['cursor'] = d_para.get('cur_t')
        s_sql = exec_sql(s_sql, d_vp, 0)
        if s_sql[0] != 0:
            d_val['message2'] = 'Info - FAILED_PRE_PROCESS : %s.' % s_sql[1]


    ## Get Source Table Cols
    # s_cols
    #print('----TEST')
    #print('field_s_l: %s' % d_para.get('field_s_l'))
    #print('owner_s: %s' % d_para['owner_s'])
    #print('table_s: %s' % d_para['table_s'])
    #print(d_para)


    s_cols = d_para.get('field_s_l')
    if s_cols:
        d_val['col_s'] = s_cols                                                # M03: old = ''
    else:
        d_para['type'] = "'NULL'"
        s_col_def      = d_para.get('field_s', '').strip(' ')
        if not s_col_def or s_col_def == '*':
            s_col_def = '{}'
        if '{' in s_col_def:
            s_tmp  = re.search('\{.*\}', s_col_def)[0].strip('{}')
        else:
            s_tmp  = s_col_def
        d_para['col']  = "'" + s_tmp.replace(' ', '').replace(',', "','") + "'"
        d_para['tab']  = d_para.get('owner_s') + '.' + d_para.get('table_s')

        # Source Table Field Enclose
        s_ENC = usql_sys.uenclose.get(d_para.get('type_s'))

        d_vp = {}
        d_vp['type'] = d_para.get('type_s')
        d_vp['tab'] = '%s.%s' % (d_para.get('owner_s'), d_para.get('table_s'))

        ## ???? notype, nocol .... in db.ini
        d_vp['data_type'] = d_para.get('notype', "'NULL'")
        d_vp['col'] = d_para.get('nocol', "'NULL'")

        _tmp = exec_sql('tab_col', d_vp)
        if not _tmp:
            d_val['result']  = 1
            d_val['memo']  = '%s_tab_col Not Define.' % d_para.get('type_s')
            # pet_unit.etl_e(d_db, d_val, 1)
            return d_val
        s_sql = _tmp[1]
        s_sql = ustr.urep(s_sql, d_vp)

        ustr.uout("etl_ins : Get Source Table Define = %s" % (s_sql), 5)

        d_cols_s = udb.uresl(cur_s, s_sql, 1000)           # source table cols <= 1000
        if d_cols_s:
            s_cols = s_ENC
            for l1 in d_cols_s:
                s_cols += l1.get('COL_NAME') + s_ENC + ',' + s_ENC
            s_cols = s_cols[:-2]
            if '{' in s_col_def:
                s_col_b = s_col_def.split('{')[0]
                s_col_b = re.sub('\(.*?\)', '', s_col_b)
                s_col_b = re.sub('\[.*?\]', '', s_col_b)
                s_col_e = s_col_def.split('}')[1]
                s_col_e = re.sub('\(.*?\)', '', s_col_e)
                s_col_e = re.sub('\[.*?\]', '', s_col_e)
            else:
                s_col_b = ''
                s_col_e = ''
            s_cols = s_col_b + s_cols + s_col_e
            d_val['col_s'] = s_cols
        else:
            d_val['result']  = 1
            d_val['memo']  = 'Source Table %s Not Exist.' % ustr.ure(d_para['tab'])
            # pet_unit.etl_e(d_db, d_val, 1)
            return d_val

    ustr.uout("etl_ins : Source Table Column List = %s" % (s_cols), 5)


    ## condition
    s_wh_s  = d_para.get('condition', '')
    if s_wh_s and (s_wh_s.strip(' ').split(' ')[0].lower() != 'limit') and (s_wh_s.strip(' ').split(' ')[0].lower() != 'where'):
        s_wh_s = ' where ' + s_wh_s.strip(' ')

    s_sql_s = 'select %s from %s %s' % (s_cols, d_para.get('owner_s')+'.'+d_para.get('table_s'), s_wh_s)




    ## Get Target - Insert
    d_vp = {}
    d_vp['table_t']  = d_para.get('table_t')
    field_t = d_para.get('field_t').strip(' ')
    if field_t:
        field_t = '( %s )' % field_t
    d_vp['field_t']  = field_t
    d_vp['val']  = ('(%s)') % udb.ifss( d_para.get('type_t'), s_cols.count(',')+1 )
    s_sql_t = exec_sql('sql_insert', d_vp)[1]
    s_sql_t_n = s_sql_t.replace(':s', "N'%s'")                                 # M05 : values :s --> N'%s'

    s_sql_t = s_sql_t.replace('()', '')
    #s_sql_t = "INSERT INTO log.l_etl1(DATE_ID,TIME_ID,PROC_NAME,PROC_ID,PROC_DESC,TIME_B,TIME_E,SECS,COUNTS,SOURCE_TABLE,TARGET_TABLE,RESULT,OP_INFO,MEMO) VALUES "
    s_sql_t_n = s_sql_t_n.replace('()', '')



    ustr.uout("etl_ins : Source SQL : %s" % s_sql_s, 5)
    ustr.uout("etl_ins : Target SQL : %s" % s_sql_t, 5)
    ustr.uout("etl_ins : Target SQL-N : %s" % s_sql_t_n, 5)

    ## M01 : Source Cols VS Target Cols
    ##tmp_1 = len(s_sql_s.split(',')) -1

    # '0000-00-00 00:00:00' MySQL(ustr.valconv)
    bn = ustr.str2num(d_para.get('bn'))                    # bulk num
    if (not bn) or (bn <= 0):
        bn = C_BN
    step = ustr.str2num(d_para.get('step'))
    if (not step) or (step <= 0):
        step = C_STEP
    ustr.uout("etl_ins : Bulk Num : %s" % bn, 5)
    ustr.uout("etl_ins : Step Num : %s" % step, 5)
    n_count = 0
    i  = 0
    if bn > 1:
        ustr.uout("etl_ins : Insert Table With Bulk. Bulk Num=%s" % bn, 3)
        try:
            l_rs_bulk = []
            cur_s.execute(s_sql_s)
            cols_len_s = len(cur_s.description)

            for rs1 in cur_s:
                i += 1
                l_rs_bulk.append(tuple(ustr.valconv(rs1, 1)))
                if i % bn == 0:
                    try:
                        cur_t.executemany(s_sql_t, l_rs_bulk)
                    except Exception as e:
                        _rs = etl_ins_many_err(cur_t, s_sql_t, l_rs_bulk)
                        if _rs.get('result', 0) > 0:
                            raise Exception(_rs.get('memo', ''), _rs.get('result', 0))
                    l_rs_bulk = []
            if l_rs_bulk:
                try:
                    cur_t.executemany(s_sql_t, l_rs_bulk)
                except Exception as e:
                    ustr.uout("Error: %s" % str(e), 9)
                    _rs = etl_ins_many_err(cur_t, s_sql_t, l_rs_bulk)
                    if _rs.get('result', 0) > 0:
                        raise Exception(_rs.get('memo', ''), _rs.get('result', 0))

            cur_t.execute('commit')
        except Exception as e:
            d_val['result']  = 1
            d_val['message'] = 'Error - ETL.'
            d_val['memo']    = ustr.ure(str(e))
            return d_val
    else:
        ustr.uout("etl_ins : Insert Table With Not Bulk.", 3)
        i = 0
        try:
            cur_s.execute(s_sql_s)
            cols_len_s = len(cur_s.description)

            for rs1 in cur_s:
                i = i + 1
                try:
                    cur_t.execute(s_sql_t_n % rs1)                             # M05 : values :s --> N'%s'
                except Exception as e:
                    ustr.uout("Error: %s" % str(e), 9)
                    if i < 10:
                        ustr.uout("Error - Code - S: %s - %s" % (i, rs1), 9)
                    rs1 = ustr.valconv(rs1, 1)
                    if i < 10:
                        ustr.uout("Error - Code - T: %s - %s" % (i, rs1), 9)
                    cur_t.execute(s_sql_t, rs1)

                if i%step == 0:
                    try:
                        cur_t.execute('commit')
                    except Exception as e:
                        print(e)
                        pass

            try:
                cur_t.execute('commit')
            except Exception as e:
                print(e)
                pass
        except Exception as e:
            d_val['result']   = 1
            d_val['message']  = 'Error - Insert Data. offset : %s' % str(i)
            d_val['message2'] = 'Error - Insert Data. data : %s' % str(rs1)
            d_val['memo']     = ustr.ure(str(e))
            return d_val

    d_val['rs'] = i
    d_val['cs'] = i

    # Finished task in s_todolist
    d_val['result'] = 0
    d_val['message'] = ''

    return d_val
