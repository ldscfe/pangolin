# Pangolin - ETL Tools
##
##    v1.95
##    @2018-2022
##
#### 20210824  Adam        v1.8  - Patch, MySQL Connect.
#### 20210826  LLC         v1.81 - c_kv/s_kv db define ADD [pre=0]
#### 20210831  LLC         v1.82 - DB Connect Error --> Logtion
#### 20210911  LLC         v1.83 - lib/etl_unit Patch --> pre_condi
#### 20211212  Adam        v1.85 - lib/etl_unit Patch, Support utf8 special characters, bn=1
#### 20220920  Adam        v1.86 - pet/pet Patch, Add parameter - proc_name, etc...
#### 20220513  LLC         v1.87 - pet/pet Patch, condition - replace date_id, etc...
#### 20220718  Adam        v1.88 - pangolin Modify, ppy --> pet2
#### 20220721  LLC         v1.89 - usql_sys, Add oracle_oracle_ts
#### 20220920  Adam        v1.92 - pup/pup2 Add, Add Load CSV to DB(Greenplum)
#### 20221011  Adam        v1.95 - pup/pup2 Add, Add s,t Variable: yesterday, now-N
####


# PET
##### 1. Run s_todolist Task
##### 2. Create target table if it not exist
##### 3. The amount of data processed each time is recommended to be less than 10M.

`pangolin app=pet db=devnh_need_rename`



# PET2(PPY)
##### 1. Direct ETL
##### 2. Create target table if it not exist
##### 3. The amount of data processed each time is recommended to be less than 10M.

`pangolin app=pet2 ds=cbsvp-st s="nhbase.base_app_login_info;{}, 2 REPORT_PARTITION(8), [autopart]" dt=devnh t="ods.z_t_inf_audit_snapshot1"`



# PUP2
##### 1. Direct ETL
##### 2. Created target table
##### 3. The amount of data processed each time is recommended to be less than 1000M.

`./pangolin app=pup2 ds=fcbs71 s="/owdb/%yesterday%*.csv.gz" dt=cbsgp188 t="ods.gio_ads_track_click_2"`



# bulk
`pet.sh devnh 10`
##### pet.sh devnh = pet.sh devnh 1

# Conf
##### Normally, pre_process will clear the data, and delete and truncate have been granted, but alter has not been granted.
#### Oracle :
#####   grant alter any table to bi;

# Table
##### create table s_res
##### Add column field_name_l vc(32000) in c_etl_map
