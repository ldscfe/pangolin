/*
******************************************************************************
  Name     : expo
  Purpose  : Export Data From Oracle
  Author   : Adam

  Revisions:
   Ver        Date        Author           Description
   --------  --------  ---------------  ------------------------------------
   1.0       20220929  Adam             Create
   1.2       20221008  Adam             M01: Add data is NULL, export value from user define.
   1.21      20221010  Adam             M02: Fix: Unsupported column data type --> Segmentation fault (core dumped)

format:
  expo ds=bidb s="select * from test1" header=1 datenull='1970-01-01 00:00:00' intnull=NULL > test1.csv 2>test1.err
  ds                source file (from db.ini)
  s                 source file (regular expression: *)
  pr                [2] passwd PRE
  header            [0] not export column name
  datenull          [] if source data's date colunm is NULL, set datenull
  intnull           [] if source data's numrice colunm is NULL, set intnull

  -- s Variable:
     yesterday         yyyymmdd, now-1
     now-N             yyyymmdd
  -- supported column data type
     (1)  : VARCHAR2/VARCHAR/NVARCHAR/NVARCHAR2
            CHAR(96)/RAW(23)
     (2)  : NUMBER/FLOAT
     (12) : DATE(12)
     (112): CLOB
  -- Unsupported column data type
     TIMESTAMP(0)/TIMESTAMP(6)/TIMESTAMP(7)/TIMESTAMP(9)
  -- Stream buffer size can't be > 1 in this case
     BLOB、LONG
******************************************************************************
*/
#include <iostream>

#include <string.h>

#include <ldurstr.h>
#include <ldurini.h>
#include <ldurtime.h>
#include <ldurinfo.h>
#include <ldurmap.h>
#include <db/ldurora.h>
#include <hash/ldurcrypt.h>                           //lddr::decrypt

using namespace std;

//from ustr
using ldur::lower;
using ldur::strRep;
using ldur::strInstr;
using ldur::strInstrRemain;
using ldur::num2str;
//from map
using ldur::LDURMAP;

//from ini
using ldur::LDURINI;

//from sql_*
using ldur::SQLORA;

//from crypt
using ldur::decrypt;
//from info
using ldur::pid;
using ldur::udebug;
//from time
using ldur::getTime;
using ldur::updateTimeVar;

LDURMAP ump;                            //db info, etl info

// datetime --> string: yyyy-mm-dd hh24:mi:ss
string s_dt(otl_datetime od1)
{
   string res;
   char charTmp[32];
   if (od1.year > 0) {
     sprintf(charTmp,"%02d-%02d-%02d %02d:%02d:%02d",od1.year,od1.month,od1.day,od1.hour,od1.minute,od1.second);
     res = charTmp;
   }
   else
      res = "";
   
   return res;
}

// string --> csv format
// return the converted string, the original string is also changed.
string s_csv(string& str1, const string& delimiter=",")
{
   if ( strstr(str1.c_str(), "\"") || strstr(str1.c_str(), "\r") || strstr(str1.c_str(), "\n")  || strstr(str1.c_str(), delimiter.c_str()) ) {
      str1 = strRep(str1, "\"", "\"\"");
      str1 = "\"" + str1 + "\"";
   }
   return str1;
}

//Get Base Info From db.ini
int get_db_info(const string& file, const string& ini_group, const int pre=2)
{
   LDURINI uini1;
   uini1.set(file);

   //ump.init();
   ump.set("t_type", lower(uini1.get(ini_group, "type")));
   ump.set("t_host", uini1.get(ini_group, "host"));
   ump.set("t_user", uini1.get(ini_group, "user"));
   ump.set("t_passwd", ldur::decrypt(uini1.get(ini_group, "passwd")).substr(pre));
   ump.set("t_port", uini1.get(ini_group, "port"));
   ump.set("t_db", uini1.get(ini_group, "db"));
   ump.set("memo", "");

   return 0;
}

int main( int argc, const char* argv[] )
{

   const int LineMax=2000000000;
   const int CharMax=32000;
   const int LobMax=1000000;

   int N;                   // output line max limit
   int PR;                  // passwd PRE
   // if NULL, set default value
   string dateNULL;
   string intNULL;

   int i,j;

   // tmp variable
   string strTmp;
   char charTmp[CharMax];

   //Get Now-N Time, yyyymmdd

   //Get variable: key-value
   for (i=0; i<argc-1; i++)
   {
      strTmp = argv[i+1];
      ump.set(strInstr(strTmp, '=', 1), strInstrRemain(strTmp, '=', 1));
   }

   string s_ds = ump.get("ds");
   string s_sql = ump.get("s");
   s_sql = updateTimeVar(s_sql);

   dateNULL = ump.get("datenull");
   intNULL = ump.get("intnull");
   //cout << "NULL: " << dateNULL << "," << intNULL << endl;

   strTmp = ump.get("n");
   if ( strTmp == "" )
      N = LineMax;
   else
      N = atoi(strTmp.c_str());
   //cout << "Max Limit: " << N << endl;

   strTmp = ump.get("pr");
   if ( strTmp == "" )
      PR = 2;
   else
      PR = atoi(strTmp.c_str());
   //cout << "Max Limit: " << N << endl;

   //out header, yes/on/1 = 1
   string s_header = ump.get("header");
   if ( strstr(s_header.c_str(), "yes") || strstr(s_header.c_str(), "on") || strstr(s_header.c_str(), "1") )
      s_header = "1";

   SQLORA dbin;


   // INI --> Base DB Info
   i = get_db_info( "db.ini", s_ds, PR);    //--> b_:ype, host, port, user, passwd, db, code

// debug
/*
ump.begin();
for ( i=0; i<(int)ump.size; i++ )
{
   strTmp = ump.get();
   cout << "ETL Info(key-value) - " << ump.key(strTmp) << ": " << strTmp << endl;
}
*/

   otl_column_desc* COL;
   int COLS;
   try
   {
      dbin.conn(ump.get("t_host"), ump.get("t_port"), ump.get("t_user"), ump.get("t_passwd"), ump.get("t_db"));
      //cout << "DB Status: " << dbin.status << endl;

      dbin.dbConn.set_max_long_size(LobMax); // set maximum long string size for connect object
      //dbin.rs.set_all_column_types(otl_all_num2str | otl_all_date2str);
      dbin.rs.set_all_column_types(otl_all_num2str);

      i = dbin.query(s_sql);
      //cout << "DB Query: " << s_sql << endl;
      //cout << "DB Query Status: " << i << endl;
      //M02: Fix: Unsupported column data type --> Segmentation fault (core dumped)
      if (i < 0) {
         cerr << "#### ERR: DB Query ============" << endl;
         cerr << "SQL:  " << s_sql << endl;
         cerr << "Query Status: " << i <<endl;
         ump.set("memo", "ORA-00000: DB Query Error.");
         return -1;
      }

      COL = dbin.rs.describe_select(COLS);
      //cout << "DB Describe: " << COL << endl;

// debug
/*
// column info
cout << "name, type, otl_var_dbtype, dbsize, scale, prec, nullok" << endl;
for (i=0;i<COLS;i++)
{
   cout << COL[i].name << ", " << COL[i].dbtype << ", " << COL[i].otl_var_dbtype << ", " << COL[i].dbsize << ", " << COL[i].scale << ", " << COL[i].prec << ", " << COL[i].nullok << endl;
}
*/
      otl_datetime dtTmp;                // define date, timestamp
      otl_long_string lsTmp(LobMax);     // define long string variable
      int n_res;

      //output CSV
      // header
      if ( s_header == "1" ) {
         for( i=0; i<COLS-1; i++ ) {
            cout << COL[i].name << ",";
         }
         cout << COL[i].name << endl;
      }

      // data
      n_res = 0;
      string s_endl = "";
      while ( ! dbin.rs.eof() && n_res < N )
      {
         s_endl = ",";
         for( i=0; i<COLS-1; i++ ) {
            switch(COL[i].dbtype) {
               case 2:    //numeric
                  dbin.rs >> charTmp;
                  if (dbin.rs.is_null())
                     cout << intNULL << s_endl;
                  else
                     cout << charTmp << s_endl;
                  break;
               case 12:   //data, timestamp
                  dbin.rs >> dtTmp;
                  strTmp = s_dt(dtTmp);
                  if (strTmp == "")
                     cout << dateNULL << s_endl;
                  else
                     cout << strTmp << s_endl;
                  break;
               case 112:  //clob
                  dbin.rs >> lsTmp;
                  strTmp = "";
                  for (j=0;j<lsTmp.len();j++)
                     strTmp += lsTmp[j];
                  cout << s_csv(strTmp) << s_endl;
                  break;
               default:   // varchar
                  dbin.rs >> charTmp;
                  strTmp = charTmp;
                  cout << s_csv(strTmp) << s_endl;
                  break;
            }
         }

         s_endl = '\n';
         // last line
            switch(COL[i].dbtype) {
               case 2:    //numeric
                  dbin.rs >> charTmp;
                  if (dbin.rs.is_null())
                     cout << intNULL << s_endl;
                  else
                     cout << charTmp << s_endl;
                  break;
               case 12:   //data, timestamp
                  dbin.rs >> dtTmp;
                  strTmp = s_dt(dtTmp);
                  if (strTmp == "")
                     cout << dateNULL << s_endl;
                  else
                     cout << strTmp << s_endl;
                  break;
               case 112:  //clob
                  dbin.rs >> lsTmp;
                  strTmp = "";
                  for (j=0;j<lsTmp.len();j++)
                     strTmp += lsTmp[j];
                  cout << s_csv(strTmp) << s_endl;
                  break;
               default:   // varchar
                  dbin.rs >> charTmp;
                  strTmp = charTmp;
                  cout << s_csv(strTmp) << s_endl;
                  break;
            }
         n_res++;
      }
   }
   catch(otl_exception& p) {
      cerr << "#### ERR: Get SQL Value ============" << endl;
      cerr << "Current Time: " << getTime() << endl;
      cerr << "host = " << ump.get("t_host") <<endl;
      cerr << "user = " << ump.get("t_user") <<endl;
      //cerr << "passwd = " << ump.get("t_passwd") <<endl;
      cerr << "db = " << ump.get("t_db") <<endl;
      cerr << "sql = " << ump.get("t_sql") <<endl;
      cerr << p.msg <<endl;
      //cerr << p.stm_text <<endl;
      cerr << p.var_info <<endl;
      string msg = p.var_info;
      msg = "ORA-00000:" + msg;
      ump.set("memo", msg);
      return -1;
   }

   return 0;
}


