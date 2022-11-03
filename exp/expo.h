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
