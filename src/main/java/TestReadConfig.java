import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class TestReadConfig {
    public static String DB_Host = "192.168.211.111";
    public static String DB_User = "root";
    public static String DB_Password = "qwer1234";
    public static String DB_Schema = "schema_test";
    public static String DB_Table = "Students";

    public static int Thead_Count=100;
    public static int Connect_Count=100;
    public static int Loop_Count=0;
    public static int Multi_Insert_Count=10;
    public static int Multi_Count=100;
    public static int Is_Col72 = 0;
    public static int Insert_Type=2;
    public static String Sql_Str=null;
    public static String Sql_Str2=null;
    public static HashMap<String,String> map=new HashMap<>();
    public static String JdbcUrl=null;
    public static String Str1=null;
    public static String Str2=null;
    public static String Str3=null;
    public static String Sql_Str3=null;
    public static String Batch_pstmt=null;
    public static String AddInFile=null;
    // File read
    public static void readConfFileByLines(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                if(tempString.startsWith("#")) { continue; }
                String[] rdStrs = tempString.split("=");
                if(rdStrs.length == 2){
                    if("Thead_Count".equals(rdStrs[0])){
                        Thead_Count = Integer.parseInt(rdStrs[1]);
                    }else if ("Connect_Count".equals(rdStrs[0])){
                        Connect_Count = Integer.parseInt(rdStrs[1]);
                    }else if ("Loop_Count".equals(rdStrs[0])){
                        Loop_Count = Integer.parseInt(rdStrs[1]);
                    } else if ("Multi_Insert_Count".equals(rdStrs[0])){
                        Multi_Insert_Count = Integer.parseInt(rdStrs[1]);
                    } else if ("Multi_Count".equals(rdStrs[0])){
                        Multi_Count = Integer.parseInt(rdStrs[1]);
                    } else if ("Sql_Str".equals(rdStrs[0])){
                        Sql_Str = rdStrs[1];
                    }else if ("Sql_Str2".equals(rdStrs[0])){
                        Sql_Str2 = rdStrs[1];
                    }else if ("Sql_Str3".equals(rdStrs[0])){
                        Sql_Str3 = rdStrs[1];
                    }else if ("Str1".equals(rdStrs[0])){
                        Str1 = rdStrs[1];
                    }else if ("Str2".equals(rdStrs[0])){
                        Str2 = rdStrs[1];
                    }else if ("Str3".equals(rdStrs[0])){
                        Str3 = rdStrs[1];
                    }else if ("DB_Host".equals(rdStrs[0])){
                        DB_Host = rdStrs[1];
                    }else if ("DB_User".equals(rdStrs[0])){
                        DB_User = rdStrs[1];
                    }else if ("DB_Password".equals(rdStrs[0])){
                        DB_Password = rdStrs[1];
                    }else if ("DB_Schema".equals(rdStrs[0])){
                        DB_Schema = rdStrs[1];
                    }else if ("DB_Table".equals(rdStrs[0])){
                        DB_Table = rdStrs[1];
                    }else if ("Is_Col72".equals(rdStrs[0])){
                        Is_Col72 = Integer.parseInt(rdStrs[1]);
                    }else if ("Insert_Type".equals(rdStrs[0])) {
                        Insert_Type = Integer.parseInt(rdStrs[1]);
                    }else if("JdbcUrl".equals(rdStrs[0])){
                        JdbcUrl=rdStrs[1];
                    }else if("Batch_pstmt".equals(rdStrs[0])){
                        Batch_pstmt=rdStrs[1];
                    }else if("AddInFile".equals(rdStrs[0])){
                        AddInFile=rdStrs[1];
                    }else if (tempString.startsWith("$")){
                        String i=rdStrs[0].substring(2,3);
                        map.put(i,rdStrs[1]);
                    }

                }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }
}
