import java.sql.*;

public class test {

    public static void main(String[] args) {
        Connection conn=null;
        long streamOffsetScn=0;
        long streamOffsetendScn=0;
        //Statement stat=null;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            conn= DriverManager.getConnection("jdbc:oracle:thin:@127.0.0.1:1521:oracletest","logminer","logminer");
            String logMinerStartScr="begin \nSYS.DBMS_LOGMNR.START_LOGMNR(STARTSCN => ?,ENDSCN => ?,";//这里不写endscn会卡死.？
            logMinerStartScr=logMinerStartScr+"OPTIONS =>  SYS.DBMS_LOGMNR.SKIP_CORRUPTION+SYS.DBMS_LOGMNR.NO_SQL_DELIMITER+SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT+SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + SYS.DBMS_LOGMNR.CONTINUOUS_MINE+SYS.DBMS_LOGMNR.COMMITTED_DATA_ONLY+SYS.DBMS_LOGMNR.STRING_LITERALS_IN_STMT"+") \n; end;";
           // stat=conn.createStatement();
           // if (streamOffsetScn==0L){
                //skipRecord=false;
            System.out.println(logMinerStartScr);
                PreparedStatement currentSCNStmt=conn.prepareCall("select checkpoint_change# from v$database");
                PreparedStatement currentSCNend=conn.prepareCall("select current_scn from v$database");
                ResultSet currentScnResultSet=currentSCNStmt.executeQuery();
                ResultSet endscnRS=currentSCNend.executeQuery();
                while(currentScnResultSet.next()){
                    streamOffsetScn=currentScnResultSet.getLong(1);
                    System.out.println(streamOffsetScn);
                }
                while(endscnRS.next()){
                     streamOffsetendScn=endscnRS.getLong(1);
                     System.out.println(streamOffsetendScn);
                 }
                currentScnResultSet.close();
                endscnRS.close();
                currentSCNend.close();
                currentSCNStmt.close();
               // log.info("Getting first scn from current log {}",streamOffsetScn);
          //  }

            CallableStatement logMinerStartStmt=conn.prepareCall(logMinerStartScr);
            logMinerStartStmt.setLong(1, streamOffsetScn);
            logMinerStartStmt.setLong(2,streamOffsetendScn);
            logMinerStartStmt.execute();
            PreparedStatement logMinerSelect=conn.prepareCall("select sql_redo from v$logmnr_contents where seg_name='EMP'");

            ResultSet logMinerData=logMinerSelect.executeQuery();
           // log.info("Logminer started successfully");
            //ResultSet rs=stat.executeQuery(sql);
            while(logMinerData.next()){
                String redo=logMinerData.getString(1);
                System.out.println(redo);
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {

            if (conn!=null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

