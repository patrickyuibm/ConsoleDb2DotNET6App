// See https://aka.ms/new-console-template for more information

//***************************** IMPORT DEPENDENCIES *****************************
using System;
using System.Data;
using System.Data.Odbc;
using System.Threading;
using System.IO;
using System.Net;
using System.Net.NetworkInformation;
using System.Runtime.InteropServices;
using IBM.Data.Db2;
using System.Configuration;
using System.Collections.Specialized;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.Xml;
using IBM.Data.DB2Types;
using System.Text;

namespace ConsoleDb2DotNET6App 
{
  class ConsoleDb2DotNET6App {
    
    String[] select_statements =  {"SELECT MAX(T1.P_SIZE) FROM TPCHSC01.PART T1, TPCHSC05.SUPPLIER T2",
                                   "SELECT * FROM DB2ADM.TB2"}; //the first statement is deliberately computationally expensive
    String[] insert_statements =  {"INSERT INTO DB2ADM.TB2(C1, C2) VALUES(1, 2)"};
    String[] update_statements =  {"UPDATE DB2ADM.TB2 SET C2 = RAND()*20000 WHERE C1 < RAND()*20000", 
                                   "UPDATE DB2ADM.TB2 SET C1 = RAND()*5000 WHERE C2 > RAND()*5000"};
    String[] delete_statements = {"DELETE FROM DB2ADM.TB2 WHERE C2 > 8000 AND C1 > 3000", 
                                  "DELETE FROM DB2ADM.TB2 WHERE C1 > 12000", 
                                  "DELETE FROM DB2ADM.TB2 WHERE C2 > 12000",
                                  "DELETE FROM DB2ADM.TB2 WHERE C2 > 8000 AND C1 < 3000"};

    static Dictionary<string, string> DSConfigs_properties;
    static Dictionary<string, string> WrkloadConfigs_properties;
    static Dictionary<string, string> Test_properties;
    static String connString;
    static int logLevel = 1; //only supports 1 and 2 for now
    static String log;
    static String logDir;
    static String logFile;
    static StreamWriter m_log;
    
    public static void Main(String[] args) {
      ConsoleDb2DotNET6App cdb = new ConsoleDb2DotNET6App(); 
      
      DSConfigs_properties = cdb.getProperties("/etc/dsconfigs/DSConfigs_properties.txt");
      WrkloadConfigs_properties = cdb.getProperties("/etc/wrkloadconfigs/WrkloadConfigs_properties.txt");
      Test_properties = cdb.getProperties("/etc/testprop/Test_properties.txt"); 
      logLevel = int.Parse(WrkloadConfigs_properties["LOG_LEVEL"]);
      connString = cdb.connectDb();
      DB2Connection conn = new DB2Connection(connString); 
      conn.Open();
      Console.WriteLine(conn.ServerVersion); 
      conn.Close();
      
      logDir = "/etc/logs";
      logFile = logDir + "/run" + "-" + DateTime.Now.Year.ToString() + "-" +
                              DateTime.Now.Month.ToString() + "-" +
                              DateTime.Now.Day.ToString() + "-" +
                              DateTime.Now.Hour.ToString() + "-" +
                              DateTime.Now.Minute.ToString() +
                              ".txt";
      m_log = new StreamWriter(logFile);
    
      int numInsertThreads = int.Parse(WrkloadConfigs_properties["COUNT"]);
      Thread[] myThreads = new Thread[numInsertThreads];
      for (int i = 0; i < numInsertThreads; i++) {
        Thread t = new Thread(new ThreadStart(() => cdb.startSelect()));
        t.Start();
        myThreads[i] = t;
      }
      foreach (Thread t in myThreads) {
        t.Join();
      }
      
    }
    
    void startSelect() {
      int thid = System.Threading.Thread.CurrentThread.ManagedThreadId;  
      string connString = connectDb() + ";ClientApplicationName="+thid.ToString();
      DB2Connection conn = new DB2Connection(connString);
      conn.Open();
      try {  
          run_transaction(conn);
      }  catch (DB2Exception myException) { 
          for (int i=0; i < myException.Errors.Count; i++) { 
             m_log.WriteLine("For Thread_" + thid.ToString() + ": \n" + 
                 "Message: " + myException.Errors[i].Message + "\n" + 
                 "Native: " + myException.Errors[i].NativeError.ToString() + "\n" + 
                 "Source: " + myException.Errors[i].Source + "\n" + 
                 "SQL: " + myException.Errors[i].SQLState + "\n" + 
                 "At time: " + DateTime.Now);
          }
      } finally {
          conn.Close();
      }
    }
    
    void run_transaction(DB2Connection myConnection) {
       float thread_timespan = float.Parse(Test_properties["THREAD_MINUTES_TIMESPAN"]); 
       float commit_frequency = float.Parse(Test_properties["COMMIT_FREQUENCY"]);
       int repetitions = (int) (thread_timespan / commit_frequency);
      
       DB2Command myCommand = new DB2Command(); 
       myCommand.Connection = myConnection;  
       myCommand.CommandText = select_statements[0]; 
       DB2Transaction myTrans = myConnection.BeginTransaction(IsolationLevel.ReadCommitted);
       myCommand.Transaction = myTrans;
      
       try { 
          Stopwatch s = new Stopwatch();  
          for (int i = 0; i < repetitions; i++) {
            s.Start();
            //m_log("Running DML at {0}", DateTime.Now);
            while (s.Elapsed < TimeSpan.FromMinutes(commit_frequency)) {  
              myCommand.ExecuteNonQuery();
            }
            //m_log("Resetting stopwatch and committing DML at time {0}", DateTime.Now);
            myTrans.Commit();
            myTrans = myConnection.BeginTransaction(IsolationLevel.ReadCommitted);
            myCommand.Transaction = myTrans;
            s.Reset();
          }
          s.Stop();
       } catch(Exception e) { 
         myTrans.Rollback(); 
         m_log(e.ToString()); 
       } finally { 
         myConnection.Close(); 
       } 
    } 
    
    Dictionary<string, string> getProperties(String full_path) {
      Dictionary<string, string> props = new Dictionary<string, string>();
      try {
        using (StreamReader sr = new StreamReader(full_path)) {
            String line;
            while ((line = sr.ReadLine()) != null) {
              int equalSignIndex = line.IndexOf("=");
              props.Add(line.Substring(0, equalSignIndex), line.Substring(equalSignIndex + 1));
            }
          }
      } catch (Exception e) {
          m_log("The file could not be read:");
          m_log(e.Message);
        }
      return props;
    }
    
    String connectDb() {
      DB2ConnectionStringBuilder connb = new DB2ConnectionStringBuilder();
      
      //Server credentials
      connb.Database = DSConfigs_properties["DS_DATABASE_NAME"];
      connb.UserID = DSConfigs_properties["DS_USER"];
      connb.Password = DSConfigs_properties["DS_PASSWORD"];
      if (DSConfigs_properties["DS_ENABLE_SSL"].ToLower().Contains("t")) {
        connb.Server = DSConfigs_properties["DS_SSL_SERVER"];
        connb.Security = "SSL";
        connb.SSLClientKeystash = "/etc/stash/zosclientdb.sth";
        connb.SSLClientKeystoredb = "/etc/keystore/zosclientdb.kdb";
        //connb.SSLClientLabel = "clientcert";
        //connb.SSLClientKeystoreDBPassword = "PASS";
      } else {
        connb.Server = DSConfigs_properties["DS_SERVER_NAME"];
      }
      
      //Pooling
      connb.Pooling = true;
      connb.MinPoolSize = 0;
      connb.MaxPoolSize = int.Parse(WrkloadConfigs_properties["COUNT"]);
    
      //Timeout management
      //connb.Connect_Timeout = 60;
      connb.ConnectionLifeTime = int.Parse(Test_properties["CONN_LIFETIME"]);
      
      return connb.ConnectionString;
    }
    
    void run_select_queries(DB2Connection conn) {
      Random rnd = new Random();
      int index = rnd.Next(0, select_statements.Length);
      DB2Command cmd1 = new DB2Command(select_statements[index], conn);
      DB2DataReader dr1 = cmd1.ExecuteReader();
      dr1.Close();
    }
    
    void run_update_queries(DB2Connection conn) {
      Random rnd = new Random();
      int index = rnd.Next(0, update_statements.Length);
      DB2Command cmd1 = new DB2Command(update_statements[index], conn);
      DB2DataReader dr1 = cmd1.ExecuteReader();
      dr1.Close();
    }
    
    void run_insert_queries(DB2Connection conn) {
      Random rnd = new Random();
      int index = rnd.Next(0, insert_statements.Length);
      DB2Command cmd1 = new DB2Command(insert_statements[index], conn);
      DB2DataReader dr1 = cmd1.ExecuteReader();
      dr1.Close();
    }
    
    void run_delete_queries(DB2Connection conn) {
      Random rnd = new Random();
      int index = rnd.Next(0, delete_statements.Length);
      DB2Command cmd1 = new DB2Command(delete_statements[index], conn);
      DB2DataReader dr1 = cmd1.ExecuteReader();
      dr1.Close();
    }
    
    void run_Cursor_WH_SP(DB2Connection conn) {
      DB2Command cmd = conn.CreateCommand();
      String spname = "DB2ADM.CURSOR_WH_TB2";
      String procCall = "CALL " + spname;
      cmd.CommandText = procCall;
      DB2DataReader myReader = cmd.ExecuteReader(); 
      myReader.Close(); 
    }
    
  }
}
