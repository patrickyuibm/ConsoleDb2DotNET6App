// See https://aka.ms/new-console-template for more information

//***************************** IMPORT DEPENDENCIES *****************************
using System;
using System.Data;
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

//***************************** GLOBAL VARIABLES *****************************
String[] select_statements =  {"SELECT MAX(T1.P_SIZE) FROM TPCHSC01.PART T1, TPCHSC05.SUPPLIER T2",
                               "SELECT * FROM DB2ADM.TB2", 
                               "SELECT * FROM DB2ADM.TB2 WHERE C1 > RAND()*5000",
                               "SELECT * FROM DB2ADM.TB2 WHERE C1 > RAND()*10000",
                               "SELECT * FROM DB2ADM.TB2 WHERE C2 > RAND()*5000",
                               "SELECT * FROM DB2ADM.TB2 WHERE C2 > RAND()*10000"}; //the first statement is deliberately computationally expensive
String[] insert_statements =  {"INSERT INTO DB2ADM.TB2 (C1, C2) VALUES(RAND()*10000,  RAND()*100000)", 
                               "INSERT INTO DB2ADM.TB2 (C1, C2) VALUES(RAND()*100000,  RAND()*10000)",
                               "INSERT INTO DB2ADM.TB2 (C1, C2) VALUES(RAND()*5000,  RAND()*10000)",
                               "INSERT INTO DB2ADM.TB2 (C1, C2) VALUES(RAND()*10000,  RAND()*5000)"};
String[] update_statements =  {"UPDATE DB2ADM.TB2 SET C2 = RAND()*20000 WHERE C1 < RAND()*20000", 
                               "UPDATE DB2ADM.TB2 SET C1 = RAND()*20000 WHERE C2 < RAND()*20000",
                               "UPDATE DB2ADM.TB2 SET C1 = RAND()*5000 WHERE C2 > RAND()*5000",
                               "UPDATE DB2ADM.TB2 SET C1 = RAND()*5000 WHERE C2 > RAND()*5000"};
String[] delete_statements = {"DELETE FROM DB2ADM.TB2 WHERE C2 > 8000 AND C1 > 3000", 
                              "DELETE FROM DB2ADM.TB2 WHERE C1 > 12000", 
                              "DELETE FROM DB2ADM.TB2 WHERE C2 > 12000",
                              "DELETE FROM DB2ADM.TB2 WHERE C1 < 12000", 
                              "DELETE FROM DB2ADM.TB2 WHERE C2 < 12000",
                              "DELETE FROM DB2ADM.TB2 WHERE C2 < 8000 AND C1 < 3000",
                              "DELETE FROM DB2ADM.TB2 WHERE C2 < 8000 AND C1 > 3000",
                              "DELETE FROM DB2ADM.TB2 WHERE C2 > 8000 AND C1 < 3000"};
int selects = 0;
int deletes = 0;
int inserts = 0;
int updates = 0;
int total_records_affected = 0;

Dictionary<string, string> DSConfigs_properties;
Dictionary<string, string> WrkloadConfigs_properties;
Dictionary<string, string> Test_properties;
String connString;

//***************************** METHODS *****************************

void main() {
  DSConfigs_properties = getProperties("/etc/DSConfigs_properties.txt");
  WrkloadConfigs_properties = getProperties("/etc/WrkloadConfigs_properties.txt");
  Test_properties = getProperties("/etc/Test_properties.txt");

  connString = connectDb();
  
  System.Diagnostics.Stopwatch watch = new System.Diagnostics.Stopwatch();
  watch.Start();

  //display network statistics intermittently while threads run, to show active connections on clinet side. 
  System.Diagnostics.Process p = new System.Diagnostics.Process();
  p.StartInfo.WorkingDirectory = "/etc";
  p.StartInfo.FileName = "/etc/DisplayNetStats.sh";
  p.StartInfo.UseShellExecute = true;
  p.Start();

  int numInsertThreads = int.Parse(WrkloadConfigs_properties["COUNT"]);
  Thread[] myThreads = new Thread[numInsertThreads];
  for (int i = 0; i < numInsertThreads; i++) {
    Thread t = new Thread(new ThreadStart(() => startSelect()));
    t.Start();
    myThreads[i] = t;
  }
  foreach (Thread t in myThreads) {
    t.Join();
  }
  
  watch.Stop();
  
  TimeSpan ts = watch.Elapsed;
  // Format and display the TimeSpan value.
  string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds,ts.Milliseconds / 10);
  Console.WriteLine("All threads complete"); 
  Console.WriteLine("Time elapsed (hours:minutes:seconds:milliseconds): " + elapsedTime);
  Console.WriteLine("Number of threads ran: " + numInsertThreads.ToString());
  Console.WriteLine("Number of selects ran: " + selects.ToString());
  Console.WriteLine("Number of deletes ran: " + deletes.ToString());
  Console.WriteLine("Number of inserts ran: " + inserts.ToString());
  Console.WriteLine("Number of updates ran: " + updates.ToString());
  Console.WriteLine("Number of rows affected: " + total_records_affected.ToString());
}

void startSelect() {
  int thid = System.Threading.Thread.CurrentThread.ManagedThreadId;
  int select_statements_index = int.Parse(Test_properties["SELECT_STATEMENT_INDEX"]);
  
  DB2Connection conn = new DB2Connection();
  conn.ConnectionString = connectDb() + ";ClientApplicationName="+thid.ToString();
  conn.Open();
  
  //DB2Transaction transaction;
  //transaction = conn.BeginTransaction();
  try {
     DB2Command cmd1 = new DB2Command(select_statements[select_statements_index], conn);
     DB2DataReader dr1 = cmd1.ExecuteReader();
     dr1.Close();
  } catch (DB2Exception myException) { 
      for (int i=0; i < myException.Errors.Count; i++) { 
         Console.WriteLine("For Thread_" + thid.ToString() + ": \n" + 
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

Dictionary<string, string> getProperties(String full_path) {
  Dictionary<string, string> props = new Dictionary<string, string>();
  try {
    // Create an instance of StreamReader to read from a file.
    // The using statement also closes the StreamReader.
    using (StreamReader sr = new StreamReader(full_path)) {
        String line;
        // Read and display lines from the file until the end of
        // the file is reached.
        while ((line = sr.ReadLine()) != null) {
          int equalSignIndex = line.IndexOf("=");
          props.Add(line.Substring(0, equalSignIndex), line.Substring(equalSignIndex + 1));
        }
      }
  } catch (Exception e) {
      // Let the user know what went wrong.
      Console.WriteLine("The file could not be read:");
      Console.WriteLine(e.Message);
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

void ping(int thid) {
  Ping p1 = new Ping();
  PingReply reply = p1.Send(DSConfigs_properties["DS_SSL_SERVER"].Substring(0, 12));
  if (reply.Status != IPStatus.Success) {
    Console.WriteLine("Pod Name: " + Environment.MachineName + "; Thread: " + thid.ToString() + "; ping failed");
  }
}

void run_select_queries(DB2Connection conn) {
  Random rnd = new Random();
  int index = rnd.Next(0, select_statements.Length);
  DB2Command cmd1 = new DB2Command(select_statements[index], conn);
  DB2DataReader dr1 = cmd1.ExecuteReader();
  total_records_affected += dr1.RecordsAffected;
  dr1.Close();
}

void run_update_queries(DB2Connection conn) {
  Random rnd = new Random();
  int index = rnd.Next(0, update_statements.Length);
  DB2Command cmd1 = new DB2Command(update_statements[index], conn);
  DB2DataReader dr1 = cmd1.ExecuteReader();
  total_records_affected += dr1.RecordsAffected;
  dr1.Close();
}

void run_insert_queries(DB2Connection conn) {
  Random rnd = new Random();
  int index = rnd.Next(0, insert_statements.Length);
  DB2Command cmd1 = new DB2Command(insert_statements[index], conn);
  DB2DataReader dr1 = cmd1.ExecuteReader();
  total_records_affected += dr1.RecordsAffected;
  dr1.Close();
}

void run_delete_queries(DB2Connection conn) {
  Random rnd = new Random();
  int index = rnd.Next(0, delete_statements.Length);
  DB2Command cmd1 = new DB2Command(delete_statements[index], conn);
  DB2DataReader dr1 = cmd1.ExecuteReader();
  total_records_affected += dr1.RecordsAffected * -1;
  dr1.Close();
}

void run_insert_and_select_tb2_SP(DB2Connection conn) {
  //Set up a stored procedure
  DB2Parameter parm = null;
  DB2Transaction trans = conn.BeginTransaction();
  DB2Command cmd = conn.CreateCommand();
  String spname = "DB2ADM.INSERT_AND_SELECT_TB2";
  String procCall = "CALL " + spname + " (@param1, @param2)";
  cmd.Transaction = trans;
  cmd.CommandText = procCall;
  
  // Register input-output and output parameters for the DB2Command
  parm = cmd.Parameters.Add("@param1", DB2Type.Integer);
  parm.Direction = ParameterDirection.Input;
  parm = cmd.Parameters.Add("@param2", DB2Type.Integer);
  parm.Direction = ParameterDirection.Output;
  Random rnd = new Random();
  Random rnd2 = new Random();
  int p1 = rnd.Next(1,99);
  int p2 = rnd2.Next(1,99);
  cmd.Parameters["@param1"].Value = p1;
  cmd.Parameters["@param2"].Value = p2;

  // Call the stored procedure
  //Console.WriteLine("Calling stored procedure " + spname);
  DB2DataReader myReader = cmd.ExecuteReader(); 

  //Retrieve the return code (output parameter in SP)
  int outParm = (int)cmd.Parameters["@param2"].Value;
  //Console.WriteLine("Return code " + outParm.ToString());
  if (outParm != 0) {
    Console.WriteLine("Call failed");
    if (outParm == 99) {
      Console.WriteLine("Table not found");
    }
  } 

  // always call Close when done reading. 
  myReader.Close(); 
}



//***************************** RUN METHODS HERE *****************************
main();










