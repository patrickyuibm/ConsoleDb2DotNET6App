// See https://aka.ms/new-console-template for more information

//***************************** IMPORT DEPENDENCIES *****************************
using System;
using System.Data;
using System.Threading;
using System.IO;
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
Dictionary<string, string> connectionDict = new Dictionary<string, string>();
connectionDict.Add("uid", Environment.GetEnvironmentVariable("uid"));
connectionDict.Add("pwd", Environment.GetEnvironmentVariable("pwd"));
connectionDict.Add("server", Environment.GetEnvironmentVariable("server"));
connectionDict.Add("db", Environment.GetEnvironmentVariable("db"));
connectionDict.Add("sslserver", Environment.GetEnvironmentVariable("sslserver"));


String[] select_statements =  {"SELECT * FROM DB2ADM.TB2", 
                               "SELECT * FROM DB2ADM.TB2 WHERE C1 > RAND()*5000",
                               "SELECT * FROM DB2ADM.TB2 WHERE C1 > RAND()*10000",
                               "SELECT * FROM DB2ADM.TB2 WHERE C2 > RAND()*5000",
                               "SELECT * FROM DB2ADM.TB2 WHERE C2 > RAND()*10000"};
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

//***************************** METHODS *****************************

void main() {
  System.Diagnostics.Stopwatch watch = new System.Diagnostics.Stopwatch();
  watch.Start();
  int numInsertThreads = 100;
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
  string elapsedTime = String.Format("{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds,ts.Milliseconds / 10);
  Console.WriteLine("All threads complete"); 
  Console.WriteLine("Time elapsed (minutes:seconds:milliseconds): " + elapsedTime);
  Console.WriteLine("Number of selects ran: " + selects.ToString());
  Console.WriteLine("Number of deletes ran: " + deletes.ToString());
  Console.WriteLine("Number of inserts ran: " + inserts.ToString());
  Console.WriteLine("Number of updates ran: " + updates.ToString());
  Console.WriteLine("Number of rows affected: " + total_records_affected.ToString());
}

void startSelect() {
  int thid = System.Threading.Thread.CurrentThread.ManagedThreadId;
  DB2Connection conn = connectDb(thid);
  Console.WriteLine(conn.ConnectionString);
  conn.Open();
  try { 
      DB2Command cmd1 = new DB2Command("SELECT MAX(T1.P_SIZE) FROM TPCHSC01.PART T1, TPCHSC05.SUPPLIER T2", conn);
      for (int i = 0; i < 100; i++) {
        DB2DataReader dr1 = cmd1.ExecuteReader();
        dr1.Close();
        //run_select_queries(conn);
        //run_insert_queries(conn);
        //run_delete_queries(conn);
      }

      /*
      //Check if pooling was successful 
      if (!conn.IsConnectionFromPool) { 
        Console.WriteLine("Pooling failed for Thread_" + System.Threading.Thread.CurrentThread.ManagedThreadId.ToString()); 
      } else {
        Console.WriteLine("Pooling successful for Thread_" + System.Threading.Thread.CurrentThread.ManagedThreadId.ToString());
      }
      
      Random rnd = new Random();
      int iterations = rnd.Next(1,4);
      for (int i = 0; i < iterations; i++) {
        Random r = new Random();
        int val = r.Next(1,36); //Frequency ratio: 20 selects : 9 inserts : 5 updates : 1 delete 
        if (val < 2) {
          log += thname + " running; action: delete; random value = " + val.ToString();
          deletes += 1;
          run_delete_queries(conn);
        } else if (val < 7) {
          log += thname + " running; action: update; random value = " + val.ToString();
          updates += 1;
          run_update_queries(conn);
        } else if (val < 16) {
          log += thname + " running; action: insert; random value = " + val.ToString();
          inserts += 1;
          run_insert_queries(conn);
        } else {
          log += thname + " running; action: select; random value = " + val.ToString();
          selects += 1;
          run_select_queries(conn);
        }
      }
      */
  } catch (DB2Exception myException) { 
      for (int i=0; i < myException.Errors.Count; i++) { 
         Console.WriteLine("For Thread_" + thid.ToString() + ": \n" + 
             "Message: " + myException.Errors[i].Message + "\n" + 
             "Native: " + myException.Errors[i].NativeError.ToString() + "\n" + 
             "Source: " + myException.Errors[i].Source + "\n" + 
             "SQL: " + myException.Errors[i].SQLState + "\n");
       } 
   } finally { 
      conn.Close();
    }
}

DB2Connection connectDb(int threadID) {
  DB2ConnectionStringBuilder connb = new DB2ConnectionStringBuilder();
  
  //Name the thread
  connb.ClientApplicationName = "Thread_" + threadID.ToString();

  //Server credentials
  connb.Database = connectionDict["db"];
  connb.UserID = connectionDict["uid"];
  connb.Password = connectionDict["pwd"];
  connb.Server = connectionDict["sslserver"];
  
  //Pooling
  connb.Pooling = true;
  connb.MinPoolSize = 0;
  connb.MaxPoolSize = 1000;
  
  //SSL
  connb.Security = "SSL";
  connb.SSLClientKeystash = "/etc/stash/zosclientdb.sth";
  connb.SSLClientKeystoredb = "/etc/keystore/zosclientdb.kdb";
  //connb.SSLClientLabel = "clientcert";
  //connb.SSLClientKeystoreDBPassword = "PASS";

  DB2Connection conn = new DB2Connection(connb.ConnectionString);
  Console.WriteLine(conn.ConnectionString);
  conn.Open();
  Console.WriteLine("Connection successful");
  conn.Close();
  return conn;
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
//Method to set up threads that run several stored procedures
connectDb(1);










