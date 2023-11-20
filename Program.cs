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

String[] select_statements =  {"SELECT * FROM DB2ADM.TB2", "SELECT * FROM DB2ADM.TB2 WHERE C1 > 3500"};
String[] insert_statements =  {"INSERT INTO DB2ADM.TB2 (C1, C2) VALUES(RAND()*10000,  RAND()*100000)", 
                                      "INSERT INTO DB2ADM.TB2 (C1, C2) VALUES(1, 2)"};
String[] update_statements =  {"UPDATE DB2ADM.TB2 SET C2 = RAND()*20000 WHERE C2 > 40000"};

//***************************** METHODS *****************************
//Method to run stored procedure
/*
Note: currently, since the parameters for each SP are different, one method to run any SP is nearly impossible. We have to know what 
SP we want to run the code it in first. 
*/
void main() {
  int numInsertThreads = 7000;
  Thread[] myThreads = new Thread[numInsertThreads];
  for (int i = 0; i < numInsertThreads; i++) {
    Thread t = new Thread(new ThreadStart(() => startSelect()));
    t.Name = "Thread_" + (i+1).ToString();
    t.Start();
    myThreads[i] = t;
  }
  foreach (Thread t in myThreads) {
    t.Join();
  }
  Console.WriteLine("All threads complete");
}

void startSelect() {
  DB2ConnectionStringBuilder connb = new DB2ConnectionStringBuilder();
  connb.Database = connectionDict["db"];
  connb.UserID = connectionDict["uid"];
  connb.Password = connectionDict["pwd"];
  connb.Server = connectionDict["server"];
  connb.Pooling = true;
  connb.MinPoolSize = 0;
  connb.MaxPoolSize = 10000;
  DB2Connection conn = new DB2Connection(connb.ConnectionString);
  String thname = System.Threading.Thread.CurrentThread.Name;
  conn.Open();
  //Console.WriteLine(thname + " running");
  //run_insert_and_select_tb2_SP(conn);
  run_select_queries(conn);
  if (!conn.IsConnectionFromPool) {
    Console.WriteLine("Error: Pooling failed for " + thname);
  } else {
    Console.WriteLine("Pooling successful for " + thname);
  }
  conn.Close(); 
  //Console.WriteLine(thname + " closed");
}

void run_select_queries(DB2Connection conn) {
  String query1 = select_statements[0];
  String query2 = select_statements[1];
  DB2Command cmd1 = new DB2Command(query1, conn);
  DB2Command cmd2 = new DB2Command(query2, conn);
  DB2DataReader dr1 = cmd1.ExecuteReader();
  DB2DataReader dr2 = cmd2.ExecuteReader();
  dr1.Close();
  dr2.Close();
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
main();












