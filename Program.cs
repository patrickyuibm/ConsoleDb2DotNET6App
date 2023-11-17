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
//*******************************************************************************


//***************************** METHODS *****************************
//Method to run stored procedure
/*
Note: currently, since the parameters for each SP are different, one method to run any SP is nearly impossible. We have to know what 
SP we want to run the code it in first. 
*/
void main() {
  //Connection String
  string uid = Environment.GetEnvironmentVariable("uid");
  string pwd = Environment.GetEnvironmentVariable("pwd");
  string server = Environment.GetEnvironmentVariable("server");
  string db = Environment.GetEnvironmentVariable("db");
  string connString = "uid=" + uid + ";pwd=" + pwd + ";server=" + server + ";database=" + db;
  
  int numInsertThreads = 30;
  Thread[] myThreads = new Thread[numInsertThreads];
  for (int i = 0; i < numInsertThreads; i++) {
    Thread t = new Thread(new ThreadStart(() => startSelect(connString)));
    t.Name = "Thread_" + (i+1).ToString();
    t.Start();
    myThreads[i] = t;
  }
  foreach (Thread t in myThreads) {
    t.Join();
  }
  Console.WriteLine("All threads complete");
}

void startSelect(String connectionString) {
  DB2Connection conn = new DB2Connection(connectionString);
  String thname = System.Threading.Thread.CurrentThread.Name;
  conn.Open();
  Console.WriteLine(thname + " running");
  //run_insert_and_select_tb2_SP(conn);
  run_select_queries(conn);
  conn.Close(); 
  Console.WriteLine(thname + " closed");
}

void run_select_queries(DB2Connection conn) {
  String query1 = "SELECT * FROM DB2ADM.TB2";
  DB2Command cmd1 = new DB2Command(query1, conn);
  DB2DataReader myReader = cmd1.ExecuteReader();
  myReader.Close();
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

//*******************************************************************


//***************************** RUN METHODS HERE *****************************
//Method to set up threads that run several stored procedures
main();
//****************************************************************************












