// See https://aka.ms/new-console-template for more information

//***************************** IMPORT DEPENDENCIES *****************************
using IBM.Data.Db2;
using System.Threading;
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

  //Multithreading
  int numInsertThreads = 4; 
  Thread[] myThreads = new Thread[numInsertThreads]; 
  for (int i = 0; i < numInsertThreads; i++) { 
    myThreads[i] = new Thread(new ThreadStart(() => startSelect(connString, i))); 
  } 
  int ctr = 1;
  foreach (Thread thread in myThreads) {
    thread.IsBackground = true;
    thread.Name = "Thread_" + ctr.ToString();
    thread.Start();
    Thread.Sleep(1);
  }
  foreach (Thread thread in myThreads) {
    thread.Join();
  }
}

void startSelect(String connectionString, int threadNumber) {
  DB2Connection conn = new DB2Connection(connectionString);
  conn.Open();
  Console.WriteLine("Connection opened successfully");
  run_insert_and_select_tb2_SP(conn);
  conn.Close(); 
  Console.WriteLine("Connection Closed");   
}

void run_insert_and_select_tb2_SP(DB2Connection conn) {
  //Set up a stored procedure
  DB2Transaction trans = conn.BeginTransaction();
  DB2Command cmd = conn.CreateCommand();
  String spname = "DB2ADM.INSERT_AND_SELECT_TB2";
  String procCall = "CALL " + spname + " (@param1, @param2)";
  cmd.Transaction = trans;
  cmd.CommandText = procCall;
  
  // Register input-output and output parameters for the DB2Command
  cmd.Parameters.Add( new DB2Parameter("@param1", 5));
  cmd.Parameters.Add( new DB2Parameter("@param2", 6));
  
  // Call the stored procedure
  Console.WriteLine("Calling stored procedure " + spname);
  DB2DataReader myReader = cmd.ExecuteReader(); 

  
  //Retrieve the return code (output parameter in SP)
  outParm = (int)cmd.Parameters["@param2"].Value;
  if (outParm != 0) {
    Console.WriteLine("Call failed");
  } else {
    Console.WriteLine("Connection successful");
  }
  
  
  // always call Close when done reading. 
  myReader.Close(); 
}

//*******************************************************************


//***************************** RUN METHODS HERE *****************************
//Method to set up threads that run several stored procedures
main();
//****************************************************************************












