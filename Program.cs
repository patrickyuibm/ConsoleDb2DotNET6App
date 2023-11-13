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
  string uid = Environment.GetEnvironmentVariable("uid");
  string pwd = Environment.GetEnvironmentVariable("pwd");
  string server = Environment.GetEnvironmentVariable("server");
  string db = Environment.GetEnvironmentVariable("db");

  //Connection String
  string connString = "uid=" + uid + ";pwd=" + pwd + ";server=" + server + ";database=" + db;
  DB2Connection conn = new DB2Connection(connString);
  conn.Open();
  Console.WriteLine("Connection Opened successfully");

  //Establish multithreading
  Thread[] myThreads = new Thread[numInsertThreads];
  for (int i = 0; i < numInsertThreads; i++) {
    myThreads[i] = new Thread(new ThreadStart(() => startSelect(conn)));
  }
}

void run_insert_and_select_tb2_SP(DB2Connection conn) {
  //Set up a stored procedure
  DB2Transaction trans = conn.BeginTransaction();
  DB2Command cmd = conn.CreateCommand();
  String spname = 'DB2ADM.INSERT_AND_SELECT_TB2';
  String procCall = "CALL " + spname + " (@param1, @param2)";
  cmd.Transaction = trans;
  cmd.CommandText = procCall;
  // Register input-output and output parameters for the DB2Command
  cmd.Parameters.Add( new DB2Parameter("@param1", 5));
  cmd.Parameters.Add( new DB2Parameter("@param2", 6));
  // Call the stored procedure
  Console.WriteLine("Call stored procedure named " + procName);
  DB2DataReader myReader = cmd.ExecuteReader();
}

void startselect(DB2Connection conn) {
  //call running stored procedures here
  run_insert_and_select_tb2_SP(conn);
}



//*******************************************************************


//***************************** RUN METHODS HERE *****************************
//Method to set up threads that run several stored procedures
main();
//****************************************************************************








// always call Close when done reading.
myReader.Close();
// always call Close when done with connection.
conn.Close();
Console.WriteLine("Connection Closed");  









