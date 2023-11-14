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
  run_insert_and_select_tb2_SP(conn);
}

string getRS(DB2Command cmd)
  {
    Console.WriteLine('1');
    string resultString = "";
    DB2ResultSet rs = cmd.ExecuteResultSet(
      DB2ResultSetOptions.Scrollable |
      DB2ResultSetOptions.Sensitive |
      DB2ResultSetOptions.SkipDeleted);
    resultString = rs.GetDB2Date(0).ToString();
    resultString += ", " + rs.GetDB2String(1).ToString();
    resultString += ", " + rs.GetDB2String(2).ToString();
    resultString += ", " + rs.GetDB2Int32(3).ToString();
    /*
    if (rs.Scrollable)
    {
      Console.WriteLine('2');
      if (rs.ReadLast())
      {
        Console.WriteLine('3');
        resultString = rs.GetDB2Date(0).ToString();
        resultString += ", " + rs.GetDB2String(1).ToString();
        resultString += ", " + rs.GetDB2String(2).ToString();
        resultString += ", " + rs.GetDB2Int32(3).ToString();
      }
    }
    */

    return resultString;
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
  Console.WriteLine("Call stored procedure named " + spname);
  DB2DataReader myReader = cmd.ExecuteReader();
  // always call Close when done reading.
  myReader.Close();
  // Print the result
  Console.WriteLine(getRS(cmd));
  
}

//*******************************************************************


//***************************** RUN METHODS HERE *****************************
//Method to set up threads that run several stored procedures
main();
//****************************************************************************












