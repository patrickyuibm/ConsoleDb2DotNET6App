// See https://aka.ms/new-console-template for more information

using IBM.Data.Db2;

string uid = Environment.GetEnvironmentVariable("uid");
string pwd = Environment.GetEnvironmentVariable("pwd");
string server = Environment.GetEnvironmentVariable("server");
string db = Environment.GetEnvironmentVariable("db");

//Connection String
string connString = "uid=" + uid + ";pwd=" + pwd + ";server=" + server + ";database=" + db;
DB2Connection conn = new DB2Connection(connString);
conn.Open();
Console.WriteLine("Connection Opened successfully");

DB2Transaction trans = conn.BeginTransaction();
DB2Command cmd = conn.CreateCommand();
String procName = "DB2ADM.INSERT_AND_SELECT_TB2";
String procCall = "CALL " + procName + " (@param1, @param2)";
cmd.Transaction = trans;
//cmd.CommandType = CommandType.Text;
cmd.CommandText = procCall;

// Register input-output and output parameters for the DB2Command
cmd.Parameters.Add( new DB2Parameter("@param1", 5));
cmd.Parameters.Add( new DB2Parameter("@param2", 6));

// Call the stored procedure
Console.WriteLine("Call stored procedure named " + procName);
DB2DataReader myReader = cmd.ExecuteReader();

// always call Close when done reading.
myReader.Close();
// always call Close when done with connection.
conn.Close();
Console.WriteLine("Connection Closed");  



