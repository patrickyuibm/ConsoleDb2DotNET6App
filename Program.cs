
// See https://aka.ms/new-console-template for more information

using IBM.Data.Db2;

Console.WriteLine("Using DB2 .NET6 provider");
Console.WriteLine("test edit");

string uid = Environment.GetEnvironmentVariable("uid");
string pwd = Environment.GetEnvironmentVariable("pwd");
string server = Environment.GetEnvironmentVariable("server");
string db = Environment.GetEnvironmentVariable("db");

//Connection String
string connString = "uid=" + uid + ";pwd=" + pwd + ";server=" + server + ";database=" + db;

DB2Connection con = new DB2Connection(connString);
con.Open();
Console.WriteLine("Connection Opened successfully");

//Run a SQL query
Console.Read();

string mySelectQuery = "SELECT * FROM SYSIBM.SYSDUMMY1";
          
DB2Command myCommand = new DB2Command(mySelectQuery, con);
Console.WriteLine("Fetch data from blogs table");
DB2DataReader myReader = myCommand.ExecuteReader();
try
{
    while (myReader.Read())
    {
         Console.WriteLine(myReader.GetInt32(0) + ", " + myReader.GetString(1));
    }
}
finally
{
    // always call Close when done reading.
    myReader.Close();
    // always call Close when done with connection.
    con.Close();
    Console.WriteLine("Connection Closed");  
}


/*
Old code
con.Close();
Console.WriteLine("Connection Closed");
*/
