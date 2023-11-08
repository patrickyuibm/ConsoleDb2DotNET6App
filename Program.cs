
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
con.Close();
Console.WriteLine("Connection Closed");
