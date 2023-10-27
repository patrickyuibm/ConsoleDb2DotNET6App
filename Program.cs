
// See https://aka.ms/new-console-template for more information

using IBM.Data.Db2;

Console.WriteLine("Using DB2 .NET6 provider");

//Connection String
string connstr = Environment.GetEnvironmentVariable("connstring");

DB2Connection con = new DB2Connection(connstr);

con.Open();

Console.WriteLine("Connection Opened successfully");

con.Close();

Console.WriteLine("Connection Closed");
