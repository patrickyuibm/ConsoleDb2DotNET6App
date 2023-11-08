using System;
using System.Data;
using System.Threading;
using System.IO;
using IBM.Data.DB2;
using System.Configuration;
using System.Collections.Specialized;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.Xml;
using IBM.Data.DB2Types;
using System.Text;

public void callSP(DB2Connection conn) {
  DB2DataReader db2reader = null;
  DB2Command db2cmd = null;
  DB2Transaction trans = null;

  String procName = "";
  String cmdString = "CALL " + procName + "(?,?)";

  trans = conn.BeginTransaction();
  DB2Command db2cmd = conn.createCommand();
  db2cmd.CommandText = cmdString;
  
  Console.WriteLine(db2cmd.CommandText);
  DB2DataReader db2reader = db2cmd.ExecuteReader();
  Console.WriteLine("===========================================");
  
  //print the output here
  
  db2reader.Close();
  db2cmd.Dispose();
  trans.Dispose();
  conn.Close();  
}
