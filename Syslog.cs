using System;
using System.Data;
using System.IO;
using System.Threading;
using IBM.Data.DB2;
using System.Configuration;

namespace ConsoleDb2DotNET6App
{

    public class Syslog
    {
        StreamWriter m_log;
        int log_level = 0;
         
        String db2SSID, workloadName, componentName;
        public Syslog(string filename)
        {
            m_log = new StreamWriter(filename);
            log_level = Convert.ToInt32(ConfigurationManager.AppSettings.Get("LOG_LEVEL"));
            db2SSID = ConfigurationManager.AppSettings.Get("DATABASE_NAME");
            workloadName = ConfigurationManager.AppSettings.Get("CLIENT_APPLNAME");
            componentName = ConfigurationManager.AppSettings.Get("CLIENT_ACCTNG");
        }
        public void Log(string txt)
        {
            System.Threading.Monitor.Enter(m_log);
            m_log.WriteLine(System.Threading.Thread.CurrentThread.Name + " " + DateTime.Now + " " + txt);
            m_log.Flush();         
            System.Threading.Monitor.Exit(m_log);
        }//Log
        
        
        public void LogError(string txt, String errQual)
        {
            System.Threading.Monitor.Enter(m_log);
            
              m_log.WriteLine(System.Threading.Thread.CurrentThread.Name + " " + DateTime.Now + " " + txt);
              m_log.Flush();
              if(log_level == 3 )
                CallCEL(errQual, txt);
              System.Threading.Monitor.Exit(m_log);
        }//Log
        public void CallCEL(String errQual, String messageText)
        {
            //Set up the Stored Procedure stuff.
            string sProcName = "SYSPROC.SP_COMMON_ERR_LOG";
            string API = "C#";
            string machineName;
            if (db2SSID.Length > 5)
                db2SSID = db2SSID.Substring(1, 5).Trim();
            Log("calling Procedure " + sProcName);
            string celConnectionString = "Database=DSNA1;User ID=LGB0083;Password=pilsner;";
            DB2Connection celConnection = new DB2Connection(celConnectionString);
            machineName = Environment.MachineName;
            DB2DataAdapter SQLDataAdapter = new DB2DataAdapter();
            SQLDataAdapter.SelectCommand = new DB2Command(sProcName, celConnection);
            SQLDataAdapter.SelectCommand.CommandType = CommandType.StoredProcedure;

            SQLDataAdapter.SelectCommand.Parameters.Add("DB2", DB2Type.Char, 5, "DB2");
            SQLDataAdapter.SelectCommand.Parameters[0].Value = db2SSID;
            SQLDataAdapter.SelectCommand.Parameters[0].Direction = ParameterDirection.Input;

            SQLDataAdapter.SelectCommand.Parameters.Add("WL_NAME", DB2Type.Char, 8, "WL_NAME");
            SQLDataAdapter.SelectCommand.Parameters[1].Value = workloadName;
            SQLDataAdapter.SelectCommand.Parameters[1].Direction = ParameterDirection.Input;

            SQLDataAdapter.SelectCommand.Parameters.Add("COMP_NAME", DB2Type.VarChar, 24, "COMP_NAME");
            SQLDataAdapter.SelectCommand.Parameters[2].Value = componentName;
            SQLDataAdapter.SelectCommand.Parameters[2].Direction = ParameterDirection.Input;

            SQLDataAdapter.SelectCommand.Parameters.Add("API", DB2Type.Char, 4, "API");
            SQLDataAdapter.SelectCommand.Parameters[3].Value = API;
            SQLDataAdapter.SelectCommand.Parameters[3].Direction = ParameterDirection.Input;

            SQLDataAdapter.SelectCommand.Parameters.Add("ERRQUAL", DB2Type.Char, 5, "ERRQUAL");
            SQLDataAdapter.SelectCommand.Parameters[4].Value = errQual;
            SQLDataAdapter.SelectCommand.Parameters[4].Direction = ParameterDirection.Input;

            SQLDataAdapter.SelectCommand.Parameters.Add("MACHINE", DB2Type.VarChar, 24, "MACHINE");
            SQLDataAdapter.SelectCommand.Parameters[5].Value = machineName;
            SQLDataAdapter.SelectCommand.Parameters[5].Direction = ParameterDirection.Input;

            SQLDataAdapter.SelectCommand.Parameters.Add("MESSAGE", DB2Type.VarChar, 31000, "MESSAGE");
            SQLDataAdapter.SelectCommand.Parameters[6].Value = messageText;
            SQLDataAdapter.SelectCommand.Parameters[6].Direction = ParameterDirection.Input;

            SQLDataAdapter.SelectCommand.Parameters.Add("RETURNCODE", DB2Type.Integer, 4, "RETURNCODE");
            SQLDataAdapter.SelectCommand.Parameters[7].Value = 0;
            SQLDataAdapter.SelectCommand.Parameters[7].Direction = ParameterDirection.Output;

            SQLDataAdapter.SelectCommand.Parameters.Add("SQLCODE", DB2Type.Integer, 4, "SQLCODE");
            SQLDataAdapter.SelectCommand.Parameters[8].Value = 0;
            SQLDataAdapter.SelectCommand.Parameters[8].Direction = ParameterDirection.Output;

            SQLDataAdapter.SelectCommand.Parameters.Add("RETURNMESSAGE", DB2Type.VarChar, 100, "RETURNMESSAGE");
            SQLDataAdapter.SelectCommand.Parameters[9].Value = " ";
            SQLDataAdapter.SelectCommand.Parameters[9].Direction = ParameterDirection.Output;

            try
            {
                celConnection.Open();
                SQLDataAdapter.SelectCommand.ExecuteNonQuery();
            }
            catch (DB2Exception de)
            {

                Log("Error while calling Procedure " + sProcName);
                Log(de.ToString());
            }
            finally
            {
                celConnection.Close();
                celConnection.Dispose();
                m_log.Flush();
                Thread.Sleep(2500);		// If a thread hits an error, let it sit here for 2.5 seconds to eliminate a flood of messages.
            }

        }//PushMemoryFile
        public void LogDB2Error(DB2Exception myException, string errQual, string extraText)
        {
            string messageText;
            
                       
            for (int i = 0; i < myException.Errors.Count; i++)
            {
                
                DateTime currentSystemTime = DateTime.Now;
                messageText = myException.Errors[i].NativeError.ToString() + " : " + myException.Errors[i].Message +
                    " : " + myException.Errors[i].Source + " : " + myException.Errors[i].SQLState.ToString() +
                    " : " + extraText;
                if ((log_level == 1)||(log_level == 2) )
                {
                    Log("Index # " + i + "\n" +
                        " Message : " + myException.Errors[i].Message + "\n" +
                        " Native : " + myException.Errors[i].NativeError.ToString() + "\n" +
                        " Source : " + myException.Errors[i].Source + "\n" +
                        " SQLState : " + myException.Errors[i].SQLState + "\n");
                    Log(extraText + ":" + errQual);
                    Log(myException.StackTrace);
                    m_log.Flush();
                    Thread.Sleep(2500);
                }
                if (log_level == 3)
                {
                    CallCEL(errQual, messageText);
                    
                }
            }
        }
    }
  }
