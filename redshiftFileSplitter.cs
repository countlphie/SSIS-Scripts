#region 
this code is used in a vbscript in SSIS
used to split a file to optimize loads to redshift

reminder: the namespace is invalid and this is a draft. needs refinement to be re-used, but the base is there
#endregion


#region Namespaces
using System;
using System.Data;
using Microsoft.SqlServer.Dts.Runtime;
using System.Windows.Forms;
using System.IO;
using System.Linq;
using System.Collections.Generic;

#endregion

namespace __fileSplitter__
{
    /// <summary>
    /// ScriptMain is the entry point class of the script.  Do not change the name, attributes,
    /// or parent of this class.
    /// </summary>
	[Microsoft.SqlServer.Dts.Tasks.ScriptTask.SSISScriptTaskEntryPointAttribute]
	public partial class ScriptMain : Microsoft.SqlServer.Dts.Tasks.ScriptTask.VSTARTScriptObjectModelBase
	{
        #region Help:  Using Integration Services variables and parameters in a script
        /* To use a variable in this script, first ensure that the variable has been added to 
         * either the list contained in the ReadOnlyVariables property or the list contained in 
         * the ReadWriteVariables property of this script task, according to whether or not your
         * code needs to write to the variable.  To add the variable, save this script, close this instance of
         * Visual Studio, and update the ReadOnlyVariables and 
         * ReadWriteVariables properties in the Script Transformation Editor window.
         * To use a parameter in this script, follow the same steps. Parameters are always read-only.
         * 
         * Example of reading from a variable:
         *  DateTime startTime = (DateTime) Dts.Variables["System::StartTime"].Value;
         * 
         * Example of writing to a variable:
         *  Dts.Variables["User::myStringVariable"].Value = "new value";
         * 
         * Example of reading from a package parameter:
         *  int batchId = (int) Dts.Variables["$Package::batchId"].Value;
         *  
         * Example of reading from a project parameter:
         *  int batchId = (int) Dts.Variables["$Project::batchId"].Value;
         * 
         * Example of reading from a sensitive project parameter:
         *  int batchId = (int) Dts.Variables["$Project::batchId"].GetSensitiveValue();
         * */

        #endregion

        #region Help:  Firing Integration Services events from a script
        /* This script task can fire events for logging purposes.
         * 
         * Example of firing an error event:
         *  Dts.Events.FireError(18, "Process Values", "Bad value", "", 0);
         * 
         * Example of firing an information event:
         *  Dts.Events.FireInformation(3, "Process Values", "Processing has started", "", 0, ref fireAgain)
         * 
         * Example of firing a warning event:
         *  Dts.Events.FireWarning(14, "Process Values", "No values received for input", "", 0);
         * */
        #endregion

        #region Help:  Using Integration Services connection managers in a script
        /* Some types of connection managers can be used in this script task.  See the topic 
         * "Working with Connection Managers Programatically" for details.
         * 
         * Example of using an ADO.Net connection manager:
         *  object rawConnection = Dts.Connections["Sales DB"].AcquireConnection(Dts.Transaction);
         *  SqlConnection myADONETConnection = (SqlConnection)rawConnection;
         *  //Use the connection in some code here, then release the connection
         *  Dts.Connections["Sales DB"].ReleaseConnection(rawConnection);
         *
         * Example of using a File connection manager
         *  object rawConnection = Dts.Connections["Prices.zip"].AcquireConnection(Dts.Transaction);
         *  string filePath = (string)rawConnection;
         *  //Use the connection in some code here, then release the connection
         *  Dts.Connections["Prices.zip"].ReleaseConnection(rawConnection);
         * */
        #endregion
        double minFileSize = 75;
        double divisor = 1048576;
        int minLinesPerFile = 500000;

        double maxSizePerFile = 1073741824;  // 1 GB in Bytes

        static String FileName;
        static String TblName;
        
        string rootPath = @"C:\SSIS\RedshiftPOCVS2010\Files\";
        string tS3Path = @"C:\SSIS\RedshiftPOCVS2010\Files\ToS3\";
        string rootPathS3 = "s3://ultra-dba-archive/RedshiftPOC/FACT/Load/";
        /*
         * Do not use this function. part of rough draft
         */
        public int GetNumFiles(long fileLength)
        {
            int numFiles;
            
            // very rough method to determine how to divide file up
            numFiles = (int)Math.Round(fileLength / divisor / minFileSize);

            numFiles = numFiles % 2 == 0 ? numFiles : numFiles + 1;

            return numFiles;
        }

        /*
         * 
         */
        public void CreateManifest(List<string> manifestFileNames, String FilePath)
        {

            String manifestFileName = TblName + "_manifest";
            String manifestLine;

            using (var str = new StreamWriter(tS3Path + manifestFileName, false))
            {

                str.WriteLine("{");
                str.WriteLine("  \"entries\": [");

                String last = manifestFileNames.Last();
                foreach (String splitFileName in manifestFileNames)
                {
                    manifestLine = "    {\"url\":\"" + rootPathS3 + splitFileName + "\"}";

                    if (splitFileName == last)
                    {
                        str.WriteLine(manifestLine);
                    }
                    else
                    {
                        str.WriteLine(manifestLine + ",");
                    }

                }

                str.WriteLine("    ]");
                str.WriteLine("}");
                str.Flush();
            }

            // send back manifest name
            Dts.Variables["User::varManifestName"].Value = manifestFileName;

            return;
        }



        /*
         * SplitFilesBySlice
         * Takes a file and splits the file into the same number of slices Redshift has
         */
        public void SplitFilesBySlice(String filePath, int numSlices)
        {

            // get file size in bytes
            double fileSize = new FileInfo(filePath).Length;

            // calculating minimum number of files to generate based on # of slices
            int numFiles = numSlices * (int)(Math.Floor(fileSize / (maxSizePerFile * numSlices)) + 1);

            // get number of lines in original file
            int numLines = File.ReadLines(filePath).Count();

            // number of lines per file, rounding downwards
            int linesPerFile = (int)Math.Floor((double) numLines / (double) numFiles);

            // declare variables needed for loops
            int fileNumber = 0;
            int linesWritten = 0;
            String tmpNewFileName = "";
            StreamWriter writer = null;
            string line;

            //array of newly created file names for manifest
            List<string> manifestFiles = new List<string>();

            // used to sanity check our file split
            int totalLinesWritten = 0;

            try
            {
                using (StreamReader inputFile = new StreamReader(filePath))
                {
                    while ((line = inputFile.ReadLine()) != null)
                    {

                        // Create a new file if:
                        //   - this is the first iteration of loop
                        //   - # of lines written exceeds minimum # of lines per file
                        // Continue writing if currently writing to the last file

                        if (fileNumber < (numFiles - 1) && (writer == null || linesWritten > linesPerFile))
                        {

                            // done writing to current file.  close and reset loop variables
                            if (writer != null)
                            {
                                writer.Close();
                                writer = null;

                                totalLinesWritten += linesWritten;
                                linesWritten = 0;
                                
                                // tracking # of files created
                                ++fileNumber;
                            }

                            // each split file is the original file name with a number appended to the end
                            tmpNewFileName = FileName + "." + fileNumber.ToString();

                            // add newly created file name to manifest
                            manifestFiles.Add(tmpNewFileName);

                            writer = new StreamWriter(tS3Path + tmpNewFileName);
                        }

                        writer.WriteLine(line);
                        ++linesWritten;
                    }
                }
            }
            finally
            {
                if (writer != null)
                    writer.Close();

                totalLinesWritten += linesWritten;
            }


            if (totalLinesWritten != numLines)
                MessageBox.Show("File split failed.  Lines counts not matching.");

            CreateManifest(manifestFiles, filePath);

            return;

        }

        public void SplitFilesByLine(String filePath)
        {
            String tmpNewFileName = "";

            int i = 0;
            int linesWritten = 0;
            
            int NumLines = File.ReadLines(filePath).Count();

            int numFiles = (NumLines / minLinesPerFile) % 2 == 0 ? (NumLines / minLinesPerFile) : (NumLines / minLinesPerFile) + 1;

            StreamWriter writer = null;
            string line;

            try
            {
                using (StreamReader inputFile = new StreamReader(filePath))
                {

                    while ((line = inputFile.ReadLine()) != null)
                    {

                        // if i == max number of files then we don't want to create any more files
                        // we want to put any remaining lines in the last file

                        // if writer == null, then it's the first iteration

                        // if the current file size exceeds our max size per file then create new file

                        if (i < (numFiles - 1) && (writer == null || linesWritten > minLinesPerFile))
                        {
                            //MessageBox.Show("File size: " + tmpfileSize.ToString());

                            // means file size limit hit, so close file and free up resource
                            if (writer != null)
                            {
                                writer.Close();
                                writer = null;
                                linesWritten = 0;
                            }

                            tmpNewFileName = filePath + "." + i.ToString();

                            writer = new StreamWriter(tmpNewFileName);

                            //MessageBox.Show(tmpNewFileName + " created");

                            //increment number of files
                            i++;
                        }

                        writer.WriteLine(line);

                        // increment number of lines written
                        linesWritten++;
                    }
                }
            }
            finally
            {
                if (writer != null)
                    writer.Close();
            }

            return;
        }

        public void SplitFiles(String filePath)
        {
            String tmpNewFileName = "";

            int i = 0;

            double tmpfileSize = 0;

            FileInfo fileToBeProcessed = new FileInfo(filePath);

            long fileLength = fileToBeProcessed.Length;

            int numFiles = GetNumFiles(fileLength);
            
            // reverse the calculation to get estimated size of each file
            // since we are rounding upwards on dividing files
            double fileSizeEst = fileLength / divisor / numFiles;


            FileInfo fi = null;
            StreamWriter writer = null;
            string line;

            try
            {
                using (StreamReader inputFile = fileToBeProcessed.OpenText())
                {

                    while ((line = inputFile.ReadLine()) != null)
                    {

                        // if i == max number of files then we don't want to create any more files
                        // we want to put any remaining lines in the last file

                        // if writer == null, then it's the first iteration

                        // if the current file size exceeds our max size per file then create new file

                        if (i < (numFiles - 1) && (writer == null || tmpfileSize > fileSizeEst))
                        {
                            //MessageBox.Show("File size: " + tmpfileSize.ToString());

                            // means file size limit hit, so close file and free up resource
                            if (writer != null)
                            {
                                writer.Close();
                                writer = null;
                                tmpfileSize = 0;
                            }

                            tmpNewFileName = filePath + "." + i.ToString();
                            fi = new FileInfo(tmpNewFileName);
                            writer = fi.CreateText();

                            //MessageBox.Show(tmpNewFileName + " created");

                            //writer = new StreamWriter(tmpNewFileName);

                            //increment number of files
                            i++;
                        }

                        writer.WriteLine(line);

                        // keeping track of file size
                        tmpfileSize = new FileInfo(tmpNewFileName).Length / divisor;
                    }
                }
            }
            finally
            {
                if (writer != null)
                    writer.Close();
            }

            return;
        }

		/// <summary>
        /// This method is called when this script task executes in the control flow.
        /// Before returning from this method, set the value of Dts.TaskResult to indicate success or failure.
        /// To open Help, press F1.
        /// </summary>
        /// 
		public void Main()
		{
			// TODO: Add your code here

            // use SSIS variable for path
            FileName = Dts.Variables["User::varFileName"].Value.ToString();
            String FilePath = @"C:\SSIS\RedshiftPOCVS2010\Files\" + FileName;

            TblName = FileName.Substring(0, FileName.IndexOf(".csv"));

            Dts.Variables["User::varCurrentTblName"].Value = TblName;

            SplitFilesBySlice(FilePath, 2);
           // long length = new FileInfo(FilePath).Length;

            //int numFiles = GetNumFiles(length);

            //MessageBox.Show(GetNumFiles(length).ToString());

			Dts.TaskResult = (int)ScriptResults.Success;
		}

        #region ScriptResults declaration
        /// <summary>
        /// This enum provides a convenient shorthand within the scope of this class for setting the
        /// result of the script.
        /// 
        /// This code was generated automatically.
        /// </summary>
        enum ScriptResults
        {
            Success = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Success,
            Failure = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Failure
        };
        #endregion

	}
}
