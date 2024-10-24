using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Security.Cryptography;
using System.Text;
using System.Windows.Forms;
using System.Threading;
using System.IO;

using Cogent.Data;
using Cogent.LOB;
using Cogent.Collections;
using Cogent.UpdaterMain.Updates;
using Cogent.UpdaterMain;
using Cogent.BusinessServices;

using Xceed.Zip;

namespace CogentQC_Update_ImportSpecs
{
	public partial class Main : Cogent.UpdaterMain.Main
	{
		private string outputFileName	= "CogentQC_NMI_SampleDataUpdate.txt";
		private DateTime startTime		= DateTime.Now;
		private DateTime endTime;

        private const string _UnderwriterNameCode = "UnderwriterNameCode";
        private const string _UnderwritingGrpCode = "UnderwritingGrpCode";

        private LookupTable ltUnderwriterNameCode = null;
        private LookupTable ltUnderwritingGrpCode = null;


        public Main()
		{
			InitializeComponent();
			this.UpdateClicked				+= new EventHandler(ProcessUpdate);
			Xceed.Zip.Licenser.LicenseKey	= "ZIN64-YL4WG-WKN4J-A25A";	// "ZIN50-YJWH2-A4AEK-882A";
		}

		void ProcessUpdate(object sender, EventArgs e)
		{
			Update.PostProgress postProgress = new Update.PostProgress(UpdateText);

            ProcessSampleLoanDataUpdate(postProgress);

			this.Process(postProgress);

			endTime = DateTime.Now;
			this.FinishProcessing();

		}

		private void Process(Update.PostProgress postProgress)
		{
			UpdateLog ul = new UpdateLog();
			ul.UpdateID = Guid.NewGuid();
			ul.Name = "CogentQC_NMI_SampleDataUpdate";
			ul.UpdateDate = DateTime.Now;
			ul.UpdateType = "Updater Program";
			ul.Notes = "Update Underwriter Name and Underwriting Group sample data.";
			ul.InsertInDb();
		}

		private void FinishProcessing()
		{
			endTime = DateTime.Now;
			GenerateLogFile();
			this.promptForClose = false;
//			this.Close();
		}

		private void GenerateLogFile()
		{
			TextWriter tw = new StreamWriter(outputFileName);

			tw.WriteLine("This log file generated at: " + DateTime.Now.ToString());
			tw.WriteLine(" ");
			tw.WriteLine("Updater began: " + startTime.ToString());
			tw.WriteLine(" ");
			tw.WriteLine("================================");
			tw.WriteLine(" ");
			tw.WriteLine(outputTxt.Text);
			tw.WriteLine(" ");
			tw.WriteLine("================================");
			tw.WriteLine(" ");
			tw.WriteLine("Updater ended: " + endTime.ToString());

			// close the stream
			tw.Close();
		}


		private int ProcessSampleLoanDataUpdate(Update.PostProgress postProgress)
		{
            int retVal = 0;
            try
            {
                //Get Lookup Tables
                ltUnderwriterNameCode = LookupServices.GetLookupTable(_UnderwriterNameCode);
                ltUnderwritingGrpCode = LookupServices.GetLookupTable(_UnderwritingGrpCode);


                //Get Sampled Loans with Underwriter Name or Underwriting Group values
                ProcessSampledLoans(postProgress, false);

                //Get Sampled DAR Loans with Underwriter Name or Underwriting Group values
                ProcessSampledLoans(postProgress, true);

                //remove unused lookup values
                int countUW = DeleteUnusedLookupValues(postProgress, true);
                postProgress("Unused Underwriter Names deleted: " + countUW);
                int countUG = DeleteUnusedLookupValues(postProgress, false);
                postProgress("Unused Underwriting Groups deleted: " + countUG);

                return retVal;
            }
            catch (Exception ex)
            {
                postProgress("ERROR: " + ex.Message + Environment.NewLine + ex.StackTrace);
                return retVal;
            }

        }

        private bool ProcessSampledLoans(Update.PostProgress postProgress, bool isDAR)
        {
            try
            {
                NameValueCollection nvcUWName = new NameValueCollection();
                NameValueCollection nvcUWGroup = new NameValueCollection();

                int updateCount = 0;
                int failedCount = 0;

                DataTable sampledLoansToUpdate = GetSampledLoansToProcess(postProgress, false);
                if (sampledLoansToUpdate.Rows.Count > 0)
                {
                    postProgress("START: Process Sampled" + ((isDAR) ? "DAR " : string.Empty) + "Loans With Values");
                    postProgress("");
                    postProgress("Number of Sampled " + ((isDAR) ? "DAR ": string.Empty) +"Loans to Process: " + sampledLoansToUpdate.Rows.Count.ToString());
                    postProgress("");
                    foreach (DataRow dr in sampledLoansToUpdate.Rows)
                    {
                        if (dr["LoanNumber"] != null && dr["LoanNumber"].ToString().Trim() != string.Empty)
                        {
                            DataTable populationRecord = GetLoanImportRecord(postProgress, dr["LoanNumber"].ToString().Trim(), false);
                            if(populationRecord.Rows.Count > 0)
                            {
                                NameValueCollection results = GetUWGuids(postProgress, populationRecord.Rows[0], nvcUWName, nvcUWGroup);

                                UpdateSampledLoanRecord(postProgress, results, dr);
                                updateCount++;
                            }
                            else
                            { postProgress(((isDAR) ? "DAR " : string.Empty) + "POPULATION RECORD NOT FOUND: " + dr["LoanNumber"].ToString().Trim()); failedCount++; }
                        }
                    }
                    postProgress("FINISH: Process Sampled" + ((isDAR) ? "DAR " : string.Empty) + "Loans With Values");
                    postProgress("");
                    postProgress("Update Count: " + updateCount);
                    postProgress("Failed Count: " + failedCount);
                    postProgress("");
                }
                else
                {
                    postProgress("NOTE: There are no sampled loans with Underwriter Name or Underwriting Group values to process.");
                }

                return true;
            }
            catch(Exception ex)
            {
                postProgress("ERROR: " + ex.Message + Environment.NewLine + ex.StackTrace);
                return false;
            }
        }

        private bool UpdateSampledLoanRecord(Update.PostProgress postProgress, NameValueCollection results, DataRow sampledRecord)
        {
            try
            {
                Guid underwriterNameID = Guid.Empty;
                underwriterNameID = (Guid)results[_UnderwriterNameCode];


                Guid underwritingGroupID = Guid.Empty;
                underwritingGroupID = (Guid)results[_UnderwritingGrpCode];

                string SQL = string.Format("UPDATE Loan SET underwriterName = {0}, underwritingGroup = {1} WHERE LoanID = {2} AND SampleTypeID = {3}", DbConvert.ToSqlLiteral(underwriterNameID), DbConvert.ToSqlLiteral(underwritingGroupID), DbConvert.ToSqlLiteral(sampledRecord["LoanID"].ToString()), DbConvert.ToSqlLiteral(sampledRecord["SampleTypeID"].ToString()));
                Db.ExecuteSQL(SQL);

                return true;
            }
            catch (Exception ex)
            {
                postProgress("ERROR: " + ex.Message + Environment.NewLine + ex.StackTrace);
                return false;
            }
        }

        private NameValueCollection GetUWGuids(Update.PostProgress postProgress, DataRow dr, NameValueCollection nvcUWName, NameValueCollection nvcUWGroup)
        {
            NameValueCollection retVal = new NameValueCollection();
            string underwriterName = (dr["underwriterName"] != null) ? dr["underwriterName"].ToString().Trim() : string.Empty;
            string underwritingGroup = (dr["underwritingGroup"] != null) ? dr["underwritingGroup"].ToString().Trim() : string.Empty;

            //Process UnderwriterName information
            Guid underwriterNameID = Guid.Empty;
            if (nvcUWName.ContainsName(underwriterName))
            {
                underwriterNameID = (Guid)nvcUWName[underwriterName];
            }
            else
            {
                underwriterNameID = GetLookupValueID(postProgress, underwriterName, ltUnderwriterNameCode, true);
                if (!nvcUWName.ContainsName(underwriterName))
                {
                    nvcUWName.Add(underwriterName, underwriterNameID);
                }
            }

            //Process UnderwritingGroup information
            Guid underwritingGroupID = Guid.Empty;
            if (nvcUWGroup.ContainsName(underwritingGroup))
            {
                underwritingGroupID = (Guid)nvcUWGroup[underwritingGroup];
            }
            else
            {
                underwritingGroupID = GetLookupValueID(postProgress, underwritingGroup, ltUnderwritingGrpCode, false);
                if (!nvcUWGroup.ContainsName(underwritingGroup))
                {
                    nvcUWGroup.Add(underwritingGroup, underwritingGroupID);
                }
            }

            retVal.Add(_UnderwriterNameCode, underwriterNameID);
            retVal.Add(_UnderwritingGrpCode, underwritingGroupID);

            return retVal;
        }


        private int DeleteUnusedLookupValues(Update.PostProgress postProgress, bool isUW)
        {
            try
            {
                LookupTable targetLT = (isUW) ? ltUnderwriterNameCode : ltUnderwritingGrpCode;
                string targetField = (isUW) ? "underwriterName" : "underwritingGroup";

                string SQL = string.Format("DELETE LookupValue " +
                    "FROM LookupValue " +
                    "INNER JOIN LookupTable ON LookupValue.LookupTableID = LookupTable.LookupTableID " +
                    "WHERE LookupTable.LookupTableID = {0} " +
                    "AND (LookupValue.LookupValueID NOT IN(" +
                        "SELECT {1} " +
                        "FROM Loan " +
                        "WHERE {1} <> '00000000-0000-0000-0000-000000000000')" +
                     "AND LookupValue.LookupValueID NOT IN(" +
                        "SELECT {1} " +
                        "FROM LoanDAR " +
                        "WHERE {1} <> '00000000-0000-0000-0000-000000000000'))", DbConvert.ToSqlLiteral(targetLT.LookupTableID), targetField);
               int count = Db.ExecuteSQL(SQL);
                
                return count;
            }catch(Exception ex)
            {
                postProgress("ERROR: " + ex.Message + Environment.NewLine + ex.StackTrace);
                return -1;
            }
        }

        private DataTable GetSampledLoansToProcess(Update.PostProgress postProgress, bool isDAR)
        {
            string tableName = (isDAR) ? "LoanDAR" : "Loan";
            DataTable retval = new DataTable();
            //Get Sampled Loans with Underwriter Name or Underwriting Group values
            string SQLDAR = string.Format("SELECT LoanID, SampleTypeID, LoanNumber, underwriterName, underwritingGroup FROM {0} WHERE (underwriterName <> '00000000-0000-0000-0000-000000000000' OR underwritingGroup <> '00000000-0000-0000-0000-000000000000')", tableName);
            Db.ExecuteSQLFillDataTable(retval, SQLDAR);

            return retval;

        }
        private DataTable GetLoanImportRecord(Update.PostProgress postProgress, string loanNumber, bool isDAR)
        {
            string tableName = (isDAR) ? "DARLoanImport" : "LoanImport";
            DataTable retval = new DataTable();
            //Get Sampled Loans with Underwriter Name or Underwriting Group values
            string SQL = string.Format("SELECT LoanNumber, underwriterName, underwritingGroup FROM {0} WHERE LTRIM(RTRIM(LoanNumber)) = {1}", tableName, DbConvert.ToSqlLiteral(loanNumber));
            Db.ExecuteSQLFillDataTable(retval, SQL);

            return retval;
        }

        private Guid GetLookupValueID(Update.PostProgress postProgress, string code, LookupTable lookupTable, bool isUWName)
        {
            LookupValue lv = LookupServices.GetLookupValueByCode(lookupTable.LookupTableCode, code);
            if(lv != null)
            {
                return lv.LookupValueID;
            }
            else
            {
                LookupValue newLV = new LookupValue();
                newLV.LookupTableID = lookupTable.LookupTableID;
                newLV.Code = code;
                newLV.Name = (isUWName) ? "Underwriter Name" : "Underwriting Group";
                newLV.IsActive = true;
                newLV.SaveToDb();
                return newLV.LookupValueID;
            }
        }

        

    }
}