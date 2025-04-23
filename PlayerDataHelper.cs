using System.Data;
using BlobTrigger.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace BlobTrigger.Helpers
{
    public static class PlayerDataHelper
    {
        public static async Task SavePlayersInBatch(List<Player> players, string connectionString, ILogger log)
        {
            if (players == null || players.Count == 0)
            {
                log.LogWarning("No players to insert into the database.");
                return;
            }

            var playerDataTable = CreatePlayerDataTable(players);

            try
            {
                using var connection = new SqlConnection(connectionString);
                await connection.OpenAsync();

                using var command = new SqlCommand("MergePlayers", connection)
                {
                    CommandType = CommandType.StoredProcedure
                };

                var parameter = command.Parameters.AddWithValue("@PlayerTable", playerDataTable);
                parameter.SqlDbType = SqlDbType.Structured;
                parameter.TypeName = "PlayerTableType";

                await command.ExecuteNonQueryAsync();
                log.LogInformation("Player data successfully merged into the database.");
            }
            catch (Exception ex)
            {
                log.LogError($"Error while saving data to the database: {ex.Message}");
                throw ex;
            }
        }

        private static DataTable CreatePlayerDataTable(List<Player> players)
        {
            var dataTable = new DataTable();

            // Define columns in DataTable
            var columns = new[]
            {
                new DataColumn("PAI_0000200_Operator_Player_ID", typeof(int)),
                new DataColumn("PAI_0000300_Operator_ID", typeof(int)),
                new DataColumn("PAI_0000400_Operator_Platform_ID", typeof(int)),
                new DataColumn("PAI_0000500_First_Name", typeof(string)),
                new DataColumn("PAI_0000600_Middle_Name", typeof(string)),
                new DataColumn("PAI_0000700_Last_Name", typeof(string)),
                new DataColumn("PAI_0000800_Email_Address", typeof(string)),
                new DataColumn("PAI_0000900_Phone_Number", typeof(long)),
                new DataColumn("PAI_0001000_Country_Code_ID", typeof(int)),
                new DataColumn("PAI_0001100_Date_of_Birth", typeof(DateTime)),
                new DataColumn("PAI_0001200_Gender_ID", typeof(int)),
                new DataColumn("PAI_0001201_Gender", typeof(string)),
                new DataColumn("PAI_0001300_Nationality_ID", typeof(int)),
                new DataColumn("PAI_0001400_Address", typeof(string)),
                new DataColumn("PAI_0001500_City", typeof(string)),
                new DataColumn("PAI_0001600_StateProvince", typeof(string)),
                new DataColumn("PAI_0001700_Postal_Code", typeof(string)),
                new DataColumn("PAI_0001800_Country_of_residence_ID", typeof(int)),
                new DataColumn("PAI_0001900_ID_Verification_Status_ID", typeof(int)),
                new DataColumn("PAI_0002000_Document_Type_ID", typeof(int)),
                new DataColumn("PAI_0002100_Document_Number", typeof(string)),
                new DataColumn("PAI_0002200_Document_Expiry_Date", typeof(DateTime)),
                new DataColumn("PAI_0002300_Operator_Account_Status_ID", typeof(int)),
                new DataColumn("PAI_0002400_Account_Creation_Date", typeof(DateTime)),
                new DataColumn("PAI_0002500_Last_Login_Date_and_Time", typeof(DateTime)),
                new DataColumn("PAI_0002600_Account_Balance", typeof(decimal)),
                new DataColumn("PAI_0002800_Marketing_Opt_in_Status_ID", typeof(int)),
                new DataColumn("PAI_0002900_Marketing_Opt_in_Status_Updated", typeof(DateTime)),
                new DataColumn("PAI_0003000_Date_and_Time_of_Last_Marketing_Message", typeof(DateTime)),
                new DataColumn("PAI_0003100_AML_Risk_Assessment_Score_ID", typeof(int)),
                new DataColumn("PAI_0003200_Payment_Method_Token", typeof(string)),
                new DataColumn("PAI_0003300_Payment_Method_Token", typeof(string)),
                new DataColumn("PAI_0003400_Payment_Method_Token", typeof(string)),
                new DataColumn("PAI_0003500_Payment_Method_Token", typeof(string)),
                new DataColumn("PAI_0003600_Payment_Method_Token", typeof(string)),
                new DataColumn("PAI_0003700_Last_Stake_Date_and_Time", typeof(DateTime)),
                new DataColumn("PAI_0003800_Monthly_Deposits_Last_30_Days_Total_Deposits", typeof(decimal)),
                new DataColumn("PAI_0003900_Monthly_Withdrawals_Last_30_Days", typeof(decimal)),
                new DataColumn("PAI_0004000_Enhanced_Due_Diligence_Status_ID", typeof(int))
            };

            dataTable.Columns.AddRange(columns);

            // Add rows to DataTable
            foreach (var player in players)
            {
                dataTable.Rows.Add(
                    player.PAI_0000200_Operator_Player_ID,
                    player.PAI_0000300_Operator_ID,
                    player.PAI_0000400_Operator_Platform_ID,
                    player.PAI_0000500_First_Name,
                    player.PAI_0000600_Middle_Name,
                    player.PAI_0000700_Last_Name,
                    player.PAI_0000800_Email_Address,
                    player.PAI_0000900_Phone_Number,
                    player.PAI_0001000_Country_Code_ID,
                    player.PAI_0001100_Date_of_Birth,
                    player.PAI_0001200_Gender_ID,
                    player.PAI_0001201_Gender,
                    player.PAI_0001300_Nationality_ID,
                    player.PAI_0001400_Address,
                    player.PAI_0001500_City,
                    player.PAI_0001600_StateProvince,
                    player.PAI_0001700_Postal_Code,
                    player.PAI_0001800_Country_of_residence_ID,
                    player.PAI_0001900_ID_Verification_Status_ID,
                    player.PAI_0002000_Document_Type_ID,
                    player.PAI_0002100_Document_Number,
                    player.PAI_0002200_Document_Expiry_Date,
                    player.PAI_0002300_Operator_Account_Status_ID,
                    player.PAI_0002400_Account_Creation_Date,
                    player.PAI_0002500_Last_Login_Date_and_Time,
                    player.PAI_0002600_Account_Balance,
                    player.PAI_0002800_Marketing_Opt_in_Status_ID,
                    player.PAI_0002900_Marketing_Opt_in_Status_Updated,
                    player.PAI_0003000_Date_and_Time_of_Last_Marketing_Message,
                    player.PAI_0003100_AML_Risk_Assessment_Score_ID,
                    player.PAI_0003200_Payment_Method_Token,
                    player.PAI_0003300_Payment_Method_Token,
                    player.PAI_0003400_Payment_Method_Token,
                    player.PAI_0003500_Payment_Method_Token,
                    player.PAI_0003600_Payment_Method_Token,
                    player.PAI_0003700_Last_Stake_Date_and_Time,
                    player.PAI_0003800_Monthly_Deposits_Last_30_Days_Total_Deposits,
                    player.PAI_0003900_Monthly_Withdrawals_Last_30_Days,
                    player.PAI_0004000_Enhanced_Due_Diligence_Status_ID
                );
            }

            return dataTable;
        }
    }
}
