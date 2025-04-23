using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using QueueTrigger.Models;

namespace QueueTrigger.Helpers
{
    public static class PlayerDataHelper
    {
        public static async Task SavePlayer(Player player, string connectionString, ILogger log)
        {
            if (player == null)
            {
                log.LogWarning("Player object is null. Skipping database insert.");
                return;
            }

            const string query = @"
                INSERT INTO Players (
                    PAI_0000200_Operator_Player_ID, PAI_0000300_Operator_ID, PAI_0000400_Operator_Platform_ID,
                    PAI_0000500_First_Name, PAI_0000600_Middle_Name, PAI_0000700_Last_Name,
                    PAI_0000800_Email_Address, PAI_0000900_Phone_Number, PAI_0001000_Country_Code_ID,
                    PAI_0001100_Date_of_Birth, PAI_0001200_Gender_ID, PAI_0001201_Gender,
                    PAI_0001300_Nationality_ID, PAI_0001400_Address, PAI_0001500_City, PAI_0001600_StateProvince,
                    PAI_0001700_Postal_Code, PAI_0001800_Country_of_residence_ID,
                    PAI_0001900_ID_Verification_Status_ID, PAI_0002000_Document_Type_ID,
                    PAI_0002100_Document_Number, PAI_0002200_Document_Expiry_Date,
                    PAI_0002300_Operator_Account_Status_ID, PAI_0002400_Account_Creation_Date,
                    PAI_0002500_Last_Login_Date_and_Time, PAI_0002600_Account_Balance,
                    PAI_0002800_Marketing_Opt_in_Status_ID, PAI_0002900_Marketing_Opt_in_Status_Updated,
                    PAI_0003000_Date_and_Time_of_Last_Marketing_Message, PAI_0003100_AML_Risk_Assessment_Score_ID,
                    PAI_0003200_Payment_Method_Token, PAI_0003300_Payment_Method_Token,
                    PAI_0003400_Payment_Method_Token, PAI_0003500_Payment_Method_Token,
                    PAI_0003600_Payment_Method_Token, PAI_0003700_Last_Stake_Date_and_Time,
                    PAI_0003800_Monthly_Deposits_Last_30_Days_Total_Deposits, PAI_0003900_Monthly_Withdrawals_Last_30_Days,
                    PAI_0004000_Enhanced_Due_Diligence_Status_ID
                )
                VALUES (
                    @OperatorPlayerID, @OperatorID, @PlatformID, @FirstName, @MiddleName, @LastName,
                    @Email, @Phone, @CountryCode, @DOB, @GenderID, @Gender,
                    @NationalityID, @Address, @City, @State, @PostalCode, @CountryOfResidence,
                    @IDVerificationStatus, @DocumentTypeID, @DocumentNumber, @DocumentExpiry,
                    @AccountStatusID, @AccountCreated, @LastLogin, @Balance,
                    @MarketingOptIn, @MarketingOptInUpdated, @LastMarketingMessage,
                    @AmlScore, @Token1, @Token2, @Token3, @Token4, @Token5,
                    @LastStake, @MonthlyDeposits, @MonthlyWithdrawals, @EDDStatus
                );";

            try
            {
                using var conn = new SqlConnection(connectionString);
                using var cmd = new SqlCommand(query, conn);

                AddPlayerParameters(cmd, player);

                await conn.OpenAsync();
                await cmd.ExecuteNonQueryAsync();

                log.LogInformation("Player inserted successfully: ID {PlayerID}", player.PAI_0000200_Operator_Player_ID);
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Error inserting player into database.");
                throw;
            }
        }

        private static void AddPlayerParameters(SqlCommand cmd, Player p)
        {
            object DBNullIfNull(object value) => value ?? DBNull.Value;

            cmd.Parameters.AddWithValue("@OperatorPlayerID", p.PAI_0000200_Operator_Player_ID);
            cmd.Parameters.AddWithValue("@OperatorID", p.PAI_0000300_Operator_ID);
            cmd.Parameters.AddWithValue("@PlatformID", p.PAI_0000400_Operator_Platform_ID);
            cmd.Parameters.AddWithValue("@FirstName", DBNullIfNull(p.PAI_0000500_First_Name));
            cmd.Parameters.AddWithValue("@MiddleName", DBNullIfNull(p.PAI_0000600_Middle_Name));
            cmd.Parameters.AddWithValue("@LastName", DBNullIfNull(p.PAI_0000700_Last_Name));
            cmd.Parameters.AddWithValue("@Email", DBNullIfNull(p.PAI_0000800_Email_Address));
            cmd.Parameters.AddWithValue("@Phone", p.PAI_0000900_Phone_Number);
            cmd.Parameters.AddWithValue("@CountryCode", p.PAI_0001000_Country_Code_ID);
            cmd.Parameters.AddWithValue("@DOB", p.PAI_0001100_Date_of_Birth);
            cmd.Parameters.AddWithValue("@GenderID", p.PAI_0001200_Gender_ID);
            cmd.Parameters.AddWithValue("@Gender", DBNullIfNull(p.PAI_0001201_Gender));
            cmd.Parameters.AddWithValue("@NationalityID", p.PAI_0001300_Nationality_ID);
            cmd.Parameters.AddWithValue("@Address", DBNullIfNull(p.PAI_0001400_Address));
            cmd.Parameters.AddWithValue("@City", DBNullIfNull(p.PAI_0001500_City));
            cmd.Parameters.AddWithValue("@State", DBNullIfNull(p.PAI_0001600_StateProvince));
            cmd.Parameters.AddWithValue("@PostalCode", DBNullIfNull(p.PAI_0001700_Postal_Code));
            cmd.Parameters.AddWithValue("@CountryOfResidence", p.PAI_0001800_Country_of_residence_ID);
            cmd.Parameters.AddWithValue("@IDVerificationStatus", p.PAI_0001900_ID_Verification_Status_ID);
            cmd.Parameters.AddWithValue("@DocumentTypeID", p.PAI_0002000_Document_Type_ID);
            cmd.Parameters.AddWithValue("@DocumentNumber", DBNullIfNull(p.PAI_0002100_Document_Number));
            cmd.Parameters.AddWithValue("@DocumentExpiry", p.PAI_0002200_Document_Expiry_Date);
            cmd.Parameters.AddWithValue("@AccountStatusID", p.PAI_0002300_Operator_Account_Status_ID);
            cmd.Parameters.AddWithValue("@AccountCreated", p.PAI_0002400_Account_Creation_Date);
            cmd.Parameters.AddWithValue("@LastLogin", p.PAI_0002500_Last_Login_Date_and_Time);
            cmd.Parameters.AddWithValue("@Balance", p.PAI_0002600_Account_Balance);
            cmd.Parameters.AddWithValue("@MarketingOptIn", p.PAI_0002800_Marketing_Opt_in_Status_ID);
            cmd.Parameters.AddWithValue("@MarketingOptInUpdated", p.PAI_0002900_Marketing_Opt_in_Status_Updated);
            cmd.Parameters.AddWithValue("@LastMarketingMessage", p.PAI_0003000_Date_and_Time_of_Last_Marketing_Message);
            cmd.Parameters.AddWithValue("@AmlScore", p.PAI_0003100_AML_Risk_Assessment_Score_ID);
            cmd.Parameters.AddWithValue("@Token1", DBNullIfNull(p.PAI_0003200_Payment_Method_Token));
            cmd.Parameters.AddWithValue("@Token2", DBNullIfNull(p.PAI_0003300_Payment_Method_Token));
            cmd.Parameters.AddWithValue("@Token3", DBNullIfNull(p.PAI_0003400_Payment_Method_Token));
            cmd.Parameters.AddWithValue("@Token4", DBNullIfNull(p.PAI_0003500_Payment_Method_Token));
            cmd.Parameters.AddWithValue("@Token5", DBNullIfNull(p.PAI_0003600_Payment_Method_Token));
            cmd.Parameters.AddWithValue("@LastStake", p.PAI_0003700_Last_Stake_Date_and_Time);
            cmd.Parameters.AddWithValue("@MonthlyDeposits", p.PAI_0003800_Monthly_Deposits_Last_30_Days_Total_Deposits);
            cmd.Parameters.AddWithValue("@MonthlyWithdrawals", p.PAI_0003900_Monthly_Withdrawals_Last_30_Days);
            cmd.Parameters.AddWithValue("@EDDStatus", p.PAI_0004000_Enhanced_Due_Diligence_Status_ID);
        }
    }
}
