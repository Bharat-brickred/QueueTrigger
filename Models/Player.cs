using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlobTrigger.Models
{
    public class Player
    {
        public int PAI_0000200_Operator_Player_ID { get; set; }
        public int PAI_0000300_Operator_ID { get; set; }
        public int PAI_0000400_Operator_Platform_ID { get; set; }
        public string PAI_0000500_First_Name { get; set; }
        public string PAI_0000600_Middle_Name { get; set; }
        public string PAI_0000700_Last_Name { get; set; }
        public string PAI_0000800_Email_Address { get; set; }
        public long PAI_0000900_Phone_Number { get; set; }
        public int PAI_0001000_Country_Code_ID { get; set; }
        public DateTime PAI_0001100_Date_of_Birth { get; set; }
        public int PAI_0001200_Gender_ID { get; set; }
        public string PAI_0001201_Gender { get; set; }
        public int PAI_0001300_Nationality_ID { get; set; }
        public string PAI_0001400_Address { get; set; }
        public string PAI_0001500_City { get; set; }
        public string PAI_0001600_StateProvince { get; set; }
        public string PAI_0001700_Postal_Code { get; set; }
        public int PAI_0001800_Country_of_residence_ID { get; set; }
        public int PAI_0001900_ID_Verification_Status_ID { get; set; }
        public int PAI_0002000_Document_Type_ID { get; set; }
        public string PAI_0002100_Document_Number { get; set; }
        public DateTime PAI_0002200_Document_Expiry_Date { get; set; }
        public int PAI_0002300_Operator_Account_Status_ID { get; set; }
        public DateTime PAI_0002400_Account_Creation_Date { get; set; }
        public DateTime PAI_0002500_Last_Login_Date_and_Time { get; set; }
        public decimal PAI_0002600_Account_Balance { get; set; }
        public int PAI_0002800_Marketing_Opt_in_Status_ID { get; set; }
        public DateTime PAI_0002900_Marketing_Opt_in_Status_Updated { get; set; }
        public DateTime PAI_0003000_Date_and_Time_of_Last_Marketing_Message { get; set; }
        public int PAI_0003100_AML_Risk_Assessment_Score_ID { get; set; }
        public string PAI_0003200_Payment_Method_Token { get; set; }
        public string PAI_0003300_Payment_Method_Token { get; set; }
        public string PAI_0003400_Payment_Method_Token { get; set; }
        public string PAI_0003500_Payment_Method_Token { get; set; }
        public string PAI_0003600_Payment_Method_Token { get; set; }
        public DateTime PAI_0003700_Last_Stake_Date_and_Time { get; set; }
        public decimal PAI_0003800_Monthly_Deposits_Last_30_Days_Total_Deposits { get; set; }
        public decimal PAI_0003900_Monthly_Withdrawals_Last_30_Days { get; set; }
        public int PAI_0004000_Enhanced_Due_Diligence_Status_ID { get; set; }

    }
}
