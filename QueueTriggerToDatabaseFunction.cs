
using BlobTrigger.Helpers;
using BlobTrigger.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace BlobTrigger
{
    public class BlobUploadToDatabaseFunction
    {
        private readonly ILogger<BlobUploadToDatabaseFunction> _logger;
        private readonly string _connectionString;

        public BlobUploadToDatabaseFunction(ILogger<BlobUploadToDatabaseFunction> logger, IConfiguration configuration)
        {
            _logger = logger;
            _connectionString = "Server=tcp:playersdbserver.database.windows.net,1433;Initial Catalog=playerdb;Persist Security Info=False;User ID=bharat;Password=Yadav@123;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"; // Environment.GetEnvironmentVariable("SQL_CONNECTION_STRING");
        }

        [Function(nameof(BlobUploadToDatabaseFunction))]
        public async Task<IActionResult> Run([BlobTrigger("player/{name}", Connection = "AzureWebJobsStorage")] Stream stream, string name)
        {
            using var blobStreamReader = new StreamReader(stream);
            var content = await blobStreamReader.ReadToEndAsync();
            _logger.LogInformation($"C# Blob trigger function processed blob\n Name: {name} \n Data: {content}");

            try
            {
                var players = JsonConvert.DeserializeObject<List<Player>>(content);
                _logger.LogInformation("Successfully deserialized the JSON content.");

                await PlayerDataHelper.SavePlayersInBatch(players, _connectionString, _logger);
                return new OkObjectResult("Data successfully saved to the database.");
            }
            catch (JsonException ex)
            {
                _logger.LogError($"Failed to deserialize the content: {ex.Message}");
                return new BadRequestObjectResult("Invalid JSON format.");
            }
            catch (Exception ex)
            {
                _logger.LogError($"An error occurred: {ex.Message}");
                return new StatusCodeResult(StatusCodes.Status500InternalServerError);
            }
        }
    }
}
