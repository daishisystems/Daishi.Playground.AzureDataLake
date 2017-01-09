using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using Microsoft.Azure.Management.DataLake.Analytics;
using Microsoft.Azure.Management.DataLake.Analytics.Models;
using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.Azure.Management.DataLake.Store.Models;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest.Azure.Authentication;

namespace Daishi.Playground.AzureDataLake
{
    internal class Program
    {
        private static DataLakeStoreAccountManagementClient _adlsClient;
        private static DataLakeStoreFileSystemManagementClient _adlsFileSystemClient;
        private static DataLakeAnalyticsJobManagementClient _adlaJobClient;

        private static string _adlsAccountName;
        private static string _resourceGroupName;
        private static string _location;
        private static string _subId;

        private static void Main(string[] args)
        {
            if (args == null || args.Length != 4)
            {
                throw new Exception("Invalid arguments.");
            }

            var dataSource = args[0];
            var year = int.Parse(args[1]);
            var month = int.Parse(args[2]);
            var day = int.Parse(args[3]);

            _adlsAccountName = "aegisdatalakestore";
            // TODO: Replace this value with the name of your existing Data Lake Store account.
            _resourceGroupName = "shieldanalytics";
            // TODO: Replace this value with the name of the resource group containing your Data Lake Store account.
            _location = "East US 2";
            _subId = "a9038fb7-3b06-4cac-bbf5-755254b9960d";

            const string localFolderPath = @"C:\Data Lake Local\";
            // TODO: Make sure this exists and can be overwritten.
            var localFilePath = localFolderPath + "file.txt"; // TODO: Make sure this exists and can be overwritten.
            const string remoteFolderPath = "/data_lake_path/";
            var remoteFilePath = remoteFolderPath + "file.txt";

            // User login via interactive popup
            // Use the client ID of an existing AAD "Native Client" application.
            //SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());
            //const string domain = "ryanair.com";
            //// Replace this string with the user's Azure Active Directory tenant ID or domain name, if needed.
            //const string nativeClientAppClientId = "1950a258-227b-4e31-a9cf-717495945fc2";
            //var activeDirectoryClientSettings = ActiveDirectoryClientSettings.UsePromptOnly(nativeClientAppClientId,
            //    new Uri("urn:ietf:wg:oauth:2.0:oob"));
            //var creds = UserTokenProvider.LoginWithPromptAsync(domain, activeDirectoryClientSettings).Result;

            // Service principal / application authentication with certificate
            // Use the client ID and certificate of an existing AAD "Web App" application.

            var fs = new FileStream(@"C:\Users\mooneyp\Downloads\cert.pfx", FileMode.Open);
            var certBytes = new byte[fs.Length];
            fs.Read(certBytes, 0, (int) fs.Length);
            fs.Close();
            var cert = new X509Certificate2(certBytes, "M3c54n1c4L\"1");
            Console.WriteLine(cert.GetPublicKey());
            Console.WriteLine(cert.GetPublicKeyString());

            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());
            var domain = "ryanair.com";
            var webApp_clientId = "e414f5b8-91af-410d-8740-330e077cbb5f";
            var clientCert = cert;
            var clientAssertionCertificate = new ClientAssertionCertificate(webApp_clientId, clientCert);
            var creds =
                ApplicationTokenProvider.LoginSilentWithCertificateAsync(domain, clientAssertionCertificate).Result;

            _adlsClient = new DataLakeStoreAccountManagementClient(creds);
            _adlsFileSystemClient = new DataLakeStoreFileSystemManagementClient(creds);
            _adlaJobClient = new DataLakeAnalyticsJobManagementClient(creds);

            _adlsClient.SubscriptionId = _subId;

            var jobId = CreateDataSource(dataSource);
            Console.WriteLine("Creating {0}...", dataSource);
            WaitForJob(jobId);
            Console.WriteLine("{0} created.", dataSource);

            Console.WriteLine("Reading files...");
            var files = ListItems($"/events/v5/{year}/{month}/{day}");

            if (!files.Any())
            {
                Console.WriteLine("Nothing to load.");
                Console.ReadLine();
                return;
            }

            var counter = 1;

            foreach (var file in files)
            {
                Console.WriteLine($"Loading {dataSource} file #{counter}...");

                jobId = SubmitJobByPath(
                    localFolderPath + "ExtractScript.txt",
                    file.PathSuffix,
                    dataSource,
                    $"Load {dataSource} file #{counter}");

                WaitForJob(jobId);
                Console.WriteLine($"{dataSource} file #{counter} loaded.");
                counter++;
            }

            Console.WriteLine("All files loaded.");
            Console.ReadLine();
        }

        private static Guid CreateDataSource(
            string dataSource,
            string scriptPath = @"C:\Data Lake Local\CreateScript.txt",
            int degreeOfParallelism = 250)
        {
            var script = File.ReadAllText(scriptPath);

            script = script.Replace("[DATASOURCE]", dataSource);

            var jobId = Guid.NewGuid();
            var properties = new USqlJobProperties(script);

            var parameters = new JobInformation(
                $"Create {dataSource}",
                JobType.USql,
                properties,
                priority: 1,
                degreeOfParallelism: degreeOfParallelism,
                jobId: jobId);

            _adlaJobClient.Job.Create("shield", jobId, parameters);
            return jobId;
        }

        // List files and directories
        private static List<FileStatusProperties> ListItems(string directoryPath)
        {
            return _adlsFileSystemClient
                .FileSystem
                .ListFileStatus(_adlsAccountName, directoryPath).
                FileStatuses
                .FileStatus
                .ToList();
        }

        private static Guid SubmitJobByPath(
            string scriptPath,
            string fileName,
            string dataSource,
            string jobName,
            int degreeOfParallelism = 250)
        {
            var script = File.ReadAllText(scriptPath);

            script = script.Replace("[FILENAME]", fileName);
            script = script.Replace("[DATASOURCE]", dataSource);

            var jobId = Guid.NewGuid();
            var properties = new USqlJobProperties(script);
            var parameters = new JobInformation(
                jobName,
                JobType.USql,
                properties,
                priority: 1,
                degreeOfParallelism: degreeOfParallelism,
                jobId: jobId);

            var jobInfo = _adlaJobClient.Job.Create("shield", jobId, parameters);

            return jobId;
        }

        private static void WaitForNewline(string reason, string nextAction = "")
        {
            Console.WriteLine(reason + "\r\nPress ENTER to continue...");

            Console.ReadLine();

            if (!string.IsNullOrWhiteSpace(nextAction))
                Console.WriteLine(nextAction);
        }

        private static JobResult WaitForJob(Guid jobId)
        {
            var jobInfo = _adlaJobClient.Job.Get("shield", jobId);
            while (jobInfo.State != JobState.Ended)
            {
                jobInfo = _adlaJobClient.Job.Get("shield", jobId);
            }
            return jobInfo.Result ?? JobResult.Failed;
        }
    }
}