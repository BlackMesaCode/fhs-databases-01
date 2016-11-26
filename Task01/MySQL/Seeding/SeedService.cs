using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Task01.MySQL.Seeding
{
    public partial class SeedService
    {

        public MySqlConnection Connection { get; set; }

        public static readonly string ExePath = System.Reflection.Assembly.GetExecutingAssembly().Location;
        public static readonly string ExeDir = Path.GetDirectoryName(ExePath);

        public static readonly string DataMiniDirName = "meetup-data-mini";
        public static readonly string DataExtractDirName = "meetup-data-extract";
        public static readonly string DataFullDirName = "meetup-data-full";
        

        public SeedService(MySqlConnection connection)
        {
            Connection = connection;

            //TruncateDatabase(); // removes all rows from all tables  !! CAREFUL !!

            //ParseFiles(DataFullDirName, "members", ParseJsonMembers);
            //ParseFiles(DataFullDirName, "groups", ParseJsonGroups);
            //ParseFiles(DataFullDirName, "events", ParseJsonEvents);

            ParseFiles(DataFullDirName, "rsvps", ParseJsonRsvps);


        }


        public void RsvpFolderFlatter()
        {
            var dataSetPath = Path.Combine(ExeDir, DataFullDirName);
            var rsvpFolderPath = Path.Combine(dataSetPath, "rsvps");

            var folders = Directory.GetDirectories(rsvpFolderPath);

            var index = 1;
            foreach (var folder in folders)
            {
                Console.WriteLine($"{index}/{folders.Length}: Flat folder: {folder}");
                var files = Directory.GetFiles(folder);
                foreach (var file in files)
                {
                    var destinationPath = Path.Combine(rsvpFolderPath, Path.GetFileName(file));
                    File.Move(file, destinationPath);
                }
                Directory.Delete(folder);
                index++;
            }
            Console.WriteLine("Flattening finished.");
        }


        public static int ParsedFilesCount = 0;

        public async void ParseFiles(string dataSetName, string subfolder, Action<string, MySqlConnection> parser)
        {
            ParsedFilesCount = 0;

            var dataSetPath = Path.Combine(ExeDir, dataSetName);
            var tableFolderPath = Path.Combine(dataSetPath, subfolder);

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var filesInDirectory = Directory.GetFiles(tableFolderPath);
            var filesCount = filesInDirectory.Length;

            // parallize task for faster insertion
            var numberOfParallelizations = 30;
            var numberOfFilesPerPart = (int) Math.Floor(filesCount / (double) numberOfParallelizations);

            List<Task> tasks = new List<Task>(); 

            for (int part = 1; part <= numberOfParallelizations; part++)
            {
                // create a copy of part to prevent lambda function closuring the part variable
                var partCopy = part;

                // launch multiple tasks (aka threads)
                tasks.Add(Task.Run(() => ParsePartOfFolder(numberOfParallelizations, filesCount,
                    partCopy, numberOfFilesPerPart, filesInDirectory, parser, stopwatch)));
            }

            // wait for all tasks to complete
            await Task.WhenAll(tasks);

            Console.WriteLine($"Finished parsing '{subfolder}' after {stopwatch.Elapsed.Minutes} minutes");
        }


        public void ParsePartOfFolder(int numberOfParallelizations, int filesCount, int part, int numberOfFilesPerPart, 
            string[] filesInDirectory, Action<string, MySqlConnection> parser, Stopwatch stopwatch)
        {
            var abortCondition = (part == numberOfParallelizations) ? filesCount : part * numberOfFilesPerPart; // to ensure we get the last files, that otherwise would remain due to rounding
            for (int fileIndex = (part - 1) * numberOfFilesPerPart; fileIndex < abortCondition; fileIndex++)
            {

                var filePath = filesInDirectory[fileIndex];
                Console.WriteLine($"Start parsing file: {filePath}");
                var jsonRaw = ReadFile(filePath);
                jsonRaw = jsonRaw.Replace("'", "''");

                var connectionString = ConnectionService.CreateConnectionString("127.0.0.1", "root", "pass", "meetup");

                // each threads needs his own mysql connection
                using (ConnectionService connectionService = new ConnectionService(connectionString))
                {
                    parser(jsonRaw, connectionService.Connection);
                }
                ParsedFilesCount++;
                Console.WriteLine($"Finished parsing file: {ParsedFilesCount}/{filesCount} after {stopwatch.Elapsed.Minutes} minutes");
            }
        }

        // Clears all rows in all tables - to make it work we have to disable foreign key checks temporarily
        public void TruncateDatabase()
        {
            var showTablesQuery = $"SHOW TABLES";
            var showTableCmd = new MySqlCommand(showTablesQuery, Connection);
            showTableCmd.ExecuteNonQuery();

            MySqlDataReader reader = showTableCmd.ExecuteReader();

            var query = $"SET foreign_key_checks = 0; ";
            while (reader.Read())
            {
                string table = (string)reader[0];
                query += $"TRUNCATE `{table}`;";
            }
            query += "SET FOREIGN_KEY_CHECKS=1;";
            reader.Close();

            var cmd = new MySqlCommand(query, Connection);
            cmd.ExecuteNonQuery();

            Console.WriteLine($"Truncated all tables.");
        }


        public string ReadFile(string filename)
        {
            using (StreamReader r = new StreamReader(filename))
            {
                return r.ReadToEnd();
            }
        }

        public static string UnixTimeStampToValidMySQLTimeStamp(dynamic unixTimeStamp)
        {
            double unixTimeStampString = Convert.ToDouble((string)unixTimeStamp);
            DateTime dtDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            dtDateTime = dtDateTime.AddMilliseconds(unixTimeStampString).ToLocalTime();
            return dtDateTime.ToString("yyyy-MM-dd HH:mm:ss");
        }




    }
}
