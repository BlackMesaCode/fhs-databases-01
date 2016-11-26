using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Task01.MySQL
{
    class CreationService
    {
        public MySqlConnection Connection { get; private set; }

        public CreationService(MySqlConnection connection, string databaseName)
        {
            Connection = connection;
            CreateDatabaseSchema(databaseName);
            
        }

        public void CreateDatabaseSchema(string databaseName)
        {
            CreateDatabase(databaseName);
            SelectDatabase(databaseName);
            //CreateTables(databaseName);
        }

        public void CreateDatabase(string databaseName)
        {
            var query = $"CREATE DATABASE IF NOT EXISTS `{databaseName}`;";
            var cmd = new MySqlCommand(query, Connection);
            cmd.ExecuteNonQuery();
            Console.WriteLine($"Database: {databaseName} created");
        }

        public void SelectDatabase(string databaseName)
        {
            var query = $"USE {databaseName};";
            var cmd = new MySqlCommand(query, Connection);
            Console.WriteLine($"Database: {databaseName} selected");
            cmd.ExecuteNonQuery();
        }

        // # maybe add later: use show create table
        public void CreateTables(string databaseName)
        {
            Console.WriteLine($"Creating tables ...");
            var query = $"";
            var cmd = new MySqlCommand(query, Connection);
            cmd.ExecuteNonQuery();
        }



    }
}
