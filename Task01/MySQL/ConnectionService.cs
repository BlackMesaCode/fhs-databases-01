using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Task01.MySQL
{
    public class ConnectionService : IDisposable
    {
        public MySqlConnection Connection { get; private set; }

        public ConnectionService(string connectionString)
        {
            Connection = new MySqlConnection(connectionString);

            OpenConnection();
        }

        public void OpenConnection()
        {
            try
            {
                Connection.Open();
            }
            catch (MySqlException ex)
            {
                Console.WriteLine(ex.Message);
            }
        }


        public void CloseConnection()
        {
            try
            {
                Connection.Close();
            }
            catch (MySqlException ex)
            {
                Console.WriteLine(ex.Message);
            }
        }



        public static string CreateConnectionString(string server, string user, string pass, string database = "")
        {
            MySqlConnectionStringBuilder connectionString = new MySqlConnectionStringBuilder();
            connectionString.Server = server;
            connectionString.UserID = user;
            connectionString.Password = pass;
            connectionString.Database = database;
            return connectionString.ToString();

        }


        public void Dispose()
        {
            Connection.Close();
        }

    }
}
