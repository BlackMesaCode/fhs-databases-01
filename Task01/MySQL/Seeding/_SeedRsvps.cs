using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Task01.MySQL.Seeding
{
    public partial class SeedService
    {

        public void ParseJsonRsvps(string jsonRaw, MySqlConnection connection)
        {
            dynamic array = JsonConvert.DeserializeObject(jsonRaw);

            if (array.Count == 0)
                return;

            foreach (var rsvp in array)
            {

                // as a few event ids in the dataset are containing characters instead of numbers, we are hashing the characters to get a valid primary key
                var eventId = (string)rsvp["event"].id;
                if (!eventId.All(char.IsDigit))
                    eventId = eventId.GetHashCode().ToString();

                // seed rsvp table
                var rsvpQuery = $@"INSERT INTO rsvp VALUES ({rsvp.rsvp_id}, '{UnixTimeStampToValidMySQLTimeStamp(rsvp.mtime)}', '{UnixTimeStampToValidMySQLTimeStamp(rsvp.created)}', {rsvp.guests}, '{rsvp.response}', {rsvp.member.member_id}, {eventId}) ON DUPLICATE KEY UPDATE ID=ID;";
                var rsvpCmd = new MySqlCommand(rsvpQuery, connection);
                rsvpCmd.ExecuteNonQuery();

                //Console.WriteLine(rsvpQuery);
            }

        }


    }
}
