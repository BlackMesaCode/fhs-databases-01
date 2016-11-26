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

        public void ParseJsonMembers(string jsonRaw, MySqlConnection connection)
        {
            dynamic array = JsonConvert.DeserializeObject(jsonRaw);

            if (array.Count == 0)
                return;

            foreach (var member in array)
            {
                // seed photo table
                var memberPhotoId = "NULL";
                if (member.photo != null)
                {
                    var photoQuery = $@"INSERT INTO photo VALUES ({member.photo.photo_id}, '{member.photo.thumb_link}', '{member.photo.photo_link}', '{member.photo.highres_link}') ON DUPLICATE KEY UPDATE ID=ID;";
                    //Console.WriteLine(photoQuery);
                    var photoCmd = new MySqlCommand(photoQuery, connection);
                    photoCmd.ExecuteNonQuery();

                    memberPhotoId = member.photo.photo_id;
                }

                // seed topic table
                foreach (var topic in member.topics)
                {
                    var topicQuery = $@"INSERT INTO topic VALUES ({topic.id}, '{topic.urlkey}', '{topic.name}') ON DUPLICATE KEY UPDATE ID=ID;";
                    var topicCmd = new MySqlCommand(topicQuery, connection);
                    topicCmd.ExecuteNonQuery();
                }

                // fix lon/lat formatting to be sql compliant
                string lon = (string)member.lon;
                lon = lon.Replace(",", ".");
                string lat = (string)member.lat;
                lat = lat.Replace(",", ".");

                // escape backslash
                var bio = (string)member.bio;
                bio = !string.IsNullOrWhiteSpace(bio) ? bio.Replace(@"\", @"\\") : string.Empty;

                // seed member table
                var memberQuery = $@"INSERT INTO member VALUES ({member.id}, '{member.status}', '{member.city}', '{member.name}', '{bio}', {memberPhotoId}, '{lon}', '{lat}', '{member.state}', '{member.country}', '{member.link}', '{UnixTimeStampToValidMySQLTimeStamp(member.joined)}', '{UnixTimeStampToValidMySQLTimeStamp(member.visited)}') ON DUPLICATE KEY UPDATE ID=ID;";
                var memberCmd = new MySqlCommand(memberQuery, connection);
                memberCmd.ExecuteNonQuery();

                //Console.WriteLine(memberQuery);
            }

        }


    }
}
