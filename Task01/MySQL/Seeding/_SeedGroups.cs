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

        public void ParseJsonGroups(string jsonRaw, MySqlConnection connection)
        {
            dynamic array = JsonConvert.DeserializeObject(jsonRaw);

            if (array.Count == 0)
                return;

            foreach (var group in array)
            {
                // seed photo table
                var groupPhotoId = "NULL";
                if (group.group_photo != null)
                {
                    var photoQuery = $@"INSERT INTO photo VALUES ({group.group_photo.photo_id}, '{group.group_photo.thumb_link}', '{group.group_photo.photo_link}', '{group.group_photo.highres_link}') ON DUPLICATE KEY UPDATE ID=ID;";
                    var photoCmd = new MySqlCommand(photoQuery, connection);
                    photoCmd.ExecuteNonQuery();
                    groupPhotoId = group.group_photo.photo_id;
                }

                // seed topic table
                foreach (var topic in group.topics)
                {
                    var topicQuery = $@"INSERT INTO topic VALUES ({topic.id}, '{topic.urlkey}', '{topic.name}') ON DUPLICATE KEY UPDATE ID=ID;";
                    var topicCmd = new MySqlCommand(topicQuery, connection);
                    topicCmd.ExecuteNonQuery();
                }

                // seed category table
                var groupCategoryId = "NULL";
                if (group.category != null)
                {
                    var categoryQuery = $@"INSERT INTO category VALUES ({group.category.id}, '{group.category.name}', '{group.category.shortname}') ON DUPLICATE KEY UPDATE ID=ID;";
                    var photoCmd = new MySqlCommand(categoryQuery, connection);
                    photoCmd.ExecuteNonQuery();
                    groupCategoryId = group.category.id;
                }

                // fix lon/lat formatting to be sql compliant
                string lon = (string)group.lon;
                lon = lon.Replace(",", ".");
                string lat = (string)group.lat;
                lat = lat.Replace(",", ".");
                string rating = (string)group.rating;
                rating = rating.Replace(",", ".");

                var groupPhotoID = group.group_photo != null ? (string.IsNullOrWhiteSpace((string)group.group_photo.photo_id) ? "NULL" : group.group_photo.photo_id) : "NULL";

                // seet group table - careful: as group is a reserved mysql keyword, we have to embrace it with ticks
                var groupQuery = $@"INSERT INTO `group` VALUES ({group.id}, '{group.name}', '{group.city}', '{rating}', '{group.description}', '{group.join_mode}', '{group.country}', '{group.who}', '{group.visibility}', '{lon}', '{lat}', '{UnixTimeStampToValidMySQLTimeStamp(group.created)}', '{group.state}', '{group.link}', {group.members}, '{group.urlname}', '{group.timezone}', {group.organizer.member_id}, {groupCategoryId}, {groupPhotoID}) ON DUPLICATE KEY UPDATE ID=ID;";
                var groupCmd = new MySqlCommand(groupQuery, connection);
                groupCmd.ExecuteNonQuery();
            }
            

            

        }


    }
}
