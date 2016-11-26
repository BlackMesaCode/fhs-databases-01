using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Task01.MySQL.Seeding
{
    public partial class SeedService
    {

        public void ParseJsonEvents(string jsonRaw, MySqlConnection connection)
        {
            dynamic array = JsonConvert.DeserializeObject(jsonRaw);

            if (array.Count == 0)
                return;

            foreach (var meetupEvent in array)  // we have to call it meetupEvent because event is a reserved c# language keyword
            {

                // seed venue table
                var meetupEventVenueId = "NULL";
                if (meetupEvent.venue != null)
                {
                    string lon = (string)meetupEvent.venue.lon;
                    lon = lon.Replace(",", ".");
                    string lat = (string)meetupEvent.venue.lat;
                    lat = lat.Replace(",", ".");
                    string repinned = meetupEvent.venue.repinned == "false" ? "FALSE" : "TRUE";
                    string zip = string.IsNullOrWhiteSpace((string)meetupEvent.venue.zip) ? "NULL" : meetupEvent.venue.zip;

                    var venueQuery = $@"INSERT INTO venue VALUES ({meetupEvent.venue.id}, '{lon}', '{lat}', '{meetupEvent.venue.name}', '{zip}', '{meetupEvent.venue.country}', '{meetupEvent.venue.phone}', '{meetupEvent.venue.state}', '{meetupEvent.venue.address_1}', '{meetupEvent.venue.address_2}', {repinned}, '{meetupEvent.venue.city}') ON DUPLICATE KEY UPDATE ID=ID;";
                    var venueCmd = new MySqlCommand(venueQuery, connection);
                    venueCmd.ExecuteNonQuery();

                    meetupEventVenueId = meetupEvent.venue.id;
                }

                // as a few ids in the dataset are containing characters instead of numbers, we are hashing the characters to get a valid primary key
                var meetupEventIdString = (string)meetupEvent.id;
                if (!meetupEventIdString.All(char.IsDigit))
                    meetupEventIdString = meetupEventIdString.GetHashCode().ToString();

                // escape backslash for description and name field
                var description = (string)meetupEvent.description;
                description = !string.IsNullOrWhiteSpace(description) ? description.Replace(@"\", @"\\") : string.Empty;
                var name = (string)meetupEvent.name;
                name = !string.IsNullOrWhiteSpace(name) ? name.Replace(@"\", @"\\") : string.Empty;


                // seed event table
                var meetupEventQuery = $@"INSERT INTO `event` VALUES ({meetupEventIdString}, '{meetupEvent.status}', {meetupEvent.utc_offset}, {meetupEvent.group.id}, '{description}', '{UnixTimeStampToValidMySQLTimeStamp(meetupEvent.created)}', '{meetupEvent.why}', {meetupEventVenueId}, '{UnixTimeStampToValidMySQLTimeStamp(meetupEvent.updated)}', '{meetupEvent.visibility}', '{meetupEvent.how_to_find_us}', {meetupEvent.yes_rsvp_count}, '{meetupEvent.event_url}', '{UnixTimeStampToValidMySQLTimeStamp(meetupEvent.time)}', {meetupEvent.waitlist_count}, {meetupEvent.headcount}, {meetupEvent.maybe_rsvp_count}, '{name}') ON DUPLICATE KEY UPDATE ID=ID;";
                var meetupEventCmd = new MySqlCommand(meetupEventQuery, connection);
                meetupEventCmd.ExecuteNonQuery();



            }
        }

    }
}
