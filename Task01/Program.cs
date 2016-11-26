using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Task01.MySQL;
using Task01.MySQL.Seeding;

namespace Task01
{
    class Program
    {
        static void Main(string[] args)
        {
            var connectionString = ConnectionService.CreateConnectionString("127.0.0.1", "root", "pass");
            using (ConnectionService connectionService = new ConnectionService(connectionString))
            {
                var dbCreationService = new CreationService(connectionService.Connection, "meetup");
                var dbSeedService = new SeedService(connectionService.Connection);

            }

            // ########## scenario 1: website ##########

            // list all groups:         SELECT g.id, g.name FROM `group` AS g
            // list a certain group:    SELECT * FROM `group` AS g WHERE g.id = 973292;

            // list all events:         SELECT g.id, g.name FROM `group` AS g LIMIT 1000;
            // list a certain event:    SELECT * FROM event AS e WHERE e.id = 8627496;


            // ########## scenario 2: visualization  ##########

            // rsvps of a single group  (0.047s)
            /*
                SELECT g.name, YEAR(e.created), MONTH(e.created), SUM(e.yes_rsvp_count)
                FROM event AS e 
                JOIN `group` AS g ON g.id = e.group_id 
                WHERE e.group_id = 6510
                GROUP BY YEAR(e.created), MONTH(e.created);
             */

            // rsvps of all groups  (347s)
            /*
                SELECT g.name, YEAR(e.created), MONTH(e.created), SUM(e.yes_rsvp_count)
                FROM event AS e 
                JOIN `group` AS g ON g.id = e.group_id 
                GROUP BY g.id, YEAR(e.created), MONTH(e.created);
             */


            // rsvps of a single category (20s)  // moving around the join order does not improve query time ... obviously the database is optimizing the query for us
            /*
                SELECT c.shortname, YEAR(e.created), MONTH(e.created), SUM(e.yes_rsvp_count) 
                FROM event AS e  
                JOIN `group` AS g ON g.id = e.group_id  
                JOIN `category` AS c ON c.id = g.category_id 
                WHERE c.id = 9
                GROUP BY c.id, YEAR(e.created), MONTH(e.created);
             */

            // rsvps of a all categories (260s)
            /*
                SELECT c.shortname, YEAR(e.created), MONTH(e.created), SUM(e.yes_rsvp_count) 
                FROM event AS e  
                JOIN `group` AS g ON g.id = e.group_id  
                JOIN `category` AS c ON c.id = g.category_id 
                GROUP BY c.Id, YEAR(e.created), MONTH(e.created)

             */

            Console.ReadLine();  // prevents application from closing

        }
    }

}
