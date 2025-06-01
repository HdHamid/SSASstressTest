using Microsoft.AnalysisServices.AdomdClient;

//reaserch 
//k6 ssas tabular 
namespace SSASStressTest
{
    class Program
    {
        static async Task Main(string[] args)
        //static async Task Main(string[] args)
        {
            //Console.WriteLine(queries[0].ToString());

            //RunStressTest(Con, queries, 1);

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"Insert Thread Count:");
            Console.ForegroundColor = ConsoleColor.White;
            int threadCount = Convert.ToInt32(Console.ReadLine());

            //string Con = "Data Source=zgdc-olap;Initial Catalog=TestDirectQuery_Hamid_Doostparvar_954b5bc3-e28d-4aef-9186-eb5600770b97;Integrated Security=SSPI";

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"Insert SSAS Connection String:");
            Console.ForegroundColor = ConsoleColor.White;
            string Con  = Console.ReadLine();


            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Insert your DAX queries (seperate each query with /Sep and type /END in a new line to finish):");
            Console.ForegroundColor = ConsoleColor.White;

            List<string> lines = new List<string>();
            string line;
            while ((line = Console.ReadLine()) != null)
            {
                if (line.Trim().ToUpper() == "/END")
                    break;

                lines.Add(line);
            }

            string daxQueries = string.Join(Environment.NewLine, lines);



            List<string> queries = daxQueries.Split("/Sep").ToList();
                
            //new List<string>
            //{
            //    $"EVALUATE\r\n\tVAR __DS0FilterTable = \r\n\t\tTREATAS({{\"ZP00\",\r\n\t\t\t\"ZP51\",\r\n\t\t\t\"ZP53\",\r\n\t\t\t\"ZP54\",\r\n\t\t\t\"ZP56\"}}, 'Query'[PlantCode])\r\n\r\n\tVAR __DS0FilterTable2 = \r\n\t\tFILTER(\r\n\t\t\tKEEPFILTERS(VALUES('Query'[InventoryDate])),\r\n\t\t\tAND(\r\n\t\t\t\t'Query'[InventoryDate] >= DATE(2022, 6, 16),\r\n\t\t\t\t'Query'[InventoryDate] < DATE(2024, 7, 18)\r\n\t\t\t)\r\n\t\t)\r\n\r\n\tVAR __DS0Core = \r\n\t\tSUMMARIZECOLUMNS(\r\n\t\t\t'Query'[MaterialCode],\r\n\t\t\t__DS0FilterTable,\r\n\t\t\t__DS0FilterTable2,\r\n\t\t\t\"TT\", 'Query'[TT]\r\n\t\t)\r\nreturn __DS0Core\r\n"
            //    ,$"DEFINE\r\n\tVAR __DS0FilterTable = \r\n\t\tTREATAS({{\"ZP56\",\r\n\t\t\t\"ZP59\",\r\n\t\t\t\"ZP60\",\r\n\t\t\t\"ZP61\"}}, 'Query'[PlantCode])\r\n\r\n\tVAR __DS0FilterTable2 = \r\n\t\tFILTER(\r\n\t\t\tKEEPFILTERS(VALUES('Query'[InventoryDate])),\r\n\t\t\tAND(\r\n\t\t\t\t'Query'[InventoryDate] >= DATE(2022, 2, 7),\r\n\t\t\t\t'Query'[InventoryDate] < DATE(2023, 8, 25)\r\n\t\t\t)\r\n\t\t)\r\n\r\n\tVAR __DS0Core = \r\n\t\tSUMMARIZECOLUMNS(\r\n\t\t\tROLLUPADDISSUBTOTAL('Query'[MaterialCode], \"IsGrandTotalRowTotal\"),\r\n\t\t\t__DS0FilterTable,\r\n\t\t\t__DS0FilterTable2,\r\n\t\t\t\"TT\", 'Query'[TT]\r\n\t\t)\r\n\r\n\tVAR __DS0PrimaryWindowed = \r\n\t\tTOPN(502, __DS0Core, [IsGrandTotalRowTotal], 0, 'Query'[MaterialCode], 1)\r\n\r\nEVALUATE\r\n\t__DS0PrimaryWindowed\r\n\r\nORDER BY\r\n\t[IsGrandTotalRowTotal] DESC, 'Query'[MaterialCode]\r\n"
            //};

            Console.WriteLine("Start Test");
            var tasks = new List<Task>();


            for (int i = 0; i < threadCount; i++)
            {
                int ThreadIndex = i;
                tasks.Add(RunStressTest(Con, queries, ThreadIndex));
                //tasks.Add(Task.Run(() => RunStressTest(Con, queries, ThreadIndex)));
            }
            await Task.WhenAll(tasks.ToArray());
            //await Task.WaitAll(tasks.ToArray());
            // Task.WaitAll: blocks the current thread until everything has completed.
            // Task.WhenAll: Returns a task which represents the action of waiting until everything has completed.
            Console.WriteLine("End Test");
        }

        private static Task RunStressTest(string connectionString, List<string> queries, int threadIndex)
        {
            return Task.Run(() =>
            {
                var server = new AdomdConnection(connectionString);
                try
                {
                    var random = new Random();
                    var Query = queries[random.Next(queries.Count)];
                    var Cmnd = new AdomdCommand(Query, server);
                    DateTime dt1 = DateTime.Now;
                    server.Open();
                    Console.WriteLine($"Thread: {threadIndex} Connected to {server.Database}");
                    var res = Cmnd.ExecuteNonQuery();
                    DateTime dt2 = DateTime.Now;

                    Console.WriteLine($"Thread: {threadIndex} evaluated in {(int)(dt2 - dt1).TotalMilliseconds} Milliseconds");
                    string ClearCache = "<ClearCache xmlns=\"http://schemas.microsoft.com/analysisservices/2003/engine\">  \r\n  <Object>  \r\n    <DatabaseID>TestDirectQuery_Hamid_Doostparvar_954b5bc3-e28d-4aef-9186-eb5600770b97</DatabaseID>  \r\n  </Object>  \r\n</ClearCache>  \r\n";
                    var Cmnd2 = new AdomdCommand(ClearCache, server);
                    Cmnd.ExecuteNonQuery();
                    server.Close();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Thread: {threadIndex} Hass error: {ex.Message}");
                }
            });


        }
    }
}