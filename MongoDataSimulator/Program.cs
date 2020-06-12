using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace MongoDataSimulator
{
    class Program
    {
        private static MongoClient destMongoClient;

        private static string destDbName = ConfigurationManager.AppSettings["dbName"];

        private static string destCollectionName = ConfigurationManager.AppSettings["collectionName"];

        private static int batchSize = Int32.Parse(ConfigurationManager.AppSettings["batchsize"]);
        private static int count= Int32.Parse(ConfigurationManager.AppSettings["count"]);
        private static int sleepInterval = Int32.Parse(ConfigurationManager.AppSettings["sleep-interval-in-milliseconds"]);
        private static List<BsonDocument> insertFailedDocs = new List<BsonDocument>();
        private static long docsCount;
        private static IMongoDatabase destDatabase;

        private static IMongoCollection<BsonDocument> destDocStoreCollection;

        private static List<BsonDocument> inMemoryBsonDocs = null;

        static void Main(string[] args)
        {
            var val = extractRetryDuration("Error=16500, RetryAfterMs=64");
            string destConnectionString =

                ConfigurationManager.AppSettings["conn"];

            MongoClientSettings destSettings = MongoClientSettings.FromUrl(

                new MongoUrl(destConnectionString)

            );

            destSettings.MinConnectionPoolSize = 800;
            destSettings.MaxConnectionPoolSize = 3500;

            //destSettings.UseSsl = false;
            //destSettings.VerifySslCertificate = false;


            destMongoClient = new MongoClient(destSettings);

            destDatabase = destMongoClient.GetDatabase(destDbName);

            destDocStoreCollection = destDatabase.GetCollection<BsonDocument>(destCollectionName);

            Console.WriteLine("Conduction 5 tests... with batch size: {0}",count);

            for (int i = 0; i < 5; i++)
            {

                Console.WriteLine("Loading inmemory docs...");

                GetInMemorySampleDocs(batchSize);

                Console.WriteLine("Inserting inmemory docs...");
                Stopwatch stopwatch = new Stopwatch();

                // Begin timing.
                stopwatch.Start();

                InsertAllDocuments(inMemoryBsonDocs, destDocStoreCollection).Wait();

                // Stop timing.
                stopwatch.Stop();

                // Write result.
                Console.WriteLine("Total time it took for inserting docs: {0} - number Of docs inserted: {1}", (inMemoryBsonDocs.Count() - insertFailedDocs.Count()), stopwatch.Elapsed.TotalSeconds);
            }

            Console.WriteLine("Press enter to exit....");
            Console.ReadLine();
            //InsertSampleDocs(batchSize).Wait();

            string failedDocsFile = "failed.log";

            if (insertFailedDocs.Any())
            {
                using (var sw = new StreamWriter(failedDocsFile))
                {
                    foreach (var doc in insertFailedDocs)
                    {
                        sw.WriteLine(doc.ToJson());
                    }
                }

                Console.WriteLine("Not all documents were exported, failed ones count: {0} - failed documents located @: {1}",
                   insertFailedDocs.Count(),failedDocsFile
                   );
            }

        }

        private static async Task InsertAllDocuments(
            IEnumerable<BsonDocument> docs,
            IMongoCollection<BsonDocument> destDocStoreCollection)
        {
            var tasks = new List<Task>();
            for (int j = 0; j < docs.Count(); j++)
            {
                tasks.Add(InsertDocument(docs.ToList()[j], destDocStoreCollection));
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
            docsCount = docsCount + docs.Count();
            Console.WriteLine(
                "Total documents copied so far: {0}",
                docsCount);
        }
         private static async Task InsertDocument(
            BsonDocument doc,
            IMongoCollection<BsonDocument> destDocStoreCollection)
        {
            bool isSucceed = false;
            for (int i = 0; i < 10; i++)
            {
                try
                {
                    var sw = new Stopwatch();
                    sw.Start();
                    await destDocStoreCollection.InsertOneAsync(doc);
                    sw.Stop();
                    Console.WriteLine("Write speed in seconds: {0}", sw.Elapsed.TotalSeconds);
                    isSucceed = true;
                    //Operation succeed just break the loop
                    break;
                }
                catch (Exception ex)
                {

                    if (!IsThrottled(ex))
                    {
                        Console.WriteLine("ERROR: With collection {0}", ex.ToString());
                        throw;
                    }
                    else
                    {
                        Console.WriteLine("Throttled");
                        // Thread will wait in between 1.5 secs and 3 secs.
                        System.Threading.Thread.Sleep(extractRetryDuration(ex.Message));
                    }
                }
            }

            if (!isSucceed)
            {
                insertFailedDocs.Add(doc);
            }

        }

        private static bool IsThrottled(Exception ex)
        {
            return ex.Message.ToLower().Contains("Request rate is large".ToLower());
        }

        private static void ChangeFeedSample()
        {
            var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>()
                                .Match(
                                        change => change.OperationType == ChangeStreamOperationType.Insert ||                         change.OperationType == ChangeStreamOperationType.Update || 
                                        change.OperationType == ChangeStreamOperationType.Replace)
                                    .AppendStage<ChangeStreamDocument<BsonDocument>,
                                         ChangeStreamDocument<BsonDocument>,
                                        BsonDocument>(
                                        "{ $project: { '_id': 1, 'fullDocument': 1, 'ns': 1, 'documentKey': 1 }}");

            int count = 0;
            var options = new ChangeStreamOptions
            {
                FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                StartAtOperationTime = new BsonTimestamp(DateTime.MinValue.Ticks)
            };

            var enumerator = destDocStoreCollection.Watch(pipeline, options).ToEnumerable().GetEnumerator();

            while (enumerator.MoveNext())
            {
                Console.WriteLine(enumerator.Current);
                count++;
                Console.WriteLine("Total documents" + count);

            }
            enumerator.Dispose();
        }



        private static IEnumerable<BsonDocument> GetSampleDocuments(int batchSize)
        {
            ConcurrentBag<BsonDocument> bsonDocuments = new ConcurrentBag<BsonDocument>();
            string sampleBson = File.ReadAllText("Sample.json");
            Parallel.For(0, batchSize, (i) =>
            {
                BsonDocument doc = BsonDocument.Parse(sampleBson);
                doc.Remove("_id");
                doc["draftNumber"] = System.Guid.NewGuid().ToString();
                bsonDocuments.Add(
                    doc
                    );
            });
            return bsonDocuments.ToList<BsonDocument>();
        }

        private static IEnumerable<BsonDocument> GetSampleDocuments2(int batchSize)
        {
            ConcurrentBag<BsonDocument> bsonDocuments = new ConcurrentBag<BsonDocument>();
            string sampleBson = "{{'id':'{0}', 'doc-id':{1}}}";
            Parallel.For(0, batchSize, (i) =>
            {
                bsonDocuments.Add(
                    BsonDocument.Parse(string.Format(sampleBson, System.Guid.NewGuid().ToString(), i))
                    );
            });
            return bsonDocuments.ToList<BsonDocument>();
        }

        private static async Task InsertDocument(BsonDocument doc)

        {
            await destDocStoreCollection.InsertOneAsync(doc);
        }

        private static async Task InsertAllDocuments(IEnumerable<BsonDocument> docs)

        {
            var tasks = new List<Task>();
            for (int j = 0; j < docs.Count(); j++)
            {
                tasks.Add(InsertDocument(docs.ToList()[j]));
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
            docsCount = docsCount + docs.Count();
            Console.WriteLine("Total documents copied so far: {0}", docsCount);

        }
        private static int extractRetryDuration(String message)
        {
            Regex rx = new Regex("RetryAfterMs=([0-9]+)",
                                 RegexOptions.Compiled);

            MatchCollection matches = rx.Matches(message);


            int retryAfter = new Random().Next(1000, 5000);

            if (matches.Count > 0)
            {

                retryAfter = Int32.Parse(matches[0].Groups[1].Value);

            }

            return retryAfter;
        }

        private static void GetInMemorySampleDocs(int batchSize)

        {
            int total = 0;
            inMemoryBsonDocs = new List<BsonDocument>();
            while (true)
            {

                inMemoryBsonDocs.AddRange(GetSampleDocuments(batchSize));
                total = total + batchSize;
                if (total >= count)
                {
                    break;
                }
                System.Threading.Thread.Sleep(sleepInterval);

            }
        }

        private static async Task InsertSampleDocs(int batchSize)

        {
            int total = 0;
            while (true)
            {

                IEnumerable<BsonDocument> batch = GetSampleDocuments(batchSize);
                await InsertAllDocuments(batch);
                total = total + batchSize;
                if(total>=count)
                {
                    break;
                }
                System.Threading.Thread.Sleep(sleepInterval);
                
            }
        }

    }
}

