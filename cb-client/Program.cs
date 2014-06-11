using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Couchbase;
using Couchbase.Configuration;
using Enyim;
using Enyim.Caching.Memcached;

namespace cb_client
{
    class Program
    {
        public static List<CouchbaseClient> Clients = new List<CouchbaseClient>(); 
        static void Main(string[] args)
        {
            var config1 = new CouchbaseClientConfiguration { Bucket = "default" };
            config1.Urls.Add(new Uri("http://localhost:8091/pools"));
            
            var client1 = new CouchbaseClient(config1);
            Clients.Add(client1);

            Thread.Sleep(100);
            var config2 = new CouchbaseClientConfiguration { Bucket = "default" };
            config2.Urls.Add(new Uri("http://localhost:8091/pools"));

            var client2 = new CouchbaseClient(config2);
            Clients.Add(client2);

            const int itemsToProcess = 100000;
            foreach (var client in Clients)
            {
               //ProcessWithTap(client, itemsToProcess/Clients.Count);
                ProcessWithParallel(client, itemsToProcess / Clients.Count);
            }

            Console.Read();
        }

        static void ProcessSync(CouchbaseClient client, int numberOfItems)
        {
            for (var i = 0; i < numberOfItems; i++)
            {
                PerformGetSet(client, i);
            }
        }

        static void ProcessWithTap(CouchbaseClient client, int numberOfItems)
        {
            for (var i = 0; i < numberOfItems; i++)
            {
                var i1 = i;
                Task.Run(() => PerformGetSet(client, i1));
            }
        }

        static void ProcessWithParallel(CouchbaseClient client, int numberOfItems)
        {
            var options = new ParallelOptions { MaxDegreeOfParallelism = 4 };

            Parallel.For(0, numberOfItems, options, i => PerformGetSet(client, i));
        }

        private static void PerformGetSet(CouchbaseClient client, int i)
        {
            var key = string.Format("key{0}", i);
            var document = string.Concat("{\"value\":\"", i, "\"}");
                        
            var set = client.ExecuteStore(StoreMode.Set, key, document);
            Console.WriteLine("Set: {0} - {1} - {2}", key, set.Success, Thread.CurrentThread.ManagedThreadId);

            var get = client.ExecuteGet<dynamic>(key);
            Console.WriteLine("Get: {0} - {1} - {2}", key, get.Success, Thread.CurrentThread.ManagedThreadId);
        }
    }
}
