using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Couchbase;
using Couchbase.Configuration;
using Enyim.Caching.Memcached;

namespace cb_client2
{
    class Program
    {
        private static List<Thread> threads = new List<Thread>(); 
        static void Main(string[] args)
        {
            var numberOfItems = 100000;
            var threadCount = 10;
            for (int i = 0; i < threadCount; i++)
            {
                var worker = new Worker {NumberOfItems = numberOfItems/threadCount};
                var thread = new Thread(worker.Process); 
                thread.Start();
                Thread.Sleep(10);//slight delay
            }
            Console.Read();
        }


        internal sealed class Worker
        {
            public Worker()
            {
                var config = new CouchbaseClientConfiguration { Bucket = "default" };
                config.Urls.Add(new Uri("http://localhost:8091/pools"));
                _client = new CouchbaseClient(config);
            }

            private readonly CouchbaseClient _client;
            public int NumberOfItems;

            public void Process()
            {
                for (int i = 0; i < NumberOfItems; i++)
                {
                    PerformGetSet(_client, i);
                }
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
}
