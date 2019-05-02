using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Management.CosmosDB.Fluent;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace GetMinRU
{
    class Program
    {
        const string ClientId = "Your Client Id for service credential";
        const string ClientSecret = "ClientSecret for service credential"; //"Secret";
        const string TenantId = "YourAADTenantId";
        const string SubscriptionId = "YourSubscriptionId";
        const string ResourceGroupFilter = null; // Specify this if you want the account lookup to be targeted to a certain RG;
        const string AccountName = "YourCosmosDBAccountName"; // null;
        const string DatabaseName = "YourDatabaseName";
        const string CollectionName = "YourCollectionName";
        const string MinRUHeaderName = "x-ms-cosmos-min-throughput";

        static void Main(string[] args)
        {
            try
            {
                MainAsync(args).Wait();
            }
            catch (AggregateException error)
            {
                foreach (Exception inner in error.InnerExceptions)
                {
                    Console.WriteLine("EXCEPTION: {0}", inner);
                }
            }
            catch (Exception error)
            {
                Console.WriteLine("EXCEPTION: {0}", error);
            }

            Console.WriteLine("Press any key to quit...");
            Console.ReadKey();
        }
        static async Task MainAsync(string[] args)
        {
            IServiceCollection serviceCollection = new ServiceCollection();
            serviceCollection.AddLogging(builder => builder
                .AddConsole()
                .AddFilter(level => level >= Microsoft.Extensions.Logging.LogLevel.Debug)
            );
            var loggerFactory = serviceCollection.BuildServiceProvider().GetService<ILoggerFactory>();
            ILogger log = loggerFactory.CreateLogger("TestDummy");

            var creds = new AzureCredentialsFactory().FromServicePrincipal(
                ClientId,
                ClientSecret,
                TenantId,
                AzureEnvironment.AzureGlobalCloud);

            IAzure azure = Azure.Authenticate(creds).WithSubscription(SubscriptionId);

            AccountInfo account = await GetAccountInfoAsync(log, azure, AccountName);
            ConnectionPolicy connectionPolicy = new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp
            };

            using (DocumentClient client = new DocumentClient(
                new Uri(account.Endoint),
                account.MasterKey,
                connectionPolicy,
                ConsistencyLevel.Eventual))
            {
                await client.OpenAsync();
                string offerLink = await GetOfferLinkAsync(client, account, DatabaseName, CollectionName);

                Console.WriteLine("Min Throughput: {0}", await GetMinThroughputAsync(client, offerLink));
            }
        }

        static async Task<AccountInfo> GetAccountInfoAsync(
            ILogger log,
            IAzure azure,
            string accountName)
        {
            if (String.IsNullOrWhiteSpace(accountName))
            {
                throw new ArgumentNullException(nameof(accountName));
            }

            IPagedCollection<ICosmosDBAccount> cosmosDBAccounts;

            if (String.IsNullOrWhiteSpace(ResourceGroupFilter))
            {
                cosmosDBAccounts = await azure.CosmosDBAccounts.ListAsync(loadAllPages: true);
            }
            else
            {
                cosmosDBAccounts = await azure.CosmosDBAccounts.ListByResourceGroupAsync(ResourceGroupFilter, loadAllPages: true);
            }

            ICosmosDBAccount account = cosmosDBAccounts.SingleOrDefault(
                a => accountName.Equals(a.Name, StringComparison.OrdinalIgnoreCase));

            if (account == null)
            {
                string message = $"Invalid account name '{accountName}'. Account couldn't be found.";
                log.LogError(message);
                throw new ArgumentException(message);
            }

            string key = null;
            try
            {
                key = (await account.ListKeysAsync()).PrimaryMasterKey;
            }
            catch (Exception error)
            {
                string message =
                    $"Invalid account name '{accountName}'. No permission to acces master key of account '{account.Id}'";
                log.LogError(message);
                log.LogWarning(error.ToString());

                throw new ArgumentException(message, nameof(accountName));
            }

            log.LogInformation($"Account '{accountName}' found and initialized.");

            return new AccountInfo
            {
                Endoint = account.DocumentEndpoint,
                MasterKey = key,
                Name = account.Name,
            };
        }

        static async Task<string> GetOfferLinkAsync(
            DocumentClient client,
            AccountInfo accountInfo,
            string dbName,
            string collectionName)
        {
            OfferV2 dbOffer = null;

            ResourceResponse<Database> databaseResponse = await client.ReadDatabaseAsync($"/dbs/{dbName}");
            ResourceResponse<DocumentCollection> collectionResponse = await client.ReadDocumentCollectionAsync(
                $"/dbs/{dbName}/colls/{collectionName}");

            string continuation = string.Empty;
            do
            {
                // Read the feed 10 items at a time until there are no more items to read
                FeedResponse<Offer> offersResponse = await client.ReadOffersFeedAsync(
                    new FeedOptions
                    {
                        MaxItemCount = 100,
                        RequestContinuation = continuation
                    });

                foreach (OfferV2 offer in offersResponse)
                {
                    if (offer.ResourceLink == databaseResponse.Resource.SelfLink)
                    {
                        dbOffer = offer;
                        continue;
                    }

                    if (offer.ResourceLink == collectionResponse.Resource.SelfLink)
                    {
                        return offer.SelfLink;
                    }
                }

                // Get the continuation so that we know when to stop.
                continuation = offersResponse.ResponseContinuation;
            } while (!string.IsNullOrEmpty(continuation));

            if (dbOffer != null)
            {
                return dbOffer.SelfLink;
            }

            throw new InvalidOperationException("Offer for either database or collection should always exist");
        }

        static async Task<int> GetMinThroughputAsync(DocumentClient client, string offerLink)
        {
            ResourceResponse<Offer> offerResponse = await client.ReadOfferAsync(offerLink);
            return Int32.Parse(offerResponse.ResponseHeaders[MinRUHeaderName]);
        }
        
        class AccountInfo
        {
            public string Endoint { get; set; }

            public string MasterKey { get; set; }

            public string Name { get; set; }
        }
    }
}
