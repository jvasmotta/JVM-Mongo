using System.Text.Json;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace JVM_Mongo;

public interface IPaginatedSearchGateway
{
    Task<Result<TOut>> GetOrCreateAsync<TOut, TRequest>(string clientId, int page, int size, TRequest searchRequest, IEnumerator<TOut> fetchFunc);
    void CleanExpiredResults(DateTime timestamp);

    public record Header([property: BsonId] string CollectionName, string ClientId, byte[] SearchParameters, long? TotalElements, DateTime CreatedAt, DateTime ExpiresAt);
    public record Result<TOut>(int Page, int Size, bool NextPage, long? TotalElements, IEnumerable<TOut> Results);

    void UpdateHeaderTotalElements<TRequest>(string clientId, TRequest searchRequest, long totalElements);
}

public class PaginatedSearchGateway(IMongoDatabase mongoDatabase, Func<ObjectId>? generateObjectId = null) : IPaginatedSearchGateway
{
    public const string HeaderCollectionName = "PaginatedSearchHeader";

    private readonly Func<ObjectId> _generateObjectId = generateObjectId ?? ObjectId.GenerateNewId;

    public async Task<IPaginatedSearchGateway.Result<TOut>> GetOrCreateAsync<TOut, TRequest>(string clientId, int page, int size, TRequest searchRequest, IEnumerator<TOut> fetchFunc)
    {
        var searchHeaderCollection = mongoDatabase.GetCollection<IPaginatedSearchGateway.Header>(HeaderCollectionName);
        var binarySearchRequest = JsonSerializer.SerializeToUtf8Bytes(searchRequest);

        var header = await searchHeaderCollection.FindOneAndUpdateAsync(
            Builders<IPaginatedSearchGateway.Header>.Filter.Eq(psc => psc.ClientId, clientId) &
            Builders<IPaginatedSearchGateway.Header>.Filter.Eq(psc => psc.SearchParameters, binarySearchRequest),
            Builders<IPaginatedSearchGateway.Header>.Update.SetOnInsert(psc => psc.CollectionName, $"PaginatedSearchResult_{_generateObjectId()}")
                .SetOnInsert(psc => psc.TotalElements, null)
                .SetOnInsert(psc => psc.CreatedAt, DateTime.UtcNow)
                .SetOnInsert(psc => psc.ExpiresAt, DateTime.UtcNow.AddMinutes(30)),
            new FindOneAndUpdateOptions<IPaginatedSearchGateway.Header> { IsUpsert = true, ReturnDocument = ReturnDocument.After }
        );

        var searchResultCollection = mongoDatabase.GetCollection<TOut>(header.CollectionName);
        var totalDocuments = await searchResultCollection.CountDocumentsAsync(_ => true);
        var results = await searchResultCollection.Find(_ => true).Skip(page * size).Limit(size).ToListAsync();
        if (results.Any())
            return new IPaginatedSearchGateway.Result<TOut>(page, size, totalDocuments / size > page, header.TotalElements, results);

        return await FetchAndInsertAsync(header, searchResultCollection, fetchFunc, page, size);
    }

    public void UpdateHeaderTotalElements<TRequest>(string clientId, TRequest searchRequest, long totalElements)
    {
        var searchHeaderCollection = mongoDatabase.GetCollection<IPaginatedSearchGateway.Header>(HeaderCollectionName);
        var binarySearchRequest = JsonSerializer.SerializeToUtf8Bytes(searchRequest);

        searchHeaderCollection.FindOneAndUpdateAsync(
            Builders<IPaginatedSearchGateway.Header>.Filter.Eq(psc => psc.ClientId, clientId) &
            Builders<IPaginatedSearchGateway.Header>.Filter.Eq(psc => psc.SearchParameters, binarySearchRequest),
            Builders<IPaginatedSearchGateway.Header>.Update.Set(psc => psc.TotalElements, totalElements));
    }

    public void CleanExpiredResults(DateTime timestamp)
    {
        var searchHeaderCollection = mongoDatabase.GetCollection<IPaginatedSearchGateway.Header>(HeaderCollectionName);
        var headerCursor = searchHeaderCollection.Find(h => h.ExpiresAt < timestamp).ToCursor();

        while (headerCursor.MoveNext())
            foreach (var header in headerCursor.Current)
            {
                mongoDatabase.DropCollection(header.CollectionName);
                searchHeaderCollection.DeleteOne(h => h.CollectionName == header.CollectionName);
            }
    }

    private async Task<IPaginatedSearchGateway.Result<TOut>> FetchAndInsertAsync<TOut>(
        IPaginatedSearchGateway.Header header,
        IMongoCollection<TOut> searchResultCollection,
        IEnumerator<TOut> fetchFunc,
        int page,
        int size)
    {
        try
        {
            var documentsToInsert = new List<TOut>();
            while (fetchFunc.MoveNext())
            {
                documentsToInsert.Add(fetchFunc.Current);
                if (documentsToInsert.Count < size * (page + 1))
                    continue;
                
                await searchResultCollection.InsertManyAsync(documentsToInsert);
                documentsToInsert.Clear();

                _ = Task.Run(async () =>
                {
                    await Task.Yield();
                    await InsertRemainingDocumentsAsync(header, searchResultCollection, fetchFunc);
                });
                break;
            }
            
            if(documentsToInsert.Any())
                await searchResultCollection.InsertManyAsync(documentsToInsert);

            var totalDocuments = await searchResultCollection.CountDocumentsAsync(_ => true);
            var results = searchResultCollection.Find(_ => true).Skip(page * size).Limit(size).ToList();
            return new IPaginatedSearchGateway.Result<TOut>(page, size, totalDocuments / size > page, header.TotalElements, results);
        }
        catch (Exception)
        {
            return new IPaginatedSearchGateway.Result<TOut>(page, size, false, 0, Array.Empty<TOut>());
        }
    }
    private async Task InsertRemainingDocumentsAsync<TOut>(
        IPaginatedSearchGateway.Header header, 
        IMongoCollection<TOut> searchResultCollection, 
        IEnumerator<TOut> fetchFunc)
    {
        var documentsToInsert = new List<TOut>();
        while (fetchFunc.MoveNext()) 
            documentsToInsert.Add(fetchFunc.Current);
        
        if (documentsToInsert.Any()) 
            await searchResultCollection.InsertManyAsync(documentsToInsert);

        var totalDocuments = await searchResultCollection.CountDocumentsAsync(_ => true);
        await UpdateHeaderTotalElements(header, totalDocuments);
    }

    private async Task UpdateHeaderTotalElements(IPaginatedSearchGateway.Header header, long totalDocuments)
    {
        var searchHeaderCollection = mongoDatabase.GetCollection<IPaginatedSearchGateway.Header>(HeaderCollectionName);
        var filter = Builders<IPaginatedSearchGateway.Header>.Filter.Eq(psc => psc.CollectionName, header.CollectionName);
        var update = Builders<IPaginatedSearchGateway.Header>.Update.Set(psc => psc.TotalElements, totalDocuments);
        var options = new FindOneAndUpdateOptions<IPaginatedSearchGateway.Header> { ReturnDocument = ReturnDocument.After };

        await searchHeaderCollection.FindOneAndUpdateAsync(filter, update, options);
    }
}