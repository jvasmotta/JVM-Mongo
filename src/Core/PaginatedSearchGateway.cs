using System.Text.Json;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace JVM_Mongo;

public record PaginatedHeader<TMetadata>(
    [property: BsonId] string CollectionName,
    TMetadata Metadata,
    byte[] SearchParameters,
    long? TotalElements,
    DateTime CreatedAt,
    DateTime ExpiresAt) where TMetadata : class, IEquatable<TMetadata>;

public record PaginatedResult<TOut>(
    int Page,
    int Size,
    bool NextPage,
    long? TotalElements,
    IEnumerable<TOut> Results);


public interface IPaginatedSearchGateway
{
    Task<PaginatedResult<TOut>> GetOrCreate<TOut, TMetadata, TRequest>(
        TMetadata metadata, 
        TRequest searchRequest,
        Func<IAsyncEnumerable<TOut>> fetchFunc, 
        int page = 0, 
        int size = 100,
        TimeSpan? expiringTimeSpan = null) where TMetadata : class, IEquatable<TMetadata>;

    Task UpdateHeaderTotalElements<TMetadata, TRequest>(
        TMetadata metadata, 
        TRequest searchRequest, 
        long totalElements) where TMetadata : class, IEquatable<TMetadata>;

    void CleanExpiredResults<TMetadata>(DateTime timestamp) where TMetadata : class, IEquatable<TMetadata>;
}

public class PaginatedSearchGateway(IMongoDatabase mongoDatabase, Func<ObjectId>? idGeneratorFunc = null) : IPaginatedSearchGateway
{
    public const string HeaderCollectionName = "PaginatedSearchHeader";
    private readonly Func<ObjectId> _idGeneratorFunc = idGeneratorFunc ?? ObjectId.GenerateNewId;

    public async Task<PaginatedResult<TOut>> GetOrCreate<TOut, TMetadata, TRequest>(
        TMetadata metadata, 
        TRequest searchRequest,
        Func<IAsyncEnumerable<TOut>> fetchFunc, 
        int page = 0, 
        int size = 100,
        TimeSpan? expiringTimeSpan = null) where TMetadata : class, IEquatable<TMetadata>
    {
        var searchHeaderCollection = mongoDatabase.GetCollection<PaginatedHeader<TMetadata>>(HeaderCollectionName);
        var binarySearchRequest = JsonSerializer.SerializeToUtf8Bytes(searchRequest);

        var header = await searchHeaderCollection.FindOneAndUpdateAsync(
            Builders<PaginatedHeader<TMetadata>>.Filter.Eq(psc => psc.Metadata, metadata) &
            Builders<PaginatedHeader<TMetadata>>.Filter.Eq(psc => psc.SearchParameters, binarySearchRequest),
            Builders<PaginatedHeader<TMetadata>>.Update.SetOnInsert(psc => psc.CollectionName, $"PaginatedSearchResult_{_idGeneratorFunc()}")
                .SetOnInsert(psc => psc.TotalElements, null)
                .SetOnInsert(psc => psc.CreatedAt, DateTime.UtcNow)
                .SetOnInsert(psc => psc.ExpiresAt, DateTime.UtcNow.Add(expiringTimeSpan ?? TimeSpan.FromMinutes(30))),
            new FindOneAndUpdateOptions<PaginatedHeader<TMetadata>> { IsUpsert = true, ReturnDocument = ReturnDocument.After }
        );

        var searchResultCollection = mongoDatabase.GetCollection<TOut>(header.CollectionName);
        var totalDocuments = await searchResultCollection.CountDocumentsAsync(_ => true);
        var results = await searchResultCollection.Find(_ => true).Skip(page * size).Limit(size).ToListAsync();

        if (results.Any())
            return new PaginatedResult<TOut>(page, size, totalDocuments > (page + 1) * size, header.TotalElements, results);

        return await FetchAndInsertAsync(header, searchResultCollection, fetchFunc, page, size);
    }

    public async Task UpdateHeaderTotalElements<TMetadata, TRequest>(
        TMetadata metadata, 
        TRequest searchRequest, 
        long totalElements) where TMetadata : class, IEquatable<TMetadata>
    {
        var searchHeaderCollection = mongoDatabase.GetCollection<PaginatedHeader<TMetadata>>(HeaderCollectionName);
        var binarySearchRequest = JsonSerializer.SerializeToUtf8Bytes(searchRequest);

        await searchHeaderCollection.FindOneAndUpdateAsync<TMetadata>(
            Builders<PaginatedHeader<TMetadata>>.Filter.Eq(psc => psc.Metadata, metadata) &
            Builders<PaginatedHeader<TMetadata>>.Filter.Eq(psc => psc.SearchParameters, binarySearchRequest),
            Builders<PaginatedHeader<TMetadata>>.Update.Set(psc => psc.TotalElements, totalElements));
    }

    public void CleanExpiredResults<TMetadata>(DateTime timestamp) where TMetadata : class, IEquatable<TMetadata>
    {
        var searchHeaderCollection = mongoDatabase.GetCollection<PaginatedHeader<TMetadata>>(HeaderCollectionName);
        var headerCursor = searchHeaderCollection.Find(h => h.ExpiresAt < timestamp).ToCursor();

        while (headerCursor.MoveNext())
            foreach (var header in headerCursor.Current)
            {
                mongoDatabase.DropCollection(header.CollectionName);
                searchHeaderCollection.DeleteOne(h => h.CollectionName == header.CollectionName);
            }
    }

    private async Task<PaginatedResult<TOut>> FetchAndInsertAsync<TMetadata, TOut>(
        PaginatedHeader<TMetadata> paginatedHeader,
        IMongoCollection<TOut> searchResultCollection,
        Func<IAsyncEnumerable<TOut>> fetchFunc,
        int page,
        int size) where TMetadata : class, IEquatable<TMetadata>
    {
        try
        {
            var documentsToInsert = new List<TOut>();
            await foreach (var item in fetchFunc())
            {
                documentsToInsert.Add(item);
                if (documentsToInsert.Count < size * (page + 1))
                    continue;
                
                await searchResultCollection.InsertManyAsync(documentsToInsert);
                documentsToInsert.Clear();

                _ = Task.Run(async () =>
                {
                    await Task.Yield();
                    await InsertRemainingDocumentsAsync(paginatedHeader, searchResultCollection, fetchFunc);
                });
                break;
            }
            
            if(documentsToInsert.Any())
                await searchResultCollection.InsertManyAsync(documentsToInsert);

            var totalDocuments = await searchResultCollection.CountDocumentsAsync(_ => true);
            var results = await searchResultCollection.Find(_ => true).Skip(page * size).Limit(size).ToListAsync();
            return new PaginatedResult<TOut>(page, size, totalDocuments / size > page, paginatedHeader.TotalElements, results);
        }
        catch (Exception)
        {
            return new PaginatedResult<TOut>(page, size, false, 0, Array.Empty<TOut>());
        }
    }
    private async Task InsertRemainingDocumentsAsync<TMetadata, TOut>(
        PaginatedHeader<TMetadata> paginatedHeader, 
        IMongoCollection<TOut> searchResultCollection,
        Func<IAsyncEnumerable<TOut>> fetchFunc) where TMetadata : class, IEquatable<TMetadata>
    {
        var documentsToInsert = new List<TOut>();
        await foreach (var item in fetchFunc()) 
            documentsToInsert.Add(item);

        if (documentsToInsert.Any()) 
            await searchResultCollection.InsertManyAsync(documentsToInsert);

        var totalDocuments = await searchResultCollection.CountDocumentsAsync(_ => true);
        await UpdateHeaderTotalElements(paginatedHeader, totalDocuments);
    }

    private async Task UpdateHeaderTotalElements<TMetadata>(
        PaginatedHeader<TMetadata> paginatedHeader, 
        long totalDocuments) where TMetadata : class, IEquatable<TMetadata>
    {
        var searchHeaderCollection = mongoDatabase.GetCollection<PaginatedHeader<TMetadata>>(HeaderCollectionName);
        var filter = Builders<PaginatedHeader<TMetadata>>.Filter.Eq(psc => psc.CollectionName, paginatedHeader.CollectionName);
        var update = Builders<PaginatedHeader<TMetadata>>.Update.Set(psc => psc.TotalElements, totalDocuments);
        var options = new FindOneAndUpdateOptions<PaginatedHeader<TMetadata>> { ReturnDocument = ReturnDocument.After };

        await searchHeaderCollection.FindOneAndUpdateAsync(filter, update, options);
    }
}