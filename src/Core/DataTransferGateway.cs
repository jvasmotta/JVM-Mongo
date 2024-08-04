using MongoDB.Driver;

namespace JVM_Mongo;

public interface IDataTransferGateway
{
    public IEnumerable<T> LazilyEnumerate<T>(Context collectionContext, FilterDefinition<T>? filterDefinition = null, int? limit = null);
    public void Save<T>(T obj, Context collectionContext);
    public void Delete<T>(string id, Context collectionContext);

    public enum Context
    {
        WebHook
    }
}

public class DataTransferGateway(IMongoDatabase mongoDatabase) : IDataTransferGateway
{
    public IEnumerable<T> LazilyEnumerate<T>(IDataTransferGateway.Context collectionContext, FilterDefinition<T>? filterDefinition = null, int? limit = null)
    {
        var cursor = mongoDatabase.GetCollection<T>(collectionContext.ToString()).Find(filterDefinition ?? Builders<T>.Filter.Empty).Limit(limit).ToCursor();
        while (cursor.MoveNext())
            foreach (var obj in cursor.Current)
                yield return obj;
    }

    public void Save<T>(T obj, IDataTransferGateway.Context collectionContext) => mongoDatabase.GetCollection<T>(collectionContext.ToString()).InsertOne(obj);

    public void Delete<T>(string id, IDataTransferGateway.Context collectionContext)
    {
        var filter = Builders<T>.Filter.Eq("_id", id);
        mongoDatabase.GetCollection<T>(collectionContext.ToString()).DeleteOne(filter);
    }
}
