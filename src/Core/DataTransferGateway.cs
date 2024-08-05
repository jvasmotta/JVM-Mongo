using System.Data;
using System.Reflection;
using MongoDB.Driver;

namespace JVM_Mongo;


[AttributeUsage(AttributeTargets.Class)]
public class TransferRecordAttribute(string context) : Attribute
{
    public string Context { get; } = context;
}

public interface IDataTransferGateway
{
    IEnumerable<T> ReadAndDelete<T>(FilterSpecification<T>? filterSpecification = null!, SortDefinition<T>? sortDefinition = null, int? limit = null);
    void Save<T>(T obj);
}

public class DataTransferGateway(MongoDb mongoDb) : IDataTransferGateway
{
    private readonly IMongoDatabase _mongoDatabase = mongoDb.GetDatabase("DataTransfer"); 

    public IEnumerable<T> ReadAndDelete<T>(
        FilterSpecification<T>? filterSpecification = null!,
        SortDefinition<T>? sortDefinition = null,
        int? limit = null)
    {
        var collection = _mongoDatabase.GetCollection<T>(GetCollectionName<T>());
        foreach (var document in MongoCommonCore.Enumerate(collection, filterSpecification, sortDefinition, limit))
        {
            var bsonIdProperty = MongoCommonCore.GetBsonIdProperty<T>();
            var filter = Builders<T>.Filter.Eq(bsonIdProperty.Name, bsonIdProperty.GetValue(document));
            collection.DeleteOne(filter);
            
            yield return document;
        }
    }

    public void Save<T>(T obj)
    {
        MongoCommonCore.Save(_mongoDatabase.GetCollection<T>(GetCollectionName<T>()), obj);
    }

    private static string GetCollectionName<T>()
    {
        var type = typeof(T);

        if (!Attribute.IsDefined(type, typeof(TransferRecordAttribute)))
            throw new DataException("The record being transferred is not tagged as TransferRecord. Please do");

        var transferRecordAttribute = type.GetCustomAttribute<TransferRecordAttribute>();
        return transferRecordAttribute!.Context;
    }
}
