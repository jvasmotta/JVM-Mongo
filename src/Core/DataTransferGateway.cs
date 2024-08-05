using System.Data;
using System.Reflection;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace JVM_Mongo;


[AttributeUsage(AttributeTargets.Class)]
public class TransferRecordAttribute(string context) : Attribute
{
    public string Context { get; } = context;
}

public class DataTransferGateway(MongoDb mongoDb)
{
    private readonly IMongoDatabase _mongoDatabase = mongoDb.GetDatabase("DataTransfer"); 

    public IEnumerable<T> ReadAndDelete<T>()
    {
        var collection = _mongoDatabase.GetCollection<T>(GetCollectionName<T>());
        foreach (var document in MongoCommonCore.Enumerate(collection))
        {
            var bsonIdProperty = GetBsonIdProperty<T>();
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

    private static PropertyInfo GetBsonIdProperty<T>()
    {
        var properties = typeof(T)
            .GetProperties()
            .SingleOrDefault(p => Attribute.IsDefined(p, typeof(BsonIdAttribute)));

        return properties ?? throw new InvalidOperationException($"No property with [BsonId] attribute found in type {typeof(T)}.");
    }
}
