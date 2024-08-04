using System.Collections;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Bson.Serialization.Options;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;

namespace JVM_Mongo;

public class MongoDb : IDisposable
{
    private readonly IMongoClient _mongoClient;

    public record MinimalBsonDocument<T>([property: BsonId] T Id);
    public const int DefaultFetchLimit = 1000;

    static MongoDb()
    {
        BsonSerializer.RegisterSerializer(typeof(DateTimeOffset), new DateTimeOffsetSerializer(BsonType.String));
        ConventionRegistry.Register("EnumStringConvention", new ConventionPack { new EnumRepresentationConvention(BsonType.String) }, _ => true);
    }

    public MongoDb(string connectionString) => _mongoClient = new MongoClient(new MongoUrl(connectionString));
    public IMongoDatabase GetDatabase(string databaseName) => _mongoClient.GetDatabase(databaseName);
    public void Dispose() => _mongoClient.Cluster.Dispose();
}

public class DictionarySerializer<TDictionary, TKeySerializer>()
    : DictionarySerializerBase<TDictionary>(DictionaryRepresentation.ArrayOfDocuments, new TKeySerializer(), new StringSerializer())
        where TDictionary : class, IDictionary, new()
        where TKeySerializer : IBsonSerializer, new()
{
    protected override TDictionary CreateInstance()
    {
        return new TDictionary();
    }
}

public class EnumStringSerializer<TEnum>() : EnumSerializer<TEnum>(BsonType.String) where TEnum : struct, Enum;

public enum MongoErrorCode
{
    DuplicateKey = 11000, // 0x00002AF8
}

public enum MongoAction
{
    Update = 0,
    Insert = 1
}

