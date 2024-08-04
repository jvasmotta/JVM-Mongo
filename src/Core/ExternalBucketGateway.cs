using MongoDB.Driver;

namespace JVM_Mongo;

public interface IExternalBucketGateway
{
    IEnumerable<T> LazilyReadFromBucket<T>(string collectionName, FindOptions? findOptions = null);
    IMongoCollection<T> CreateBucketCollection<T>(string collectionName);
    bool IsBucketEmpty<T>(string collectionName);
    DeleteResult DeleteFromBucket<T>(string collectionName, FilterSpecification<T> bucketFilterSpecification);
    void DropBucketCollection(string collectionName);
}

public class ExternalBucketGateway(IMongoDatabase mongoDatabase) : IExternalBucketGateway
{
    public IEnumerable<T> LazilyReadFromBucket<T>(string collectionName, FindOptions? findOptions = null)
    {
        var cursor = mongoDatabase.GetCollection<T>(collectionName).Find(_ => true, findOptions).ToCursor();
        while (cursor.MoveNext())
            foreach (var item in cursor.Current)
                yield return item;
    }

    public IMongoCollection<T> CreateBucketCollection<T>(string collectionName) => mongoDatabase.GetCollection<T>(collectionName);

    public bool IsBucketEmpty<T>(string collectionName) => mongoDatabase.GetCollection<T>(collectionName).CountDocuments(_ => true) <= 0;

    public DeleteResult DeleteFromBucket<T>(string collectionName, FilterSpecification<T> bucketFilterSpecification) =>
        mongoDatabase.GetCollection<T>(collectionName).DeleteOne(bucketFilterSpecification.SpecificationExpression);

    public void DropBucketCollection(string collectionName) => mongoDatabase.DropCollection(collectionName);
}