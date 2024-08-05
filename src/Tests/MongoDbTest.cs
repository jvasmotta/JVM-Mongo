using FluentAssertions;
using JVM_Mongo;
using MongoDB.Driver;
using static Tests.Constants;

namespace Tests;

public class MongoDbTest
{
    private IMongoDatabase _mongoDatabase = null!;
    private const string TempCollectionName = "TestTempCollectionName";

    [OneTimeSetUp]
    public void Setup() => _mongoDatabase = new MongoDb(MongoLocalConnection).GetDatabase(TestsDatabaseName);

    [TearDown]
    public void TearDown() => _mongoDatabase.DropCollection(TempCollectionName);

    [Test]
    public void BulkWriteWithDuplicates()
    {
        _mongoDatabase.GetCollection<MongoDb.MinimalBsonDocument<string>>(TempCollectionName).BulkWrite(new[]
        {
            new ReplaceOneModel<MongoDb.MinimalBsonDocument<string>>(
                Builders<MongoDb.MinimalBsonDocument<string>>.Filter.Eq(md => md.Id, "A"),
                new MongoDb.MinimalBsonDocument<string>("A")) { IsUpsert = true },

            new ReplaceOneModel<MongoDb.MinimalBsonDocument<string>>(
                Builders<MongoDb.MinimalBsonDocument<string>>.Filter.Eq(md => md.Id, "B"),
                new MongoDb.MinimalBsonDocument<string>("B")) { IsUpsert = true },

            new ReplaceOneModel<MongoDb.MinimalBsonDocument<string>>(
                Builders<MongoDb.MinimalBsonDocument<string>>.Filter.Eq(md => md.Id, "C"),
                new MongoDb.MinimalBsonDocument<string>("C")) { IsUpsert = true }
        });

        _mongoDatabase.GetCollection<MongoDb.MinimalBsonDocument<string>>(TempCollectionName).BulkWrite(new[]
        {
            new ReplaceOneModel<MongoDb.MinimalBsonDocument<string>>(
                Builders<MongoDb.MinimalBsonDocument<string>>.Filter.Eq(md => md.Id, "C"),
                new MongoDb.MinimalBsonDocument<string>("C")) { IsUpsert = true },

            new ReplaceOneModel<MongoDb.MinimalBsonDocument<string>>(
                Builders<MongoDb.MinimalBsonDocument<string>>.Filter.Eq(md => md.Id, "D"),
                new MongoDb.MinimalBsonDocument<string>("D")) { IsUpsert = true }
        });

        _mongoDatabase.GetCollection<MongoDb.MinimalBsonDocument<string>>(TempCollectionName).Find(_ => true).ToList()
            .Should()
            .BeEquivalentTo(new[]
            {
                new MongoDb.MinimalBsonDocument<string>("A"),
                new MongoDb.MinimalBsonDocument<string>("B"),
                new MongoDb.MinimalBsonDocument<string>("C"),
                new MongoDb.MinimalBsonDocument<string>("D")
            });
    }

    [Test]
    public void GetSomeCollection()
    {
        _mongoDatabase.CreateCollection(TempCollectionName);
        _mongoDatabase.ListCollectionNames().ToList().Contains(TempCollectionName).Should().BeTrue();
    }

    [Test]
    public void GetNoneCollection() => _mongoDatabase.ListCollectionNames().ToList().Contains(TempCollectionName).Should().BeFalse();
}