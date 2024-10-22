using FluentAssertions;
using FluentAssertions.Extensions;
using MongoDB.Bson;
using MongoDB.Driver;
using Ragu.Mongo;
using static Tests.Constants;

namespace Tests;

public class DataTransferGatewayTest
{
    private IMongoDatabase _mongoDatabase = null!;
    private IDataTransferGateway _dataTransferGateway = null!;

    [OneTimeSetUp]
    public void Setup()
    {
        _mongoDatabase = new MongoDb(MongoLocalConnection).GetDatabase("DataTransfer");
        _dataTransferGateway = new DataTransferGateway(new MongoDb(MongoLocalConnection));
    }

    [TearDown]
    public void TearDown() => _mongoDatabase.DropCollection(MinimalDocument.TransferContext);

    [Test]
    public void GetFiltered_ByTimestamp()
    {
        var generics = new[]
        {
            new MinimalDocument(Id: ObjectId.GenerateNewId().ToString(), "Im Unique", Timestamp: 20.November(2023).AsUtc()),
            new MinimalDocument(Id: ObjectId.GenerateNewId().ToString(), "Im Unique Again", Timestamp: 20.January(2023).AsUtc()),
            new MinimalDocument(Id: ObjectId.GenerateNewId().ToString(), "Really?", Timestamp: 20.April(2023).AsUtc())
        };
        _mongoDatabase.GetCollection<MinimalDocument>(MinimalDocument.TransferContext).InsertMany(generics);

        var actual = _dataTransferGateway.ReadAndDelete(MinimalDocument.LaterThenSpec(20.September(2023).AsUtc())).ToList();
        actual.Should().BeEquivalentTo(new[] { generics[1], generics[2] });

        _mongoDatabase
            .GetCollection<MinimalDocument>(MinimalDocument.TransferContext)
            .Find(_ => true)
            .ToList()
            .Should()
            .BeEquivalentTo(new[] { generics[0] });
    }

    [Test]
    public void GetFiltered_WithLimit()
    {
        var generics = new[]
        {
            new MinimalDocument(Id: ObjectId.GenerateNewId().ToString(), "Im Unique", Timestamp: 20.November(2023).AsUtc()),
            new MinimalDocument(Id: ObjectId.GenerateNewId().ToString(), "Im Unique Again", Timestamp: 20.January(2023).AsUtc()),
            new MinimalDocument(Id: ObjectId.GenerateNewId().ToString(), "Really?", Timestamp: 20.April(2023).AsUtc())
        };
        _mongoDatabase.GetCollection<MinimalDocument>(MinimalDocument.TransferContext).InsertMany(generics);

        var actual = _dataTransferGateway.ReadAndDelete<MinimalDocument>(limit: 1).ToList();
        actual.Count.Should().Be(1);

        _mongoDatabase
            .GetCollection<MinimalDocument>(MinimalDocument.TransferContext)
            .Find(_ => true)
            .ToList()
            .Should()
            .BeEquivalentTo(new[] { generics[1], generics[2] });
    }

    [Test]
    public void Save()
    {
        var generic = new MinimalDocument(Id: "6108e5a2b056b076842452bf", "Im Unique", Timestamp: 20.November(2023).AsUtc());
        _dataTransferGateway.Save(generic);

        var actual = _mongoDatabase.GetCollection<MinimalDocument>(MinimalDocument.TransferContext).Find(_ => true).SingleOrDefault();
        actual.Should().BeEquivalentTo(generic);
    }
}