using FluentAssertions;
using FluentAssertions.Extensions;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using Ragu.Mongo;
using Tests.Utils;
using static Tests.Constants;

namespace Tests;

public class BasicMongoGatewayTest
{
    private IMongoDatabase _mongoDatabase = null!;
    private IBasicMongoGateway<MinimalDocument> _basicMongoGateway = null!;

    private static readonly IList<MinimalDocument> Documents = new[]
    {
        new MinimalDocument("ID_1", "Im Unique", 13.February(2024).AsUtc()),
        new MinimalDocument("ID_2", "See the index protecting me", 13.February(2024).AsUtc()),
        new MinimalDocument("ID_3", "Its good to be Unique", 14.February(2024).AsUtc()),
    };

    private const string BasicCollectionName = "BasicTestCollection";

    internal class RetryableException : Exception
    {
        internal RetryableException(string message) : base(message) { }
    }

    [SetUp]
    public void Setup()
    {
        _mongoDatabase = new MongoDb(MongoLocalConnection).GetDatabase(TestsDatabaseName);
        _basicMongoGateway = new BasicMongoGateway<MinimalDocument>(_mongoDatabase, BasicCollectionName, MinimalDocument.MongoIndexes());

        _mongoDatabase.GetCollection<MinimalDocument>(BasicCollectionName).InsertMany(Documents);
    }

    [TearDown]
    public void TearDown() => _mongoDatabase.DropCollection(BasicCollectionName);

    [Test]
    public void Get_Some() => _basicMongoGateway.Get(MinimalDocument.GetIdSpec("ID_1")).Should().BeSome(Documents[0]);
    
    [Test]
    public void Get_None() => _basicMongoGateway.Get(MinimalDocument.GetIdSpec("ID_4")).Should().BeNone<MinimalDocument>();

    [Test]
    public void Enumerate() => _basicMongoGateway.Enumerate().ToList().Should().BeEquivalentTo(Documents);

    private record DocumentsByDay([property: BsonId] int Day, int Count);

    [Test]
    public void Aggregate_Group()
    {
        var actual = _basicMongoGateway.Aggregate<DocumentsByDay>(MinimalDocument.All(), 
            new AggregationSpecification<MinimalDocument>.Group(@"
            {
                ""_id"" : {
                    ""$dayOfMonth"" : ""$Timestamp""
                },
                ""Count"" : {
                    ""$sum"" : NumberInt(1)
                }
            }"));
        actual.Should().BeEquivalentTo(new DocumentsByDay[]
        {
            new(13, 2),
            new(14, 1)
        });
    }

    [Test]
    public void Save()
    {
        var expected = new MinimalDocument("ID_1", "Im Unique", 25.February(2024).AsUtc());
        _basicMongoGateway.Save(expected);

        var actual = _mongoDatabase.GetCollection<MinimalDocument>(BasicCollectionName).Find(md => md.Id == "ID_1").Single();
        actual.Should().BeEquivalentTo(expected);
    }

    [Test]
    public void Save_Refining()
    {
        var expected = new MinimalDocument("ID_1", "Im Unique", 25.February(2024).AsUtc());
        _basicMongoGateway.Save(expected, refineFunc: e => e with { UniqueParameter = "I changed!" });

        var actual = _mongoDatabase.GetCollection<MinimalDocument>(BasicCollectionName).Find(md => md.Id == "ID_1").Single();
        actual.Should().BeEquivalentTo(expected with { UniqueParameter = "I changed!" });
    }

    [Test]
    public void Save_ThrowCustomException()
    {
        Assert.Throws<RetryableException>(() =>
        {
            var minimalDocument = new MinimalDocument("ID_1", "Its good to be Unique", 25.February(2024).AsUtc());
            _basicMongoGateway.Save(minimalDocument, customExceptionOnDuplicateKey: new RetryableException("Some text"));
        }); 
    }

    [Test]
    public void Delete_One()
    {
        _basicMongoGateway.Delete(MinimalDocument.GetIdSpec("ID_1"));
        _mongoDatabase
            .GetCollection<MinimalDocument>(BasicCollectionName)
            .Find(md => md.Id == "ID_1")
            .SingleOrDefault()
            .Should()
            .BeNull();
    }

    [Test]
    public void Delete_Many()
    {
        _basicMongoGateway.Delete(MinimalDocument.All());
        _mongoDatabase
            .GetCollection<MinimalDocument>(BasicCollectionName)
            .Find(_ => true)
            .ToList()
            .Should()
            .BeEmpty();
    }
}