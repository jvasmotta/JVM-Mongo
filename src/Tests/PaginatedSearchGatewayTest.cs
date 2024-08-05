using System.Text.Json;
using FluentAssertions;
using FluentAssertions.Extensions;
using JVM_Mongo;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using static Tests.Constants;

namespace Tests;

[TestFixture]
public class PaginatedSearchGatewayTest
{
    private IMongoDatabase _mongoDatabase = null!;
    private IPaginatedSearchGateway _paginatedSearchGateway = null!;

    private static readonly Func<ObjectId> GenerateObjectId = () => ObjectId.Parse("6444403d73cc9fa542610a76");
    private const string GeneratedObjectId = "6444403d73cc9fa542610a76";

    internal record Metadata(string ClientId)
    {
        internal static Metadata Example() => new("ClientId");
    }

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _mongoDatabase = new MongoDb(MongoLocalConnection).GetDatabase(TestsDatabaseName);
        _paginatedSearchGateway = new PaginatedSearchGateway(_mongoDatabase, GenerateObjectId);
    }

    [TearDown]
    public async Task TearDown()
    {
        await _mongoDatabase.DropCollectionAsync(PaginatedSearchGateway.HeaderCollectionName);
        await _mongoDatabase.DropCollectionAsync($"PaginatedSearchResult_{GeneratedObjectId}");
    }

    private static async IAsyncEnumerable<TestDocument> GenerateTestDocuments(int totalElements)
    {
        foreach (var i in Enumerable.Range(1, totalElements))
        {
            await Task.Yield();
            yield return new TestDocument(i.ToString(), i);
        }
    }

    [Test]
    [Order(1)]
    public async Task CreateFirstPaginated()
    {
        const int pageSize = 10;
        const int totalElements = 200;
        var result = await _paginatedSearchGateway.GetOrCreate(
            metadata: Metadata.Example(), 
            searchRequest: "testRequest", 
            fetchFunc: () => GenerateTestDocuments(totalElements),
            page: 0, 
            size: pageSize);

        result.Results.Count().Should().Be(pageSize);
        result.Results.First().Value.Should().Be(1);
        result.Results.Last().Value.Should().Be(10);

        await Task.Delay(TimeSpan.FromSeconds(3));
        var insertedDocuments = await _mongoDatabase
            .GetCollection<TestDocument>($"PaginatedSearchResult_{GeneratedObjectId}")
            .Find(FilterDefinition<TestDocument>.Empty).ToListAsync();

        insertedDocuments.Count.Should().Be(totalElements);
        insertedDocuments.First().Value.Should().Be(1);
        insertedDocuments.Last().Value.Should().Be(totalElements);
    }

    [Test]
    [Order(2)]
    public async Task CreateSecondPaginated()
    {
        const int pageSize = 10;
        const int totalElements = 200;
        var result = await _paginatedSearchGateway.GetOrCreate(
            metadata: Metadata.Example(),
            searchRequest: "testRequest",
            fetchFunc: () => GenerateTestDocuments(totalElements),
            page: 1,
            size: pageSize);

        result.Results.Count().Should().Be(pageSize);
        result.Results.First().Value.Should().Be(11);
        result.Results.Last().Value.Should().Be(20);

        await Task.Delay(TimeSpan.FromSeconds(3));
        var insertedDocuments = await _mongoDatabase
            .GetCollection<TestDocument>($"PaginatedSearchResult_{GeneratedObjectId}")
            .Find(FilterDefinition<TestDocument>.Empty).ToListAsync();

        insertedDocuments.Count.Should().Be(totalElements);
        insertedDocuments.First().Value.Should().Be(1);
        insertedDocuments.Last().Value.Should().Be(totalElements);
    }

    [Test]
    [Order(3)]
    public async Task GetFirstPaginated()
    {
        const int pageSize = 10;
        const int totalElements = 200;

        await _mongoDatabase.GetCollection<PaginatedHeader<Metadata>>(PaginatedSearchGateway.HeaderCollectionName).InsertOneAsync(
            new PaginatedHeader<Metadata>(
                CollectionName: $"PaginatedSearchResult_{GeneratedObjectId}",
                Metadata: Metadata.Example(), 
                SearchParameters: JsonSerializer.SerializeToUtf8Bytes("testRequest"),
                TotalElements: null!,
                CreatedAt: 23.April(2023).AsUtc(),
                ExpiresAt: 23.April(2023).AsUtc().AddMinutes(30)));

        var testDocuments = Enumerable.Range(1, totalElements).Select(i => new TestDocument(i.ToString(), i)).ToList();
        await _mongoDatabase.GetCollection<TestDocument>($"PaginatedSearchResult_{GeneratedObjectId}").InsertManyAsync(testDocuments);

        var result = await _paginatedSearchGateway.GetOrCreate(
            metadata: Metadata.Example(),
            searchRequest: "testRequest",
            fetchFunc: () => GenerateTestDocuments(totalElements),
            page: 0,
            size: pageSize);

        result.Results.Count().Should().Be(10);
        result.Results.First().Value.Should().Be(1);
        result.Results.Last().Value.Should().Be(10);
    }

    [Test]
    [Order(4)]
    public void CleanExpiredResults()
    {
        var headers = new[]
        {
            new PaginatedHeader<Metadata>("ResultTempOne", Metadata.Example(), Array.Empty<byte>(), null, 22.April(2023).AsUtc(), 25.April(2023).AsUtc()),
            new PaginatedHeader<Metadata>("ResultTempTwo", Metadata.Example(), Array.Empty<byte>(), null, 22.April(2023).AsUtc(), 23.April(2023).AsUtc())
        };
        _mongoDatabase.GetCollection<PaginatedHeader<Metadata>>(PaginatedSearchGateway.HeaderCollectionName).InsertMany(headers);
        _mongoDatabase.GetCollection<TestDocument>("ResultTempOne").InsertOne(new TestDocument("1", 1));
        _mongoDatabase.GetCollection<TestDocument>("ResultTempTwo").InsertOne(new TestDocument("1", 1));

        _paginatedSearchGateway.CleanExpiredResults<Metadata>(24.April(2023).AsUtc());

        var actual = _mongoDatabase.GetCollection<PaginatedHeader<Metadata>>(PaginatedSearchGateway.HeaderCollectionName).Find(_ => true).ToList();
        actual.Should().BeEquivalentTo(new[] { headers[0] });

        _mongoDatabase.ListCollectionNames().ToList().Should().BeEquivalentTo(PaginatedSearchGateway.HeaderCollectionName, "ResultTempOne");
        _mongoDatabase.DropCollection("ResultTempOne");
    }

    [Test]
    [Order(5)]
    public void UpdateHeaderTotalElements()
    {
        var header = new PaginatedHeader<Metadata>(
            CollectionName: $"PaginatedSearchResult_{GeneratedObjectId}",
            Metadata: Metadata.Example(), 
            SearchParameters: JsonSerializer.SerializeToUtf8Bytes("testRequest"),
            TotalElements: null,
            CreatedAt: 22.April(2023).AsUtc(),
            ExpiresAt: 25.April(2023).AsUtc());
        _mongoDatabase.GetCollection<PaginatedHeader<Metadata>>(PaginatedSearchGateway.HeaderCollectionName).InsertOne(header);

        _paginatedSearchGateway.UpdateHeaderTotalElements(Metadata.Example(), "testRequest", 200);
        _mongoDatabase
            .GetCollection<PaginatedHeader<Metadata>>(PaginatedSearchGateway.HeaderCollectionName)
            .Find(_ => true)
            .Single()
            .Should()
            .BeEquivalentTo(header with { TotalElements = 200 });
    }

    private record TestDocument([property: BsonId] string Id, int Value);
}