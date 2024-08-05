using JVM_Mongo;
using MongoDB.Bson.Serialization.Attributes;

namespace Tests;

[TransferRecord(TransferContext)]
public record MinimalDocument([property: BsonId] string Id, string UniqueParameter, DateTime? Timestamp)
{
    public const string TransferContext = "TestingDT";
    public static FilterSpecification<MinimalDocument> GetIdSpec(string id) => new FilterSpecification<MinimalDocument>.Mandatory(md => md.Id == id);
    public static FilterSpecification<MinimalDocument> All() => new FilterSpecification<MinimalDocument>.Optional(md => true);
    public static FilterSpecification<MinimalDocument> LaterThenSpec(DateTime timestamp) => new FilterSpecification<MinimalDocument>.Optional(md => md.Timestamp < timestamp);

    public static BasicMongoGateway<MinimalDocument>.FieldIndex[] MongoIndexes()
    {
        return new[]
        {
            new BasicMongoGateway<MinimalDocument>.FieldIndex(
                IndexName: "UniqueParameters",
                IsUnique: true,
                IndexNodeExpressions: new []
                {
                    new BasicMongoGateway<MinimalDocument>.IndexNodeExpression(FieldExpression: md => md.UniqueParameter, IsAscending: true)
                }),
            new BasicMongoGateway<MinimalDocument>.FieldIndex(
                IndexName: "MultipleIndexes",
                IsUnique: false,
                IndexNodeExpressions: new []
                {
                    new BasicMongoGateway<MinimalDocument>.IndexNodeExpression(FieldExpression: md => md.UniqueParameter, IsAscending: true),
                    new BasicMongoGateway<MinimalDocument>.IndexNodeExpression(FieldExpression: md => md.Timestamp!, IsAscending: false)
                })
        };
    }
}
