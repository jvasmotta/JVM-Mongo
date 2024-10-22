using MongoDB.Bson;
using MongoDB.Driver;

namespace Ragu.Mongo;

public abstract record AggregationSpecification<T>(BsonDocument Document)
{
    public record Group(string Query) : AggregationSpecification<T>(BsonDocument.Parse(Query));

    public IPipelineStageDefinition ToPipeline<TResult>()
    {
        return this switch
        {
            Group args => PipelineStageDefinitionBuilder.Group<T, TResult>(args.Document),
            _ => throw new ArgumentOutOfRangeException(ToString())
        };
    }
}
