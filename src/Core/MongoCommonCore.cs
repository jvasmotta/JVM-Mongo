using DiscriminatedOnions;
using MongoDB.Driver;

namespace JVM_Mongo;

internal static class MongoCommonCore
{
    internal static Option<T> Get<T>(IMongoCollection<T> collection, FilterSpecification<T>? filterSpecification = null!) where T : class
    {
        return Option.OfObj(collection
            .Find(filterSpecification?.SpecificationExpression ?? FilterDefinition<T>.Empty)
            .SingleOrDefault());
    }

    internal static IEnumerable<T> Enumerate<T>(
        IMongoCollection<T> collection,
        FilterSpecification<T>? filterSpecification = null!,
        SortDefinition<T>? sortDefinition = null,
        int? limit = null)
    {
        var cursor = collection.Find(filterSpecification?.SpecificationExpression ?? Builders<T>.Filter.Empty).Limit(limit).ToCursor();
        while (cursor.MoveNext())
            foreach (var obj in cursor.Current)
                yield return obj;
    }

    internal static IReadOnlyCollection<TOut> Aggregate<T, TOut>(
        IMongoCollection<T> collection,
        FilterSpecification<T> filterSpecification,
        params AggregationSpecification<T>[] aggregationSpecifications)
    {
        var matchStage = PipelineStageDefinitionBuilder.Match(filterSpecification.SpecificationExpression);
        var stages = new List<IPipelineStageDefinition> { matchStage };
        stages.AddRange(aggregationSpecifications.Select(stage => stage.ToPipeline<TOut>()));

        var pipeline = PipelineDefinition<T, TOut>.Create(stages);
        return collection.Aggregate(pipeline).ToList();
    }

    internal static void Save<T>(
        IMongoCollection<T> collection,
        T document,
        bool isUpsert = false,
        FilterDefinition<T>? filterDefinition = null,
        Func<T, T>? refineFunc = null,
        Exception? customExceptionOnDuplicateKey = null)
    {
        try
        {
            if (refineFunc is not null)
                document = refineFunc(document);

            if (isUpsert)
                collection.ReplaceOne(filterDefinition, document, new ReplaceOptions { IsUpsert = true });

            collection.InsertOne(document);
        }
        catch (MongoWriteException e) when (e.WriteError.Code == (int)MongoErrorCode.DuplicateKey)
        {
            if (customExceptionOnDuplicateKey is not null)
                throw customExceptionOnDuplicateKey;

            throw;
        }
    }

    internal static bool Delete<T>(IMongoCollection<T> collection, FilterSpecification<T> filterSpecification)
    {
        var deleteResult = collection.DeleteMany(filterSpecification.SpecificationExpression);
        return deleteResult.DeletedCount >= 1;
    }
}