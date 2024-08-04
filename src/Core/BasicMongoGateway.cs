using System.Linq.Expressions;
using System.Reflection;
using DiscriminatedOnions;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace JVM_Mongo;

public interface IBasicMongoGateway<T>
{
    Option<T> Get(FilterSpecification<T>? filterSpecification = null!);
    IEnumerable<T> Enumerate(FilterSpecification<T>? filterSpecification = null!, SortDefinition<T>? sortDefinition = null, int? limit = null);
    IReadOnlyCollection<TOut> Aggregate<TOut>(FilterSpecification<T> filterSpecification, params AggregationSpecification<T>[] aggregationSpecifications);
    void Save(T document, Func<T, T>? refineFunc = null, Exception? customExceptionOnDuplicateKey = null);
    bool Delete(FilterSpecification<T> filterSpecification);
}

public class BasicMongoGateway <T> : IBasicMongoGateway<T> where T : class
{
    private readonly IMongoCollection<T> _collection;
    private readonly PropertyInfo _idProperty;

    public record FieldIndex(string IndexName, bool IsUnique, IEnumerable<IndexNodeExpression> IndexNodeExpressions);
    public record IndexNodeExpression(Expression<Func<T, object>> FieldExpression, bool IsAscending);
    
    public BasicMongoGateway(IMongoDatabase mongoDatabase, string collectionName, params FieldIndex[] indexesToCreate)
    {
        _collection = mongoDatabase.GetCollection<T>(collectionName);
        _idProperty = GetBsonIdProperty();

        foreach (var fieldIndex in indexesToCreate)
        {
            _collection.Indexes.CreateOne(new CreateIndexModel<T>(
                keys: fieldIndex.IndexNodeExpressions.Aggregate(Builders<T>.IndexKeys.Combine(), (current, nodeExpression) => 
                    nodeExpression.IsAscending 
                        ? current.Ascending(nodeExpression.FieldExpression) 
                        : current.Descending(nodeExpression.FieldExpression)), 
                options: new CreateIndexOptions { Unique = fieldIndex.IsUnique, Name = fieldIndex.IndexName }));
        }
    }

    public Option<T> Get(FilterSpecification<T>? filterSpecification = null!)
    {
        return Option.OfObj(_collection
            .Find(filterSpecification?.SpecificationExpression ?? FilterDefinition<T>.Empty)
            .SingleOrDefault());
    }

    public IEnumerable<T> Enumerate(FilterSpecification<T>? filterSpecification = null!, SortDefinition<T>? sortDefinition = null, int? limit = null)
    {
        var cursor = _collection.Find(filterSpecification?.SpecificationExpression ?? Builders<T>.Filter.Empty).Limit(limit).ToCursor();
        while (cursor.MoveNext())
            foreach (var obj in cursor.Current)
                yield return obj;
    }

    public IReadOnlyCollection<TOut> Aggregate<TOut>(FilterSpecification<T> filterSpecification, params AggregationSpecification<T>[] aggregationSpecifications)
    {
        var matchStage = PipelineStageDefinitionBuilder.Match(filterSpecification.SpecificationExpression);
        var stages = new List<IPipelineStageDefinition> { matchStage };
        stages.AddRange(aggregationSpecifications.Select(stage => stage.ToPipeline<TOut>()));

        var pipeline = PipelineDefinition<T, TOut>.Create(stages);
        return _collection.Aggregate(pipeline).ToList();
    }

    public void Save(T document, Func<T, T>? refineFunc = null, Exception? customExceptionOnDuplicateKey = null)
    {
        try
        {
            var idValue = _idProperty.GetValue(document);
            var filter = Builders<T>.Filter.Eq(_idProperty.Name, idValue);
            
            if (refineFunc is not null)
                document = refineFunc(document);
            
            _collection.ReplaceOne(filter, document, new ReplaceOptions { IsUpsert = true });
        }
        catch (MongoWriteException e) when (e.WriteError.Code == (int)MongoErrorCode.DuplicateKey)
        {
            if (customExceptionOnDuplicateKey is not null) 
                throw customExceptionOnDuplicateKey;

            throw;
        }
    }

    public bool Delete(FilterSpecification<T> filterSpecification)
    {
         var deleteResult = _collection.DeleteMany(filterSpecification.SpecificationExpression);
        return deleteResult.DeletedCount >= 1;
    }

    private static PropertyInfo GetBsonIdProperty()
    {
        var properties = typeof(T)
            .GetProperties()
            .SingleOrDefault(p => Attribute.IsDefined(p, typeof(BsonIdAttribute)));

        return properties ?? throw new InvalidOperationException($"No property with [BsonId] attribute found in type {typeof(T)}.");
    }
}