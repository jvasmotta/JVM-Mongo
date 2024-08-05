using System.Linq.Expressions;
using System.Reflection;
using DiscriminatedOnions;
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
        _idProperty = MongoCommonCore.GetBsonIdProperty<T>();

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
        return MongoCommonCore.Get(_collection, filterSpecification);
    }

    public IEnumerable<T> Enumerate(FilterSpecification<T>? filterSpecification = null!, SortDefinition<T>? sortDefinition = null, int? limit = null)
    {
        return MongoCommonCore.Enumerate(_collection, filterSpecification, sortDefinition, limit);
    }

    public IReadOnlyCollection<TOut> Aggregate<TOut>(FilterSpecification<T> filterSpecification, params AggregationSpecification<T>[] aggregationSpecifications)
    {
        return MongoCommonCore.Aggregate<T, TOut>(_collection, filterSpecification, aggregationSpecifications);
    }

    public void Save(T document, Func<T, T>? refineFunc = null, Exception? customExceptionOnDuplicateKey = null)
    {
        var idValue = _idProperty.GetValue(document);
        var filter = Builders<T>.Filter.Eq(_idProperty.Name, idValue);

        MongoCommonCore.Save(_collection, document, isUpsert: true, filter, refineFunc, customExceptionOnDuplicateKey);
    }

    public bool Delete(FilterSpecification<T> filterSpecification)
    {
         var deleteResult = _collection.DeleteMany(filterSpecification.SpecificationExpression);
        return deleteResult.DeletedCount >= 1;
    }
}