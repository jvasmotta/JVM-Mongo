using System.Collections;
using System.Linq.Expressions;

namespace JVM_Mongo;

public record struct TimeWindowInterval(DateTime FromCreatedAt, DateTime UntilCreatedAt);

public abstract record FilterSpecification<T>(Expression<Func<T, bool>> SpecificationExpression)
{
    public record Mandatory(Expression<Func<T, bool>> SpecificationExpression) : FilterSpecification<T>(SpecificationExpression);
    public record Optional(Expression<Func<T, bool>> SpecificationExpression) : FilterSpecification<T>(SpecificationExpression);

    public static FilterSpecification<T> Aggregate(IEnumerable<FilterSpecification<T>> specs, ConcatenationMethod concatenationMethod)
    {
        var specsArray = specs.ToArray();
        if (!specsArray.Any())
            return new Mandatory(_ => true);

        return specsArray.OrderByDescending(s => s.GetTypePriority()).Aggregate((currentSpec, nextSpec) =>
        {
            return nextSpec switch
            {
                Mandatory _ => currentSpec.And(nextSpec),
                Optional _ => concatenationMethod is ConcatenationMethod.And ? currentSpec.And(nextSpec) : currentSpec.Or(nextSpec),
                _ => throw new ArgumentOutOfRangeException()
            };
        });
    }

    public FilterSpecification<T> And(FilterSpecification<T> specsToTide) => CombineSpecification(specsToTide, Expression.AndAlso);
    public FilterSpecification<T> Or(FilterSpecification<T> specsToTide) => CombineSpecification(specsToTide, Expression.OrElse);
    public static FilterSpecification<T> Not(FilterSpecification<T> specToTide)
    {
        var arg = Expression.Parameter(typeof(T));
        var combined = Expression.Not(
            new ReplaceParameterVisitor { { specToTide.SpecificationExpression.Parameters.Single(), arg } }.Visit(specToTide.SpecificationExpression.Body)!);

        return CreateTyped(specToTide, combined, arg);
    }

    private int GetTypePriority()
    {
        return this switch
        {
            Mandatory => 1,
            Optional => 2,
            _ => throw new ArgumentOutOfRangeException($"{nameof(GetTypePriority)}-{nameof(FilterSpecification<T>)}")
        };
    }

    private FilterSpecification<T> CombineSpecification(FilterSpecification<T> specToTide, Func<Expression, Expression, BinaryExpression> combiner)
    {
        var arg = Expression.Parameter(typeof(T));
        var combined = combiner.Invoke(
            new ReplaceParameterVisitor { { SpecificationExpression.Parameters.Single(), arg } }.Visit(SpecificationExpression.Body),
            new ReplaceParameterVisitor { { specToTide.SpecificationExpression.Parameters.Single(), arg } }.Visit(specToTide.SpecificationExpression.Body));

        return CreateTyped(specToTide, combined, arg);
    }

    private static FilterSpecification<T> CreateTyped(FilterSpecification<T> specToTide, Expression combined, ParameterExpression arg)
    {
        return specToTide switch
        {
            Mandatory _ => new Mandatory(Expression.Lambda<Func<T, bool>>(combined, arg)),
            Optional _ => new Optional(Expression.Lambda<Func<T, bool>>(combined, arg)),
            _ => throw new ArgumentOutOfRangeException(nameof(specToTide))
        };
    }

    private class ReplaceParameterVisitor : ExpressionVisitor, IEnumerable<KeyValuePair<ParameterExpression, ParameterExpression>>
    {
        private readonly Dictionary<ParameterExpression, ParameterExpression> _parameterMappings = new();
        protected override Expression VisitParameter(ParameterExpression node) => _parameterMappings.GetValueOrDefault(node, node);
        public void Add(ParameterExpression parameterToReplace, ParameterExpression replaceWith) => _parameterMappings.Add(parameterToReplace, replaceWith);
        public IEnumerator<KeyValuePair<ParameterExpression, ParameterExpression>> GetEnumerator() => _parameterMappings.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}

public enum ConcatenationMethod
{
    And,
    Or
}