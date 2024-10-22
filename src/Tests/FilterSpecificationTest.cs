using FluentAssertions;
using Ragu.Mongo;

namespace Tests;

[TestFixture]
public class FilterSpecificationExtensionsTest
{
    [TestCase(false, false, false)]
    [TestCase(false, true, false)]
    [TestCase(true, false, false)]
    [TestCase(true, true, true)]
    public void AndCombine(bool a, bool b, bool expectation)
    {
        var left = new FilterSpecification<bool>.Optional(_ => a);
        var right = new FilterSpecification<bool>.Optional(_ => b);

        var combine = left.And(right);
        var actual = combine.SpecificationExpression.Compile()(false);

        actual.Should().Be(expectation);
    }

    [TestCase(false, false, false)]
    [TestCase(false, true, true)]
    [TestCase(true, false, true)]
    [TestCase(true, true, true)]
    public void OrCombine(bool a, bool b, bool expectation)
    {
        var left = new FilterSpecification<bool>.Optional(_ => a);
        var right = new FilterSpecification<bool>.Optional(_ => b);

        var combine = left.Or(right);
        var actual = combine.SpecificationExpression.Compile()(false);

        actual.Should().Be(expectation);
    }

    [TestCase(false, true)]
    [TestCase(true, false)]
    public void Negate(bool a, bool expectation)
    {
        var spec = new FilterSpecification<bool>.Optional(_ => a);
        var combine = FilterSpecification<bool>.Not(spec);
        var actual = combine.SpecificationExpression.Compile()(true);

        actual.Should().Be(expectation);
    }

    [TestCase(true, true, true, ConcatenationMethod.And, true)]
    [TestCase(false, true, true, ConcatenationMethod.And, false)]
    [TestCase(true, false, true, ConcatenationMethod.And, false)]
    [TestCase(true, false, false, ConcatenationMethod.And, false)]
    [TestCase(true, false, true, ConcatenationMethod.Or, true)]
    [TestCase(false, true, true, ConcatenationMethod.Or, false)]
    [TestCase(true, true, true, ConcatenationMethod.Or, true)]
    [TestCase(true, false, false, ConcatenationMethod.Or, false)]
    public void Aggregate(bool a, bool b, bool c, ConcatenationMethod concatenationMethod, bool expectation)
    {
        var filterSpecs = new FilterSpecification<object>[]
        {
            new FilterSpecification<object>.Mandatory(_ => a),
            new FilterSpecification<object>.Optional(_ => b),
            new FilterSpecification<object>.Optional(_ => c)
        };

        var aggregate = FilterSpecification<object>.Aggregate(filterSpecs, concatenationMethod);
        var actual = aggregate.SpecificationExpression.Compile()(false);
        actual.Should().Be(expectation);
    }

    [TestCase(ConcatenationMethod.And)]
    [TestCase(ConcatenationMethod.Or)]
    public void Aggregate_WhenEmpty(ConcatenationMethod concatenationMethod)
    {
        var filterSpecs = Array.Empty<FilterSpecification<object>>();
        var aggregate = FilterSpecification<object>.Aggregate(filterSpecs, concatenationMethod);
        var actual = aggregate.SpecificationExpression.Compile()(false);
        actual.Should().Be(true);
    }
}
