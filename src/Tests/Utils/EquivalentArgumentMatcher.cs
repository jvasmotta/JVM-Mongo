using FluentAssertions;
using FluentAssertions.Equivalency;
using NSubstitute.Core;
using NSubstitute.Core.Arguments;

namespace Tests.Utils;

public class EquivalentArgumentMatcher<T>(
    T expected,
    Func<EquivalencyAssertionOptions<T>, EquivalencyAssertionOptions<T>> options)
    : IArgumentMatcher, IDescribeNonMatches
{
    private readonly ArgumentFormatter _defaultArgumentFormatter = new();

    public EquivalentArgumentMatcher(T expected) : this(expected, x => x.IncludingAllDeclaredProperties()) { }

    public override string ToString() => _defaultArgumentFormatter.Format(expected, false);

    public string DescribeFor(object? argument)
    {
        try
        {
            argument.As<T>().Should().BeEquivalentTo(expected, options);
            return string.Empty;
        }
        catch (Exception ex)
        {
            return ex.Message;
        }
    }

    public bool IsSatisfiedBy(object? argument)
    {
        try
        {
            argument.As<T>().Should().BeEquivalentTo(expected, options);
            return true;
        }
        catch
        {
            return false;
        }
    }
}