using FluentAssertions;
using FluentAssertions.Equivalency;
using NSubstitute.Core;
using NSubstitute.Core.Arguments;

namespace Tests.Utils;

public class EquivalentCollectionArgumentMatcher<T>(
    object expected,
    Func<EquivalencyAssertionOptions<T>, EquivalencyAssertionOptions<T>> options)
    : IArgumentMatcher, IDescribeNonMatches
{
    private readonly ArgumentFormatter _defaultArgumentFormatter = new ArgumentFormatter();
    private readonly Func<EquivalencyAssertionOptions<T>, EquivalencyAssertionOptions<T>> _options = options;

    public EquivalentCollectionArgumentMatcher(object expected) : this(expected, x => x.IncludingAllDeclaredProperties()) { }

    public override string ToString() => _defaultArgumentFormatter.Format(expected, false);

    public string DescribeFor(object? argument)
    {
        try
        {
            argument.As<IEnumerable<T>>().Should().BeEquivalentTo((IEnumerable<T>)expected);
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
            argument.As<IEnumerable<T>>().Should().BeEquivalentTo((IEnumerable<T>)expected);
            return true;
        }
        catch
        {
            return false;
        }
    }
}