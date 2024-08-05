using DiscriminatedOnions;
using FluentAssertions;
using FluentAssertions.Equivalency;
using FluentAssertions.Execution;
using FluentAssertions.Primitives;

namespace Tests.Utils;

public static class FluentAssertionsExtension
{
    public static void BeSome<T>(this ObjectAssertions assertions, T expected) where T : class
    {
        var option = (Option<T>)assertions.Subject;
        option.Match(
            onSome: obj => obj.Should().BeEquivalentTo(expected),
            onNone: () =>
            {
                var failWith = Execute.Assertion
                    .BecauseOf("")
                    .ForCondition(false)
                    .FailWith("Expected {context:object} to be Some but found None.");
                return new AndConstraint<ObjectAssertions>(new ObjectAssertions(failWith));
            });
    }

    public static void BeSome<T>(this ObjectAssertions assertions, T expected, Func<EquivalencyAssertionOptions<T>, EquivalencyAssertionOptions<T>> options) where T : class
    {
        var option = (Option<T>)assertions.Subject;
        option.Match(
            onSome: obj => obj.Should().BeEquivalentTo(expected, options),
            onNone: () =>
            {
                var failWith = Execute.Assertion
                    .BecauseOf("")
                    .ForCondition(false)
                    .FailWith("Expected {context:object} to be Some but found None.");
                return new AndConstraint<ObjectAssertions>(new ObjectAssertions(failWith));
            });
    }

    public static void BeNone<T>(this ObjectAssertions assertions) where T : class
    {
        var option = (Option<T>)assertions.Subject;
        option.Map(obj =>
            Execute.Assertion
                .BecauseOf("")
                .ForCondition(false)
                .FailWith("Expected {context:object} to be None but found Some."));
    }
}