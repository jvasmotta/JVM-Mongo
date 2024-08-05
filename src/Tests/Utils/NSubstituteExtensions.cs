using FluentAssertions.Equivalency;
using NSubstitute.Core;
using NSubstitute.Core.Arguments;

namespace Tests.Utils;

public static class NSubstituteExtensions
{
    public static T? Equivalent<T>(this T obj)
    {
        SubstitutionContext.Current.ThreadContext.EnqueueArgumentSpecification(
            new ArgumentSpecification(typeof(T),
                new EquivalentArgumentMatcher<T>(obj)));
        return default;
    }

    public static T? Equivalent<T>(this T obj, Func<EquivalencyAssertionOptions<T>, EquivalencyAssertionOptions<T>> options)
    {
        SubstitutionContext.Current.ThreadContext.EnqueueArgumentSpecification(
            new ArgumentSpecification(typeof(T),
                new EquivalentArgumentMatcher<T>(obj, options)));
        return default;
    }

    public static IEnumerable<T>? EquivalentAll<T>(this IEnumerable<T> obj)
    {
        SubstitutionContext.Current.ThreadContext.EnqueueArgumentSpecification(
            new ArgumentSpecification(typeof(IEnumerable<T>),
                new EquivalentCollectionArgumentMatcher<T>(obj)));
        return default;
    }

    public static IReadOnlyCollection<T>? EquivalentAll<T>(this IReadOnlyCollection<T> obj)
    {
        SubstitutionContext.Current.ThreadContext.EnqueueArgumentSpecification(
            new ArgumentSpecification(typeof(IReadOnlyCollection<T>),
                new EquivalentCollectionArgumentMatcher<T>(obj)));
        return default;
    }

    public static IEnumerable<T>? EquivalentAll<T>(this IEnumerable<T> obj, Func<EquivalencyAssertionOptions<T>, EquivalencyAssertionOptions<T>> options)
    {
        SubstitutionContext.Current.ThreadContext.EnqueueArgumentSpecification(
            new ArgumentSpecification(typeof(IEnumerable<T>),
                new EquivalentCollectionArgumentMatcher<T>(obj, options)));
        return default;
    }
}