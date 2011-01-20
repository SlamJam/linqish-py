from linqish import Query
import collections
import itertools
import unittest
import sys

def _pair(first, second):
    return first, second

def _mod2(x):
    return x % 2

def _consume(query):
    try:
        iter_ = iter(query)
        while True:
            next(iter_)
    except StopIteration:
        pass

def _raise_test_exception(*args,**kwargs):
    raise Exception('test')

class _RaisingIter(object):
    def __iter__(self):
        raise unittest.TestCase.failureException()

class _RaisingOnSecondIter(object):
    def __iter__(self):
        yield 1
        raise unittest.TestCase.failureException()

class TestCase(unittest.TestCase):
    def assertIterEqual(self, iter1, iter2):
        self.assertSequenceEqual(list(iter1), list(iter2))

class Init(TestCase):
    def test_init_source_not_an_iterable(self):
        self.assertRaisesRegexp(
            TypeError,
            'None, value of source, must be iterable but not an iterator or a callable returning an iterator\.',
            lambda: Query(None))

class Restriction(TestCase):
    def test_where_predicate_is_noncallable(self):
        self.assertRaisesRegexp(
            TypeError, "'foo', the value of predicate, is not callable\.",
            lambda: Query([]).where('foo'))

    def test_where(self):
        self.assertIterEqual('BC', Query('aBC').where(str.isupper))

    def test_where_with_index(self):
        self.assertIterEqual('bc', Query('abc').where(lambda index, item: index > 0, with_index=True))

    def test_where_works_with_builtin_callables(self):
        self.assertIterEqual([1,2], Query([0,1,2]).where(abs))

    def test_where_execution_is_deferred(self):
        #No exception
        Query(_RaisingIter()).where(abs)

    def test_where_items_are_streamed(self):
        #No exception
        iter_ = iter(Query(_RaisingOnSecondIter).where(lambda x: True))
        next(iter_)

    def test_where_propagates_exceptions_raised_by_predicate(self):
        self.assertRaisesRegexp(
            Exception, 'test',
            lambda: list(Query(['abc']).where(_raise_test_exception)))

class Projection(TestCase):
    def test_select_selector_is_noncallable(self):
        self.assertRaisesRegexp(
            TypeError, "'foo', the value of selector, is not callable\.",
            lambda: Query([]).select('foo'))

    def test_select(self):
        self.assertIterEqual('ABC', list(Query('abc').select(str.upper)))

    def test_select_with_index(self):
        self.assertIterEqual(
            [(0,'a'),(1,'b'),(2,'c')],
            Query('abc').select(lambda i,x: (i,x), with_index=True))

    def test_select_works_with_builtin_callables(self):
        self.assertIterEqual([1,0,1], Query([-1,0,1]).select(abs))

    def test_select_execution_is_deferred(self):
        #No exception
        Query(_RaisingIter()).select(abs)

    def test_select_items_are_streamed(self):
        #No exception
        iter_ = iter(Query(_RaisingOnSecondIter).select(lambda x: True))
        next(iter_)

    def test_select_propagates_exceptions_raised_by_selector(self):
        self.assertRaisesRegexp(
            Exception, 'test',
            lambda: list(Query(['abc']).select(_raise_test_exception)))

    def test_selectmany_selector_is_noncallable(self):
        self.assertRaisesRegexp(
            TypeError, "'foo', the value of selector, is not callable\.",
            lambda: Query([]).selectmany('foo'))

    def test_selectmany_resultselector_is_noncallable(self):
        self.assertRaisesRegexp(
            TypeError, "'foo', the value of resultSelector, is not callable\.",
            lambda: Query([]).selectmany(lambda x: x, 'foo'))

    def test_selectmany(self):
        self.assertIterEqual([1,2,3,4], Query([(1,2), (3,4)]).selectmany(lambda x: x))

    def test_selectmany_with_index(self):
        self.assertIterEqual(
            [1, 2, 3, 4, 3, 4],
            Query([(1,2), (3,4)]).selectmany(lambda i,x: (i + 1) * x, with_index=True))

    def test_selectmany_with_result_selector(self):
        self.assertIterEqual(
            [((1,2), 1), ((1,2), 2), ((3,4), 3), ((3,4), 4)],
            Query([(1,2), (3,4)]).selectmany(lambda x: x, lambda inner, outer: (inner, outer)))

class Partitioning(TestCase):
    def test_take(self):
        self.assertIterEqual([1,2], Query([1,2,3]).take(2))

    def test_take_count_negative(self):
        self.assertIterEqual([], Query([1,2,3]).take(-1))

    def test_take_count_larger_than_length(self):
        self.assertIterEqual([1,2,3], Query([1,2,3]).take(10))

    def test_skip(self):
        self.assertIterEqual([2, 3], Query([1,2,3]).skip(1))

    def test_skip_count_negative(self):
        self.assertIterEqual([1,2,3], Query([1,2,3]).skip(-1))

    def test_skip_count_larger_than_length(self):
        self.assertIterEqual([], Query([1,2,3]).skip(10))

    def test_takewhile(self):
        self.assertIterEqual([1,2], Query([1,2,3]).takewhile(lambda x: x < 3))

    def test_takewhile_with_index(self):
        self.assertIterEqual([1,2], Query([1,2,3]).takewhile(lambda i,x: i == 0 or x == 2, with_index=True))

    def test_skipwhile(self):
        self.assertIterEqual([3], Query([1,2,3]).skipwhile(lambda x: x < 3))

    def test_skipwhile_with_index(self):
        self.assertIterEqual([3], Query([1,2,3]).skipwhile(lambda i,x: i == 0 or x == 2, with_index=True))

class Join(TestCase):
    def test_join(self):
        self.assertIterEqual(
            [(1,1),(2,2),(3,3)],
            Query([1,2,3]).join([1,2,3],lambda x: x, lambda y: y, _pair))

    def test_join_preserves_order(self):
       self.assertIterEqual(
            [(1,1),(1,3),(2,2),(3,1),(3,3)],
            Query([1,2,3]).join([1,2,3], _mod2, _mod2, _pair))

    def test_join_nones_are_discarded(self):
        def is_even_or_none(x):
            return (x + 1) % 2 or None
        self.assertIterEqual(
            [(2,2)],
            Query([1,2,3]).join([1,2,3], is_even_or_none, is_even_or_none, _pair))

    def test_groupjoin(self):
        self.assertIterEqual(
            [(1,[1]), (2,[2,2]), (3,[])],
            Query([1,2,3]).groupjoin([1,2,2], lambda x:x, lambda y:y, _pair))

    def test_groupjoin_keeps_self_key_nones(self):
        def self_key(x):
            if x == 1:
                return None
            return x
        self.assertIterEqual(
            [(1, []), (2,[2]), (3,[3])],
            Query([1,2,3]).groupjoin([1,2,3], self_key, lambda y:y, _pair))

    def test_groupjoin_drops_other_key_nones(self):
        def key(x):
            if x == 1:
                return None
            return x
        self.assertIterEqual(
            [(1,[]), (2,[2]), (3,[3])],
            Query([1,2,3]).groupjoin([1,2,3], key, key, _pair))

    def test_groupjoin_preserves_order(self):
        self.assertIterEqual(
            [(1,[1,3]), (2,[2]), (3,[1,3])],
            Query([1,2,3]).groupjoin([1,2,3], _mod2, _mod2, _pair))

class Concatenation(TestCase):
    def test_concat_other_is_noniterable(self):
        self.assertRaisesRegexp(
            TypeError, "0, the value of other, is not iterable\.",
            lambda: Query([]).concat(0))

    def test_concat(self):
        self.assertIterEqual([1, 2, 3], Query([1]).concat([2,3]))

class Ordering(TestCase):
    def test_orderby(self):
        self.assertIterEqual([0, -1, 1], Query([-1, 0, 1]).orderby(lambda x: x**2))

    def test_orderby_with_none_items(self):
        self.assertIterEqual([None,1,2,3], Query([3,2,1,None]).orderby(lambda x: x))

    def test_thenby_after_orderby(self):
        self.assertIterEqual(
            [(1,1),(1,2),(2,1)],
            Query([(2,1),(1,2),(1,1)]).orderby(lambda x: x[0]).thenby(lambda x: x[1]))

    def test_orderbydesc_with_ints(self):
        self.assertIterEqual([-1, 1, 0], Query([-1, 0, 1]).orderbydesc(lambda x: x**2))

    def test_orderbydesc_with_strings(self):
        self.assertIterEqual(['z', 'y', 'x'], Query(['x', 'y', 'z']).orderbydesc(lambda x: x))

    def test_thenbydesc_after_orderby(self):
        self.assertIterEqual(
            [(1,2),(1,1),(2,1)],
            Query([(2,1),(1,2),(1,1)]).orderby(lambda x: x[0]).thenbydesc(lambda x: x[1]))

    def test_reverse(self):
        self.assertIterEqual([3,2,1], Query([1,2,3]).reverse())

class Grouping(TestCase):
    def test_groupby(self):
        self.assertIterEqual(
            [(3, ['ONE', 'TWO']), (5, ['THREE']), (4, ['FOUR', 'FIVE'])],
            Query(['one', 'two', 'three', 'four', 'five']).groupby(len, str.upper, lambda k,e: (k, list(e))))

class SetOperators(TestCase):
    def test_distinct(self):
        self.assertIterEqual([-1,0,2], Query([-1,0,1,2]).distinct(abs))

    def test_union(self):
        self.assertIterEqual([-3,-1,-2,0], Query([-3,-1,1,3]).union([-2,0,2],abs))

    def test_intersection(self):
        self.assertIterEqual([2, 1, 0], Query([-3,-2,-2,-1,0]).intersection([0,1,2,2,4], abs))

    def test_except(self):
        self.assertIterEqual([-2, 0], Query([-2,-2,-1,0]).except_([1,1], abs))

class Conversion(TestCase):
    def test_tolist(self):
        self.assertEqual([1,2,3], Query([1,2,3]).tolist())

    def test_todict(self):
        self.assertEqual({1:'A',2:'AB',3:'ABC'}, Query(['a','ab','abc']).todict(len,str.upper))

    def test_todict_when_keySelector_produces_duplicate(self):
        self.assertRaisesRegexp(
            TypeError, 'keySelector produced duplicate key\.',
            lambda: Query([1,1]).todict(lambda x:x))

    def test_tolookup_getitem(self):
        result = Query([-2,-1,0,1,2]).tolookup(abs, lambda x: 2*x)
        self.assertIterEqual([-4,4], result[2])
        self.assertIterEqual([-2,2], result[1])
        self.assertIterEqual([0], result[0])

    def test_tolookup_iter(self):
        result = iter(Query([-2,-1,0,1,2]).tolookup(abs, lambda x: 2*x))
        grouping = next(result)
        self.assertEqual(2, grouping.key)
        grouping = next(result)
        self.assertEqual(1, grouping.key)
        grouping = next(result)
        self.assertEqual(0, grouping.key)

class Equality(TestCase):
    def test_iter_equal_other_not_iterable(self):
        self.assertRaisesRegexp(
            TypeError, '0, the value of other, is not iterable\.',
            lambda: Query([]).iter_equal(0))

    def test_iter_equal_sequences_equal(self):
        self.assertTrue(Query([1,2]).iter_equal([1,2]))

    def test_iter_equal_sequences_not_equal(self):
        self.assertFalse(Query([1,2]).iter_equal([1,3]))

    def test_iter_equal_with_empty_sequences(self):
        self.assertTrue(Query([]).iter_equal([]))

    def test_iter_equal_with_key(self):
        key = lambda x: True
        self.assertTrue(Query([1,2]).iter_equal([3,4], key))

    def test_iter_equal_with_self_shorter(self):
        key = lambda x: True
        self.assertFalse(Query([]).iter_equal([1], key))

class ElementOperators(TestCase):
    def test_iter_equal_with_other_shorter(self):
        key = lambda x: True
        self.assertFalse(Query([1]).iter_equal([], key))

    def test_first(self):
        self.assertEqual('a', Query('abc').first())

    def test_first_empty(self):
        self.assertRaises(LookupError, lambda: Query('').first())

    def test_first_with_predicate(self):
        self.assertEqual('d', Query('abcd').first(lambda x: x > 'c'))

    def test_first_with_default(self):
        self.assertEqual('?', Query('').first(default='?'))

    def test_last(self):
        self.assertEqual('c', Query('abc').last())

    def test_last_empty(self):
        self.assertRaises(LookupError, lambda: Query('').last())

    def test_last_with_predicate(self):
        self.assertEqual('a', Query('abdc').last(lambda x: x < 'b'))

    def test_last_with_default(self):
        self.assertEqual('?', Query('').last(default='?'))

    def test_single(self):
        self.assertEqual('a', Query('a').single())

    def test_single_not_found(self):
        self.assertRaisesRegexp(
            LookupError, 'No items found.',
            lambda: Query('').single())

    def test_single_too_many_found(self):
        self.assertRaisesRegexp(
            LookupError, 'More than one item found\.',
            lambda: Query('ab').single())

    def test_single_with_predicate(self):
        self.assertEqual('b', Query('abc').single(lambda x: 'a' < x <'c'))

    def test_single_with_default(self):
        self.assertEqual('?', Query('').single(default='?'))

    def test_at(self):
        self.assertEqual('b', Query('abc').at(1))

    def test_at_index_not_int(self):
        self.assertRaisesRegexp(
            TypeError, "'foo', the value of index, is not an int\.",
            lambda: Query('').at('foo'))

    def test_at_negative_index_and_no_default(self):
        self.assertRaisesRegexp(
            ValueError, '-1, the value of index, is negative\.',
            lambda: Query('').at(-1))

    def test_at_negative_index_and_default(self):
        self.assertEqual('?', Query('').at(-1,'?'))

    def test_at_index_greater_than_number_of_elements_and_no_default(self):
        self.assertRaisesRegexp(
            ValueError, '10, the value of index, is greater than the number of elements\.',
            lambda: Query('').at(10))

    def test_at_index_greater_than_number_of_elements_and_default(self):
        self.assertEqual('?', Query('').at(10,'?'))

    def test_at_is_optimized_for_sized_sources(self):
        class SizedSource(collections.Sized):
            def __len__(self):
                return 0
            def __iter__(self):
                raise unittest.TestCase.failureException()

        self.assertRaises(ValueError, lambda: Query(SizedSource()).at(10))

    def test_at_is_optimized_for_sequence_sources(self):
        class SequenceSource(collections.Sequence):
            def __len__(self):
                return 5
            def __getitem__(self, item):
                return 'x'
            def __iter__(self):
                raise unittest.TestCase.failureException()

        self.assertEqual('x', Query(SequenceSource()).at(3))

    def test_ifempty_source_not_empty(self):
        self.assertIterEqual([1,2,3], Query([1,2,3]).ifempty(None))

    def test_ifempty_source_empty(self):
        default = object()
        self.assertIterEqual([default], Query([]).ifempty(default))

    def test_ifempty_uses_deferred_execution(self):
        source = []
        result = Query(source).ifempty(None)
        source.extend([1,2,3])
        self.assertIterEqual([1,2,3], result)

class Generation(TestCase):
    def test_empty(self):
        self.assertIterEqual([], Query.empty())

    def test_empty_value_is_cached(self):
        self.assertTrue(Query.empty() is Query.empty())

    def test_range(self):
        self.assertIterEqual([1,2,3], Query.range(1,3))

    def test_range_starts_at_start(self):
        start = 1000
        self.assertEqual(start, list(Query.range(start, 10))[0])

    def test_range_has_count_items(self):
        count = 10
        self.assertEqual(count, len(list(Query.range(0, count))))

    def test_range_returns_query(self):
        self.assertIsInstance(Query.range(0, 10), Query)

    def test_range_negative_count_raises_error(self):
        self.assertRaisesRegexp(ValueError,
            '-1, the value of count, is negative\.',
            lambda: Query.range(0, -1))

    def test_range_no_overflow_at_limit(self):
        #No exception
        _consume(Query.range(sys.maxint - 1, 1))

    def test_range_overflow(self):
        self.assertRaisesRegexp(ValueError,
            '[0-9]+ and 1, the values of start and count respectively, result in overflow\.',
            lambda: Query.range(sys.maxint, 1))

    def test_repeat(self):
        self.assertIterEqual(['x','x','x'], Query([]).repeat('x', 3))

    def test_repeat_with_zero_count(self):
        self.assertIterEqual([], Query.repeat('x', 0))

    def test_repeat_with_negative_count(self):
        self.assertRaisesRegexp(
            ValueError, '-1, the value of count, is negative\.',
            lambda: Query.repeat('x', -1))

class Quantifiers(TestCase):
    def test_any_predicate_is_noncallable(self):
        self.assertRaisesRegexp(
            TypeError, "'foo', the value of predicate, is not callable\.",
            lambda: Query([]).any('foo'))

    def test_any_empty_source(self):
        self.assertFalse(Query([]).any(lambda x: True))

    def test_any_empty_source_and_default_predicate(self):
        self.assertFalse(Query([]).any())

    def test_any_nonempty_source_and_default_predicate(self):
        self.assertTrue(Query([1,2,3]).any())

    def test_any_source_with_only_falsy_items_and_default_predicate(self):
        self.assertTrue(Query([False,None,0,[]]).any())

    def test_any_nonempty_source_and_predicate_always_false(self):
        self.assertFalse(Query([1,2,3]).any(lambda x: False))

    def test_any_nonempty_source_and_predicate_true_once(self):
        self.assertTrue(Query([1,2,3]).any(lambda x: x == 3))

    def test_any_only_iterates_until_predicate_is_true(self):
        def predicate(x):
            if x > 3:
                raise unittest.TestCase.failureException()
            return x == 3
        self.assertTrue(Query([1,2,3,4]).any(predicate))

    def test_all_predicate_is_noncallable(self):
        self.assertRaisesRegexp(
            TypeError, "'foo', the value of predicate, is not callable\.",
            lambda: Query([]).all('foo'))

    def test_all_empty_source(self):
        self.assertTrue(Query([]).all(lambda x: False))

    def test_all_nonempty_source_and_predicate_always_true(self):
        self.assertTrue(Query([1,2,3]).all(lambda x: True))

    def test_all_nonempty_source_and_predicate_false_once(self):
        self.assertFalse(Query([1,2,3]).all(lambda x: x != 3))

    def test_all_returns_when_nonmatch_found(self):
        #No exception
        Query(_RaisingOnSecondIter()).all(lambda x: False)

    def test_all_only_iterates_until_predicate_is_false(self):
        def predicate(x):
            if x > 3:
                raise unittest.TestCase.failureException()
            return x != 3
        self.assertFalse(Query([1,2,3,4]).all(predicate))

    def test_contains_match(self):
        self.assertTrue(Query([1,2,3]).contains(1))

    def test_contains_no_match(self):
        self.assertFalse(Query([1,2,3]).contains(5))

    def test_contains_with_key_and_match(self):
        self.assertTrue(Query([1,2,3]).contains(9, lambda x: x**2))

    def test_contains_with_key_and_no_match(self):
        self.assertFalse(Query([1,2,3]).contains(25, lambda x: x**2))

    def test_contains_returns_when_match_is_found(self):
        def selector(x):
            if x > 3:
                raise unittest.TestCase.failureException()
            return x
        #No exception
        Query([1,2,3,4]).select(selector).contains(3)

    def test_contains_uses_contains_method_if_able(self):
        class Container(object):
            def __int__(self):
                self.contains_called = False
            def __contains__(self, item):
                self.contains_called = True
                return True
            def __iter__(self):
                raise unittest.TestCase.failureException
        container = Container()
        Query(container).contains(1)

        self.assertTrue(container.contains_called)

class Aggregation(TestCase):
    def test_count_predicate_is_neither_None_or_noncallable(self):
        self.assertRaisesRegexp(
            TypeError, "'foo', the value of predicate, is neither None nor callable\.",
            lambda: Query([1,2,3]).count('foo'))

    def test_count_sized_with_no_predicate(self):
        self.assertEqual(3, Query([1,2,3]).count())

    def test_count_nonsized_with_no_predicate(self):
        self.assertEqual(3, Query(lambda: itertools.repeat('x', 3)).count())

    def test_count_with_predicate(self):
        self.assertEqual(2, Query([1,2,3]).count(lambda x: x < 3))

    def test_count_of_iterable_containing_nones(self):
        self.assertEqual(3, Query([1,2,3]).select(lambda x: None).count())

    def test_count_uses_len_if_able_to(self):
        class Sized(object):
            def __len__(self):
                return 10
            def __iter__(self):
                raise unittest.TestCase.failureException
        sized = Sized()
        self.assertEqual(10, Query(sized).count())

    def test_sum(self):
        self.assertEqual(6, Query([1,2,3]).sum())

    def test_sum_ignores_nones(self):
        self.assertEqual(6, Query([1,2,3,None]).sum())

    def test_sum_empty_source(self):
        self.assertEqual(0, Query([]).sum())

    def test_sum_source_of_nones(self):
        self.assertEqual(0, Query(3 * [None]).sum())

    def test_sum_with_selector(self):
        self.assertEqual(14, Query([1,2,3]).sum(lambda x: x**2))

    def test_min(self):
        self.assertEqual(1, Query([1,2,3]).min())

    def test_min_empty_source(self):
        self.assertRaises(ValueError, lambda: Query([]).min())

    def test_min_contains_none(self):
        self.assertEqual(None, Query([-1,0,1,2,3,None]).min())

    def test_min_with_selector(self):
        self.assertEqual(-3, Query([1,2,3]).min(lambda x: -x))

    def test_max(self):
        self.assertEqual(3, Query([1,2,3]).max())

    def test_max_empty_source(self):
        self.assertRaises(ValueError, lambda: Query([]).max())

    def test_max_contains_none(self):
        self.assertEqual(3, Query([-1,0,1,2,3,None]).max())

    def test_max_with_selector(self):
        self.assertEqual(-1, Query([1,2,3]).max(lambda x: -x))

    def test_average(self):
        self.assertEqual(1.5, Query([1.0,2.0]).average())

    def test_average_with_ints_does_not_floor_result(self):
        self.assertEqual(1.5, Query([1,2]).average())

    def test_average_empty_source(self):
        self.assertEqual(None, Query([]).average())

    def test_average_ignores_nones(self):
        self.assertEqual(1.5, Query([1,2,None]).average())

    def test_aggregate(self):
        self.assertEqual(6, Query([1,2,3]).aggregate(0, lambda x,y: x + y))

    def test_aggregate_starts_with_seed(self):
        self.assertEqual(10, Query([0,0,0]).aggregate(10, lambda x,y: x + y))

    def test_aggregate_empty_source_returns_seed(self):
        self.assertEqual(10, Query([]).aggregate(10, lambda x,y: x+y))

    def test_aggregate_with_result_selector(self):
        self.assertEqual(0, Query([-1,0,1]).aggregate(0, lambda x,y: x+y, abs))

