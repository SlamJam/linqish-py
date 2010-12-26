from linqish import Query
import unittest

def _pair(first, second):
    return (first, second)

def _mod2(x):
    return x % 2

class TestCase(unittest.TestCase):
    def test_init_source_not_an_iterable(self):
        self.assertRaisesRegexp(
            TypeError, 'None, value of source, is not an Iterable',
            lambda: Query(None))

    def test_where(self):
        self.assertSequenceEqual(
            [2, 3],
            list(Query([1, 2, 3]).where(lambda x: x > 1)))

    def test_where_with_index(self):
        self.assertSequenceEqual(
            ['b', 'c'],
            list(Query(['a', 'b', 'c']).where(lambda i,x: i > 0)))

    def test_where_predicate_not_function(self):
        self.assertRaisesRegexp(
            TypeError, 'None, the value of predicate, is not a function',
            lambda: Query([]).where(None))

    def test_where_predicate_has_wrong_number_of_args(self):
        self.assertRaisesRegexp(
            ValueError, '<function <lambda> at .*>, the value of predicate, has wrong number of args',
            lambda: Query([]).where((lambda: None)))

    def test_select(self):
        self.assertSequenceEqual(
            [1, 2, 3],
            list(Query([1, 2, 3]).select(lambda x: x)))

    def test_select_with_index(self):
        self.assertSequenceEqual(
            [(0,'a'),(1,'b'),(2,'c')],
            list(Query(['a', 'b', 'c']).select(lambda i,x: (i,x))))

    def test_select_selector_raises(self):
        def raiser(x):
            raise Exception('Test')
        self.assertRaisesRegexp(
            Exception, 'Test',
            lambda: list(Query([1, 2, 3]).select(raiser)))

    def test_select_selector_not_function(self):
        self.assertRaisesRegexp(
            TypeError, 'None, the value of selector, is not a function',
            lambda: Query([]).select(None))

    def test_select_selector_has_wrong_number_of_args(self):
        self.assertRaisesRegexp(
            ValueError, '.*',
            lambda: Query([]).select((lambda: None)))

    def test_selectmany(self):
        self.assertSequenceEqual(
            [1,2,3,4],
            list(Query([(1,2), (3,4)]).selectmany(lambda x: x)))

    def test_selectmany_with_index(self):
        self.assertSequenceEqual(
            [1, 2, 3, 4, 3, 4],
            list(Query([(1,2), (3,4)]).selectmany(lambda i,x: (i + 1) * x)))

    def test_selectmany_selector_not_a_function(self):
        self.assertRaisesRegexp(
            TypeError, 'None, the value of selector, is not a function',
            lambda: Query([]).select(None))

    def test_selectmany_selector_has_wrong_number_of_args(self):
        self.assertRaisesRegexp(
            ValueError, '<function <lambda> at .*>, the value of selector, has wrong number of args',
            lambda: Query([]).selectmany(lambda: None))

    def test_selectmany_with_result_selector(self):
        self.assertSequenceEqual(
            [((1,2), 1), ((1,2), 2), ((3,4), 3), ((3,4), 4)],
            list(Query([(1,2), (3,4)]).selectmany(lambda x: x, lambda inner, outer: (inner, outer))))

    def test_selectmany_result_selector_not_a_function(self):
        self.assertRaisesRegexp(
            TypeError, 'None, the value of resultSelector, is not a function',
            lambda: Query([]).selectmany(lambda x: x, None))

    def test_selectmany_result_selector_has_wrong_number_of_args(self):
        self.assertRaisesRegexp(
            ValueError, '<function <lambda> at .*>, the value of resultSelector, has wrong number of args',
            lambda: Query([]).selectmany(lambda x: x, lambda: None))

    def test_take(self):
        self.assertSequenceEqual([1,2], list(Query([1,2,3]).take(2)))

    def test_take_count_negative(self):
        self.assertSequenceEqual([], list(Query([1,2,3]).take(-1)))

    def test_take_count_larger_than_length(self):
        self.assertSequenceEqual([1,2,3], list(Query([1,2,3]).take(10)))

    def test_skip(self):
        self.assertSequenceEqual([2, 3], list(Query([1,2,3]).skip(1)))

    def test_skip_count_negative(self):
        self.assertSequenceEqual([1,2,3], list(Query([1,2,3]).skip(-1)))

    def test_skip_count_larger_than_length(self):
        self.assertSequenceEqual([], list(Query([1,2,3]).skip(10)))

    def test_takewhile(self):
        self.assertSequenceEqual([1,2], list(Query([1,2,3]).takewhile(lambda x: x < 3)))

    def test_takewhile_with_index(self):
        self.assertSequenceEqual([1,2], list(Query([1,2,3]).takewhile(lambda i,x: i == 0 or x == 2)))

    def test_takewhile_predicate_not_function(self):
        self.assertRaisesRegexp(
            TypeError, 'None, the value of predicate, is not a function',
            lambda: Query([1,2,3]).takewhile(None))

    def test_skipwhile(self):
        self.assertSequenceEqual([3], list(Query([1,2,3]).skipwhile(lambda x: x < 3)))

    def test_skipwhile_with_index(self):
        self.assertSequenceEqual([3], list(Query([1,2,3]).skipwhile(lambda i,x: i == 0 or x == 2)))

    def test_skipwhile_predicate_not_function(self):
        self.assertRaisesRegexp(
            TypeError, 'None, the value of predicate, is not a function',
            lambda: Query([1,2,3]).skipwhile(None))

    def test_join(self):
        self.assertSequenceEqual(
            [(1,1),(2,2),(3,3)],
            list(Query([1,2,3]).join([1,2,3],lambda x: x, lambda y: y, _pair)))

    def test_join_preserves_order(self):
       self.assertSequenceEqual(
            [(1,1),(1,3),(2,2),(3,1),(3,3)],
            list(Query([1,2,3]).join([1,2,3], _mod2, _mod2, _pair)))

    def test_join_nones_are_discarded(self):
        def is_even_or_none(x):
            return (x + 1) % 2 or None
        self.assertSequenceEqual(
            [(2,2)],
            list(Query([1,2,3]).join([1,2,3], is_even_or_none, is_even_or_none, _pair)))

    def test_groupjoin(self):
        self.assertSequenceEqual(
            [(1,[1]), (2,[2,2]), (3,[])],
            list(Query([1,2,3]).groupjoin([1,2,2], lambda x:x, lambda y:y, _pair)))

    def test_groupjoin_keeps_self_key_nones(self):
        def self_key(x):
            if x == 1:
                return None
            return x
        self.assertSequenceEqual(
            [(1, []), (2,[2]), (3,[3])],
            list(Query([1,2,3]).groupjoin([1,2,3], self_key, lambda y:y, _pair)))

    def test_groupjoin_drops_other_key_nones(self):
        def key(x):
            if x == 1:
                return None
            return x
        self.assertSequenceEqual(
            [(1,[]), (2,[2]), (3,[3])],
            list(Query([1,2,3]).groupjoin([1,2,3], key, key, _pair)))

    def test_groupjoin_preserves_order(self):
        self.assertSequenceEqual(
            [(1,[1,3]), (2,[2]), (3,[1,3])],
            list(Query([1,2,3]).groupjoin([1,2,3], _mod2, _mod2, _pair)))
