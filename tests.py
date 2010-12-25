from linqish import Query
import unittest

class TestCase(unittest.TestCase):
    def test_init_source_not_an_iterable(self):
        self.assertRaisesRegexp(
            TypeError, 'None is not an Iterable',
            lambda: Query(None))

    def test_where(self):
        self.assertSequenceEqual(
            [2, 3],
            list(Query([1, 2, 3]).where(lambda x: x > 1)))

    def test_where_with_index(self):
        self.assertSequenceEqual(
            ['b', 'c'],
            list(Query(['a', 'b', 'c']).where(lambda i,x: i > 0)))

    def test_where_not_function(self):
        self.assertRaisesRegexp(
            Exception, 'None is not a Python function',
            lambda: Query([]).where(None))

    def test_where_predicate_has_incorrect_number_of_args(self):
        self.assertRaisesRegexp(
            ValueError, 'value of predicate has wrong number of args',
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

    def test_select_not_function(self):
        self.assertRaisesRegexp(
            Exception, 'None is not a Python function',
            lambda: Query([]).select(None))

    def test_select_selector_has_incorrect_number_of_args(self):
        self.assertRaisesRegexp(
            ValueError, 'value of selector has wrong number of args',
            lambda: Query([]).select((lambda: None)))

    def test_selectmany(self):
        self.assertSequenceEqual(
            [1,2,3,4],
            list(Query([(1,2), (3,4)]).selectmany(lambda x: x)))

    def test_selectmany_with_index(self):
        self.assertSequenceEqual(
            [1, 2, 3, 4, 3, 4],
            list(Query([(1,2), (3,4)]).selectmany(lambda i,x: (i + 1) * x)))

