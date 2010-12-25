from linqish import Query
import unittest

class TestCase(unittest.TestCase):
    def test_select(self):
        self.assertSequenceEqual(
            [1, 2, 3],
            Query([1, 2, 3]).select(lambda x: x))

    def test_select_with_index(self):
        self.assertSequenceEqual(
            [(0,'a'),(1,'b'),(2,'c')],
            list(Query(['a', 'b', 'c']).select(lambda i,x: (i,x))))

    def test_select_selector_raises(self):
        def raiser(x):
            raise Exception('Test')
        self.assertRaisesRegexp(
            Exception, 'Test',
            lambda: Query([1, 2, 3]).select(raiser))

    def test_where(self):
        self.assertSequenceEqual(
            [2, 3],
            list(Query([1, 2, 3]).where(lambda x: x > 1)))

