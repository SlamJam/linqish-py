from linqish import Query
import unittest

class TestCase(unittest.TestCase):
    def test_select(self):
        self.assertItemsEqual(
            [1, 2, 3],
            Query([1, 2, 3]).select(lambda x: x))

    def test_select_with_index(self):
        self.assertItemsEqual(
            [(0,'a'),(1,'b'),(2,'c')],
            Query(['a', 'b', 'c']).select(lambda i,x: (i,x)))

    def test_select_selector_raises(self):
        def raiser(x):
            raise Exception('Test')
        self.assertRaisesRegexp(
            Exception, 'Test',
            lambda: Query([1, 2, 3]).select(raiser))

