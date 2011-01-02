import collections
import inspect
import itertools
import operator

class Query(object):
    def _get_number_of_args(self, func):
        return len(inspect.getargspec(func)[0])

    def _normalize_func(self, func, name='selector'):
        if not inspect.isfunction(func):
            raise TypeError('{!r}, the value of {}, is not a function'.format(func, name))

        number_of_args = self._get_number_of_args(func)
        if number_of_args == 1:
            return lambda i, x: func(x)
        if number_of_args == 2:
            return func
        else:
            raise ValueError('{!r}, the value of {}, has wrong number of args'.format(func, name))

    def __init__(self, source, _sort_keys=()):
        if not isinstance(source, collections.Iterable):
            raise TypeError('{!r}, value of source, is not an Iterable'.format(source))
        self._source = iter(source)
        self._sort_keys = _sort_keys

    def __iter__(self):
        result = self._source
        if self._sort_keys:
            result = iter(sorted(self._source, key=lambda x: list(map(lambda y: y(x), self._sort_keys))))
        return result

    def where(self, predicate):
        predicate = self._normalize_func(predicate, 'predicate')
        first, second = itertools.tee(self._source)
        return Query(itertools.compress(first, itertools.starmap(predicate, enumerate(second))))

    def select(self, selector):
        selector = self._normalize_func(selector)
        return Query(itertools.starmap(selector, enumerate(self._source)))

    def selectmany(self, selector, resultSelector=lambda i, x: x):
        selector = self._normalize_func(selector)
        if not inspect.isfunction(resultSelector):
            raise TypeError('{!r}, the value of resultSelector, is not a function'.format(resultSelector))
        if self._get_number_of_args(resultSelector) != 2:
            raise ValueError('{!r}, the value of resultSelector, has wrong number of args'.format(resultSelector))
        return Query(itertools.chain.from_iterable(itertools.imap(
            lambda x: itertools.imap(lambda y: resultSelector(x[1], y), selector(x[0], x[1])),
            enumerate(self._source))))

    def take(self, count):
        if count < 0:
            return []
        return Query(itertools.islice(self._source, count))

    def skip(self, count):
        if count < 0:
            return self._source
        return Query(itertools.islice(self._source, count, None))

    def takewhile(self, predicate):
        predicate = self._normalize_func(predicate, 'predicate')
        return Query(itertools.imap(
            operator.itemgetter(1),
            itertools.takewhile(lambda x: predicate(x[0],x[1]), enumerate(self._source))))

    def skipwhile(self, predicate):
        predicate = self._normalize_func(predicate, 'predicate')
        return Query(itertools.imap(
            operator.itemgetter(1),
            itertools.dropwhile(lambda x: predicate(x[0],x[1]), enumerate(self._source))))

    def join(self, other, keySelector, otherKeySelector, resultSelector):
        return Query(self._join(other, keySelector, otherKeySelector, resultSelector))

    def _join(self, other, keySelector, otherKeySelector, resultSelector):
        otherKeys = dict()
        for item in other:
            key = otherKeySelector(item)
            if key is not None:
                otherKeys.setdefault(otherKeySelector(item), []).append(item)

        for item in self._source:
            key = keySelector(item)
            if key is not None:
                others = otherKeys.get(key, [])
                for other in others:
                    yield resultSelector(item, other)

    def groupjoin(self, other, keySelector, otherKeySelector, resultSelector):
        return Query(self._groupjoin(other, keySelector, otherKeySelector, resultSelector))

    def _groupjoin(self, other, keySelector, otherKeySelector, resultSelector):
        otherKeys = dict()
        for item in other:
            key = otherKeySelector(item)
            if key is not None:
                otherKeys.setdefault(key, []).append(item)

        for item in self._source:
            key = keySelector(item)
            others = otherKeys.get(key, [])
            yield resultSelector(item, others)

    def concat(self, other):
        return Query(itertools.chain(self._source, other))

    def orderby(self, keySelector):
        return OrderedQuery(self, _sort_keys=(keySelector,))

    def orderbydesc(self, keySelector):
        return self.orderby(lambda x: ReverseKey(keySelector(x)))

    def reverse(self):
        return Query(reversed(list(self._source)))

class OrderedQuery(Query):
    def thenby(self, keySelector):
        return OrderedQuery(self, _sort_keys=(self._sort_keys + (keySelector,)))

    def thenbydesc(self, keySelector):
        return self.thenby(lambda x: ReverseKey(keySelector(x)))

class ReverseKey(object):
    def __init__(self, key):
        self._key = key
       
    def __cmp__(self, other):
        if self._key < other._key:
            return 1
        elif self._key > other._key:
            return -1
        else:
            return 0

