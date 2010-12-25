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

    def __init__(self, source):
        if not isinstance(source, collections.Iterable):
            raise TypeError('{!r}, value of source, is not an Iterable'.format(source))
        self._source = iter(source)

    def where(self, predicate):
        predicate = self._normalize_func(predicate, 'predicate')
        first, second = itertools.tee(self._source)
        return itertools.compress(first, itertools.starmap(predicate, enumerate(second)))

    def select(self, selector):
        selector = self._normalize_func(selector)
        return itertools.starmap(selector, enumerate(self._source))

    def selectmany(self, selector, resultSelector=lambda i, x: x):
        selector = self._normalize_func(selector)
        if not inspect.isfunction(resultSelector):
            raise TypeError('{!r}, the value of resultSelector, is not a function'.format(resultSelector))
        if self._get_number_of_args(resultSelector) != 2:
            raise ValueError('{!r}, the value of resultSelector, has wrong number of args'.format(resultSelector))
        return itertools.chain.from_iterable(itertools.imap(
            lambda x: itertools.imap(lambda y: resultSelector(x[1], y), selector(x[0], x[1])),
            enumerate(self._source)))

    def take(self, count):
        if count < 0:
            return []
        return itertools.islice(self._source, count)

    def skip(self, count):
        if count < 0:
            return self._source
        return itertools.islice(self._source, count, None)

    def takewhile(self, predicate):
        predicate = self._normalize_func(predicate, 'predicate')
        return itertools.imap(
            operator.itemgetter(1),
            itertools.takewhile(lambda x: predicate(x[0],x[1]), enumerate(self._source)))

    def skipwhile(self, predicate):
        predicate = self._normalize_func(predicate, 'predicate')
        return itertools.imap(
            operator.itemgetter(1),
            itertools.dropwhile(lambda x: predicate(x[0],x[1]), enumerate(self._source)))

    def join(self, other, keySelector, otherKeySelector, resultSelector, comparer=operator.gt):
        otherKeys = dict()
        for item in other:
            otherKeys.setdefault(otherKeySelector(item), []).append(item)

        for item in self._source:
            key = keySelector(item)
            if key is not None:
                others = otherKeys.get(key, [])
                for other in others:
                    yield resultSelector(item, other)

