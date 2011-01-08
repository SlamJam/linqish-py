import collections
import inspect
import itertools
import operator

class _ReverseKey(object):
    def __init__(self, key):
        self._key = key

    def __cmp__(self, other):
        if self._key < other._key:
            return 1
        elif self._key > other._key:
            return -1
        else:
            return 0

class _Grouping(object):
    def __init__(self, key, elements):
        self._key = key
        self._elements = elements

    @property
    def key(self):
        return self._key

    def __iter__(self):
        return iter(self._elements)

class Lookup(object):
    def __init__(self):
        self._map = collections.defaultdict(list)
        self._keys = []

    def _add(self, key, element):
        elements = self._map[key]
        if not elements:
            self._keys.append(key)
        elements.append(element)

    def __len__(self):
        return len(self._map)

    def __getitem__(self, item):
        return iter(self._map[item])

    def __contains__(self, item):
        return item in self._map

    def __iter__(self):
        return itertools.imap(lambda x: _Grouping(x, self._map[x]), self._keys)

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
        return self.orderby(lambda x: _ReverseKey(keySelector(x)))

    def reverse(self):
        return Query(reversed(list(self._source)))

    def groupby(self, keySelector, elementSelector=lambda x: x, resultSelector=_Grouping):
        lookup = Lookup()
        for item in self._source:
            lookup._add(keySelector(item), elementSelector(item))
        return Query(itertools.imap(lambda x: resultSelector(x.key, x._elements), lookup))

    def distinct(self, key=lambda x: x):
        return Query(self._distinct(key))

    def _distinct(self, key):
        keys = set()
        for item in self._source:
            item_key = key(item)
            if item_key not in keys:
                keys.add(item_key)
                yield item

    def union(self, other, key=lambda x: x):
        return Query(self._union(other, key))

    def _union(self, other, key):
        keys = set()
        for item in self._source:
            item_key = key(item)
            if item_key not in keys:
                keys.add(item_key)
                yield item

        for item in other:
            item_key = key(item)
            if item_key not in keys:
                keys.add(item_key)
                yield item

    def intersection(self, other, key=lambda x: x):
        return Query(self._intersection(other, key))

    def _intersection(self, other, key):
        other_dict = dict()
        for item in other:
            other_dict.setdefault(key(item), item)
        for item in self._source:
            try:
                yield other_dict.pop(key(item))
            except KeyError:
                pass

    def except_(self, other, key=lambda x: x):
        return Query(self._except_(other, key))

    def _except_(self, other, key):
        yielded_keys = set()
        for item in other:
            yielded_keys.add(key(item))

        for item in self._source:
            item_key = key(item)
            if item_key not in yielded_keys:
                yield item
                yielded_keys.add(item_key)

    def tolist(self):
        return list(self._source)

class OrderedQuery(Query):
    def thenby(self, keySelector):
        return OrderedQuery(self, _sort_keys=(self._sort_keys + (keySelector,)))

    def thenbydesc(self, keySelector):
        return self.thenby(lambda x: _ReverseKey(keySelector(x)))


