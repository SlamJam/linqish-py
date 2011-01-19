import collections
import functools
import inspect
import itertools
import operator

# used to indicate missing values
_missing = object()
_Tally = collections.namedtuple('Tally', ['sum', 'count'])

@functools.total_ordering
class _ReverseKey(object):
    def __init__(self, key):
        self._key = key

    def __eq__(self, other):
        return self._key == other._key

    def __lt__(self, other):
        return self._key > other._key

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

    @staticmethod
    def range(start, count):
        #The limit of this function is start+count<=sys.maxint.
        #This is different from .net which allows start+count-1<=sys.maxint.
        if count < 0:
            raise ValueError('{!r}, the value of count, is negative.'.format(count))
        try:
            return Query(xrange(start, start + count))
        except OverflowError:
            raise ValueError(('{!r} and {!r}, the values of start and count respectively, ' +
                              'result in overflow.').format(start, count))
    
    @staticmethod
    def repeat(element, count):
        if count < 0:
            raise ValueError('{!r}, the value of count, is negative.'.format(count))
        return Query(lambda: itertools.repeat(element, count))

    @staticmethod
    def empty():
        return _empty

    def __init__(self, source, _sort_keys=()):
        if not (self._is_iterable_but_not_iterator(source) or callable(source)):
            raise TypeError(('{!r}, value of source, must be iterable but not an iterator or a callable returning ' +
                             'an iterator.').format(source))
        self._source = source
        self._sort_keys = _sort_keys

    def __iter__(self):
        result = self._itersource()
        if self._sort_keys:
            #TODO: is it necessary to call iter(...)?
            result = iter(sorted(result, key=lambda x: list(map(lambda y: y(x), self._sort_keys))))
        return result

    def _is_iterable_but_not_iterator(self, instance):
        return isinstance(instance, collections.Iterable) and not isinstance(instance, collections.Iterator)

    def _itersource(self):
        return callable(self._source) and self._source() or iter(self._source)

    def where(self, predicate, with_index=False):
        if not callable(predicate):
            raise TypeError('{!r}, the value of predicate, is not callable.'.format(predicate))

        if not with_index:
            return Query(lambda: itertools.ifilter(predicate, self._itersource()))
        else:
            first, second = itertools.tee(self._source)
            return Query(lambda: itertools.compress(first, itertools.starmap(predicate, enumerate(second))))

    def select(self, selector, with_index=False):
        if not callable(selector):
            raise TypeError('{!r}, the value of selector, is not callable.'.format(selector))

        return Query(lambda: self._select(self._itersource(), selector, with_index))

    def _select(self, iter_, selector, with_index):
        if not with_index:
            return itertools.imap(selector, iter_)
        else:
            return itertools.starmap(selector, enumerate(iter_))

    def selectmany(self, selector, resultSelector=lambda i, x: x, with_index=False):
        def apply_result_selector(item, collection):
            return itertools.imap(functools.partial(resultSelector, item), collection)

        first, second = itertools.tee(self._itersource())
        return Query(lambda: itertools.chain.from_iterable(itertools.starmap(
            apply_result_selector,
            itertools.izip(first, self._select(second, selector, with_index)))))

    def take(self, count):
        if count < 0:
            return []
        return Query(lambda: itertools.islice(self._source, count))

    def skip(self, count):
        if count < 0:
            return self
        return Query(lambda: itertools.islice(self._source, count, None))

    def takewhile(self, predicate, with_index=False):
        if not with_index:
            return Query(lambda: itertools.takewhile(predicate, self._itersource()))
        else:
            return Query(lambda: itertools.imap(
                operator.itemgetter(1),
                itertools.takewhile(lambda x: predicate(*x), enumerate(self._source))))

    def skipwhile(self, predicate, with_index=False):
        if not with_index:
            return Query(lambda: itertools.dropwhile(predicate, self._itersource()))
        else:
            return Query(lambda: itertools.imap(
                operator.itemgetter(1),
                itertools.dropwhile(lambda x: predicate(*x), enumerate(self._source))))

    def join(self, other, keySelector, otherKeySelector, resultSelector):
        return Query(functools.partial(self._join, other, keySelector, otherKeySelector, resultSelector))

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
        return Query(functools.partial(self._groupjoin, other, keySelector, otherKeySelector, resultSelector))

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
        return Query(lambda: itertools.chain(self._source, other))

    def orderby(self, keySelector):
        return OrderedQuery(self, _sort_keys=(keySelector,))

    def orderbydesc(self, keySelector):
        return self.orderby(lambda x: _ReverseKey(keySelector(x)))

    def reverse(self):
        return Query(lambda: reversed(list(self._source)))

    def groupby(self, keySelector, elementSelector=lambda x: x, resultSelector=_Grouping):
        lookup = self.tolookup(keySelector, elementSelector)
        return Query(lambda: itertools.imap(lambda x: resultSelector(x.key, x._elements), lookup))

    def distinct(self, key=lambda x: x):
        return Query(functools.partial(self._distinct, key))

    def _distinct(self, key):
        keys = set()
        for item in self._source:
            item_key = key(item)
            if item_key not in keys:
                keys.add(item_key)
                yield item

    def union(self, other, key=lambda x: x):
        return Query(lambda: self._union(other, key))

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
        return Query(functools.partial(self._intersection, other, key))

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
        return Query(functools.partial(self._except_, other, key))

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

    def todict(self, keySelector, elementSelector=lambda x: x):
        result = dict()
        for item in self._source:
            item_key = keySelector(item)
            if item_key in result:
                raise TypeError('keySelector produced duplicate key.')
            result[item_key] = elementSelector(item)
        return result

    def tolookup(self, keySelector, elementSelector=lambda x:x):
        result = Lookup()
        for item in self._source:
            result._add(keySelector(item), elementSelector(item))
        return result

    def iter_equal(self, other, key=lambda x:x):
        if not isinstance(other, collections.Iterable):
            raise TypeError('{!r}, the value of other, is not iterable.'.format(other))
        return all(itertools.imap(
            lambda x: x[0] is not _missing and x[1] is not _missing and key(x[0]) == key(x[1]),
            itertools.izip_longest(self._itersource(), other, fillvalue=_missing)))

    def first(self, predicate=lambda x:True, default=_missing):
        try:
            result = next(itertools.ifilter(predicate, self._itersource()))
        except StopIteration:
            if default is _missing:
                raise LookupError()
            return default

        return result

    def last(self, predicate=lambda x:True, default=_missing):
        last = _missing
        for item in itertools.ifilter(predicate, self._itersource()):
            last = item
        if last is _missing:
            if default is _missing:
                raise LookupError()
            return default

        return last

    def single(self, predicate=lambda x:True, default=_missing):
        iter_ = itertools.ifilter(predicate, self._itersource())
        try:
            result = next(iter_)
        except StopIteration:
            if default is _missing:
                raise LookupError('No items found.')
            return default

        try:
            next(iter_)
            raise LookupError('More than one item found.')
        except StopIteration:
            pass

        return result

    def _at_overrange_error(self, index):
        return ValueError('{!r}, the value of index, is greater than the number of elements.'.format(index))
        
    def at(self, index, default=_missing):
        if type(index) is not int:
            raise TypeError('{!r}, the value of index, is not an int.'.format(index))

        if index < 0:
            if default is not _missing:
                return default
            raise ValueError('{!r}, the value of index, is negative.'.format(index))

        if isinstance(self._source, collections.Sized) and index > len(self._source):
            if default is not _missing:
                return default
            raise self._at_overrange_error(index)

        if isinstance(self._source, collections.Sequence):
            return self._source[index]

        try:
            return next(itertools.islice(self._itersource(), index, None))
        except StopIteration:
            if default is not _missing:
                return default
            raise self._at_overrange_error(index)

    def ifempty(self, default):
        return Query(lambda: self._ifempty(default))

    def _ifempty(self, default):
        iter_ = self._itersource()
        try:
            next(iter_)
            return self._itersource()
        except StopIteration:
            return iter([default])

    def any(self, predicate=lambda x: True):
        return any(itertools.imap(predicate, self._itersource()))

    def all(self, predicate):
        return all(itertools.imap(predicate, self._itersource()))

    def contains(self, value, key=None):
        if key is None:
            if isinstance(self._source, collections.Container):
                return value in self._source
            return value in self._itersource()
        return value in itertools.imap(key, self._itersource())

    def count(self, predicate=None):
        if predicate is None and isinstance(self._source, collections.Sized):
            return len(self._source)
        predicate = predicate or (lambda x: True)
        return reduce(lambda x,y: x + 1, itertools.ifilter(predicate, self._itersource()), 0)

    def sum(self, selector=lambda x: x):
        return sum(itertools.imap(selector, itertools.ifilter(lambda x: x is not None, self._itersource())))

    def min(self, selector=lambda x: x):
        return min(itertools.imap(selector, self._itersource()))

    def max(self, selector=lambda x: x):
        return max(itertools.imap(selector, self._itersource()))

    def average(self, selector=lambda x: x):
        tally = reduce(lambda x,y: _Tally(x.sum + y, x.count + 1),
                       itertools.imap(selector, itertools.ifilter(lambda x: x is not None, self._itersource())),
                       _Tally(0,0))
        if not tally.count:
            return None
        #To avoid integer division
        if type(tally.sum) is int:
            tally = _Tally(float(tally.sum),tally.count)
        return tally.sum / tally.count

    def aggregate(self, seed, func, resultSelector=lambda x:x):
        return resultSelector(reduce(func, self._itersource(), seed))

    where.__doc__ = """Filters the source using the predicate.

    Arguments:
    predicate  -- The predicate used for filtering
    with_index -- False for the predicate to be called as predicate(item)
                  True for it to be called as predicate(index, item)

    Returns:
    A Query instance with the source items filtered by the predicate.

    Exceptions:
    A TypeError is raised if the value of predicate is not callable.

    Description:
    This method returns a Query instance with the source items filtered
    so that only items where predicate(item) is true are included.

    If with_index=True is specified, only items where
    predicate(index, item) is true are included. index is the zero-based
    index of the item in source.

    The order of the result items is the same as that of the source.
    Execution is deferred until the Query instance is iterated.
    The result items are streamed as they are iterated.
    Exceptions raised by the predicate are propagated.

    Example:
    >>> list(Query([1, 2, 3, 4, 5]).where(lambda item: item > 2))
    [3, 4, 5]

    >>> list(Query([1, 2, 3, 4, 5])
    ...     .where(lambda index, item: index < 1 or item > 2, with_index=True))
    [1, 3, 4, 5]
    """

    select.__doc__ = """Projects the source using the selector.

    Arguments:
    selector   -- The selector used for projection
    with_index -- False for the selector to be called as selector(item)
                  True for it to be called as selector(index, item)

    Returns:
    A Query instance with source items projected using selector.

    Exceptions:
    A TypeError is raised if the value of selector is not callable.

    Description:
    This method returns a Query that yields selector(item) for each item of the
    source.

    If with_index=True is specified, the returned query yields
    selector(index, item) instead, where index is the zero-based index of the
    item in source.

    The order of the result items corresponds to the order of the source items.
    Execution is deferred until the Query instance is iterated.
    The result items are streamed as they are iterated.
    Exceptions raised by the selector are propagated.

    Example:
    >>> list(Query([1, 2, 3, 4, 5]).select(lambda item: item ** 2))
    [1, 4, 9, 16, 25]

    >>> list(Query([1, 2, 3, 4, 5])
    ...     .select(lambda index, item: index * item, with_index=True))
    [0, 2, 6, 12, 20]
    """

_empty = Query([])

class OrderedQuery(Query):
    def thenby(self, keySelector):
        return OrderedQuery(self, _sort_keys=(self._sort_keys + (keySelector,)))

    def thenbydesc(self, keySelector):
        return self.thenby(lambda x: _ReverseKey(keySelector(x)))


