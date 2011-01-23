import collections
import functools
import inspect
import itertools
import operator

# used to indicate missing values
_missing = object()
_Tally = collections.namedtuple('Tally', ['sum', 'count'])

class _IterFuncWrapper(object):
    def __init__(self, func):
        self._func = func

    def __iter__(self):
        return iter(self._func())

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
    def empty():
        return _empty

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

    def __init__(self, source, _sort_keys=()):
        if not (self._is_iterable_but_not_iterator(source) or callable(source)):
            raise TypeError(('{!r}, value of source, must be iterable but not an iterator or a callable returning ' +
                             'an iterator.').format(source))
        self._source = self._iterable(source)
        self._sort_keys = _sort_keys

    def __iter__(self):
        result = self._source
        if self._sort_keys:
            #TODO: is it necessary to call iter(...)?
            result = sorted(result, key=lambda x: list(map(lambda y: y(x), self._sort_keys)))
        return iter(result)

    def _is_iterable_but_not_iterator(self, instance):
        return isinstance(instance, collections.Iterable) and not isinstance(instance, collections.Iterator)

    def _iterable(self, source):
        if isinstance(source, collections.Iterable):
            return source
        return _IterFuncWrapper(source)

    def where(self, predicate, with_index=False):
        if not callable(predicate):
            raise TypeError('{!r}, the value of predicate, is not callable.'.format(predicate))

        if not with_index:
            return Query(lambda: itertools.ifilter(predicate, self._source))
        else:
            first, second = itertools.tee(self._source)
            return Query(lambda: itertools.compress(first, itertools.starmap(predicate, enumerate(second))))

    def select(self, selector, with_index=False):
        if not callable(selector):
            raise TypeError('{!r}, the value of selector, is not callable.'.format(selector))

        if not with_index:
            return Query(lambda: itertools.imap(selector, self._source))
        else:
            return Query(lambda: itertools.starmap(selector, enumerate(self._source)))

    def selectmany(self, selector, resultSelector=lambda i, x: x, with_index=False):
        if not callable(selector):
            raise TypeError("{!r}, the value of selector, is not callable.".format(selector))
        if not callable(resultSelector):
            raise TypeError("{!r}, the value of resultSelector, is not callable.".format(resultSelector))

        return Query(lambda: self._selectmany(selector, resultSelector, with_index))

    def _selectmany(self, selector, resultSelector, with_index):
        if not with_index:
            for item in self._source:
                for subitem in selector(item):
                    yield resultSelector(item, subitem)
        else:
            for index, item in enumerate(self._source):
                for subitem in selector(index, item):
                    yield resultSelector(item, subitem)

    #Here is selectmany using itertools instead of a generator.
    #It's much more complicated.
    #def selectmany(self):
    #    def apply_result_selector(item, collection):
    #        return itertools.imap(functools.partial(resultSelector, item), collection)
    #
    #    first, second = itertools.tee(self._source)
    #    return Query(lambda: itertools.chain.from_iterable(itertools.starmap(
    #        apply_result_selector,
    #        itertools.izip(first, Query(second).select(selector, with_index)))))

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
            return Query(lambda: itertools.takewhile(predicate, self._source))
        else:
            return Query(lambda: itertools.imap(
                operator.itemgetter(1),
                itertools.takewhile(lambda x: predicate(*x), enumerate(self._source))))

    def skipwhile(self, predicate, with_index=False):
        if not with_index:
            return Query(lambda: itertools.dropwhile(predicate, self._source))
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
        if not isinstance(other, collections.Iterable):
            raise TypeError('{!r}, the value of other, is not iterable.'.format(other))

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
            itertools.izip_longest(self._source, other, fillvalue=_missing)))

    def first(self, predicate=lambda x:True, default=_missing):
        try:
            result = next(itertools.ifilter(predicate, self._source))
        except StopIteration:
            if default is _missing:
                raise LookupError()
            return default

        return result

    def last(self, predicate=lambda x:True, default=_missing):
        last = _missing
        for item in itertools.ifilter(predicate, self._source):
            last = item
        if last is _missing:
            if default is _missing:
                raise LookupError()
            return default

        return last

    def single(self, predicate=lambda x:True, default=_missing):
        iter_ = itertools.ifilter(predicate, self._source)
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
            return next(itertools.islice(self._source, index, None))
        except StopIteration:
            if default is not _missing:
                return default
            raise self._at_overrange_error(index)

    def ifempty(self, default):
        return Query(lambda: self._ifempty(default))

    def _ifempty(self, default):
        iter_ = iter(self._source)
        try:
            next(iter_)
            return self._source
        except StopIteration:
            return iter([default])

    def any(self, predicate=lambda x: True):
        if not callable(predicate):
            raise TypeError("{!r}, the value of predicate, is not callable.".format(predicate))

        return any(itertools.imap(predicate, self._source))

    def all(self, predicate):
        if not callable(predicate):
            raise TypeError("{!r}, the value of predicate, is not callable.".format(predicate))

        return all(itertools.imap(predicate, self._source))

    def contains(self, value, key=None):
        if key is None:
            if isinstance(self._source, collections.Container):
                return value in self._source
            return value in self._source
        return value in itertools.imap(key, self._source)

    def count(self, predicate=None):
        if predicate is not None and not callable(predicate):
            raise TypeError("{!r}, the value of predicate, is neither None nor callable.".format(predicate))

        if predicate is None and isinstance(self._source, collections.Sized):
            return len(self._source)
        predicate = predicate or (lambda x: True)
        return reduce(lambda x,y: x + 1, itertools.ifilter(predicate, self._source), 0)

    def sum(self, selector=lambda x: x):
        return sum(itertools.imap(selector, itertools.ifilter(lambda x: x is not None, self._source)))

    def min(self, selector=lambda x: x):
        return min(itertools.imap(selector, self._source))

    def max(self, selector=lambda x: x):
        return max(itertools.imap(selector, self._source))

    def average(self, selector=lambda x: x):
        tally = reduce(lambda x,y: _Tally(x.sum + y, x.count + 1),
                       itertools.imap(selector, itertools.ifilter(lambda x: x is not None, self._source)),
                       _Tally(0,0))
        if not tally.count:
            return None
        #To avoid integer division
        if type(tally.sum) is int:
            tally = _Tally(float(tally.sum),tally.count)
        return tally.sum / tally.count

    def aggregate(self, seed, func, resultSelector=lambda x:x):
        return resultSelector(reduce(func, self._source, seed))

    where.__doc__ = """Filters the source using the predicate.

    Arguments:
      predicate  -- The predicate used for filtering
      with_index -- False for the predicate to be called as predicate(item)
                  True for it to be called as predicate(index, item)

    Returns:
      A Query instance with the source items filtered by the predicate.

    Raises:
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

    Examples:
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

    Raises:
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

    Examples:
      >>> list(Query([1, 2, 3, 4, 5]).select(lambda item: item ** 2))
      [1, 4, 9, 16, 25]

      >>> list(Query([1, 2, 3, 4, 5])
      ...     .select(lambda index, item: index * item, with_index=True))
      [0, 2, 6, 12, 20]
    """

    selectmany.__doc__ = """Performs a one-to-many projection on source

    Arguments:
      selector       -- Callable, accepting one or two arguments depending on
                        the value of with_index, and returning an
                        iterable, used to project source elements to iterables
      resultSelector -- Callable, accepting two arguments, that combines the
                        element and its collection projection to project
                        a result
      with_index     -- False for the selector to be called as selector(item)
                        True for it to be called as selector(index, item)

    Returns:
      Returns a Query instance containing a one-to-many project of
      the source items using selector and then resultSelector.

    Raises:
      TypeError if selector is not callable or resultSelector is not callable.

    Description:
      The source items are projected to iterables using selector, then
      each item of these iterables is projected to a result element by
      resultSelector.

      The order of the result items corresponds to the order of the source items
      and then items projected by selector.
      Execution is deferred until the Query instance is iterated.
      The result items are streamed as they are iterated.
      Exceptions raised by the selector are propagated.

    Examples:
      >>> list(Query([1, 2, 3]).selectmany(lambda item: range(1, item + 1)))
      [1, 1, 2, 1, 2, 3]

      >>> list(Query([1, 2, 3]).selectmany(
      ...     lambda item: range(1, item + 1),
      ...     lambda item, subitem: item * subitem))
      [1, 2, 4, 3, 6, 9]

      >>> list(Query([1, 2, 3]).selectmany(
      ...     lambda index, item: range(index, item + 1),
      ...     with_index=True))
      [0, 1, 1, 2, 2, 3]
    """

    join.__doc__ = """Performs a one-to-one join to other.

      Arguments:
        other            -- Iterable that source is joined to.
        keySelector      -- Callable, accepting one arg, used to get key
                            values for source items
        otherKeySelector -- Callable, accepting one arg, used to get key
                            values for other items
        resultSelector   -- Callable, accepting two args, used to combine
                            joined item pairs

      Returns:
        Query yielding the joined item pairs of source and other combined
        using resultSelector. An item in source is matched to an item
        in other if keySelector(source_item) == otherKeySelector(other_item).
        The items are ordered by source item and then other item using
        the orderings ordering of source and other.

      Examples:
        >>> import string
        >>> list(Query(string.ascii_lowercase).join(
        ...     'The War of the Worlds'.split(),
        ...     lambda item: item.upper(),
        ...     lambda other_item: other_item[0].upper(),
        ...     lambda item, other_item: (item, other_item)))
        [('o', 'of'), ('t', 'The'), ('t', 'the'), ('w', 'War'), ('w', 'Worlds')]

        >>> list(Query([2, 1, 0]).join(
        ...     range(0, 5),
        ...     lambda item: item,
        ...     lambda other_item: other_item % 3,
        ...     lambda item, other_item: (item, other_item)))
        [(2, 2), (1, 1), (1, 4), (0, 0), (0, 3)]
    """

    concat.__doc__ =  """Returns a Query containing the concatenation of source and other.

    Arguments:
      other - The iterable to concatenate

    Returns:
      A Query object containing the concatenation of source and other. In
      other words, an instance of Query that yields all the items in source and
      then all the items in other.

    Examples:
      >>> list(Query([1, 2]).concat([3, 4, 5]))
      [1, 2, 3, 4, 5]
    """

    groupby.__doc__ = """Returns a Query that yields the processed items grouped by key.

    Arguments:
      key             -- Callable, accepting one arg, using for grouping.
      elementSelector -- Callable, accepting one arg, applied to items to
                         provide the values that are grouped.
      resultSelector  -- Callable, accepting a key value and an iterable of
                         items grouped that key, used to produce the final
                         result.

    Returns:
      A Query object yielding the results of resultSelector applied to each
      group of source items grouped by key. The results are unordered.

    Examples:
      >>> groups = list(Query('The Cat in the Hat'.split()).groupby(len))
      >>> first_group, second_group = groups[0], groups[1]
      >>> if first_group.key > second_group.key:
      ...     first_group, second_group = second_group, first_group
      >>> first_group.key, list(first_group)
      (2, ['in'])
      >>> second_group.key, list(second_group)
      (3, ['The', 'Cat', 'the', 'Hat'])

      >>> groups = list(Query('The Cat in the Hat'.split())
      ...     .groupby(len, elementSelector=str.upper))
      >>> first_group, second_group = groups[0], groups[1]
      >>> if first_group.key > second_group.key:
      ...     first_group, second_group = second_group, first_group
      >>> first_group.key, list(first_group)
      (2, ['IN'])
      >>> second_group.key, list(second_group)
      (3, ['THE', 'CAT', 'THE', 'HAT'])

      >>> group_mins = list(Query('The Cat in the Hat'.split())
      ...     .groupby(len, elementSelector=str.upper,
      ...              resultSelector=lambda key, items: min(items)))
      >>> first_min, second_min = group_mins[0], group_mins[1]
      >>> if len(first_min) > len(second_min):
      ...     first_min, second_min = second_min, first_min
      >>> first_min
      'IN'
      >>> second_min
      'CAT'
     """

    distinct.__doc__ = """Returns a Query containing the distinct items of source.

    Arguments:
      key -- Callable, that accepts one arg, which returns a value used to compare
             items.

    Returns:
      A Query containing the distinct items of source. Items are considered
      distinct if they produce different values when key is applied to them.
      If more than one item has the same key value, the first is kept and the
      rest are dropped.

    Examples:
     >>> list(Query([1, 1, 2, 3]).distinct())
     [1, 2, 3]

     >>> list(Query([-2, -1, 0, 1, 2]).distinct(abs))
     [-2, -1, 0]
    """

    union.__doc__ = """Returns a Query that yields the union of source and other

    Arguments:
      other -- Iterable with which the iterm of Query will be unioned
      key   -- Callable, that accepts one arg, which returns a value used to compare
               items.

    Returns:
      A Query yielding the distinct items of source unioned with the items of
      other. Items are considered distinct if they produce different values
      when key is applied to them. If more than one item has the same key
      value, the first is kept and the rest are dropped.

    Examples:
      >>> list(Query([1, 1, 2]).union([3]))
      [1, 2, 3]

      >>> list(Query([-2, -1, 0]).union([0, 1, 2], key=abs))
      [-2, -1, 0]
    """

    intersection.__doc__ = """Returns a Query that yields the intersection of source and other

    Arguments:
      other -- Iterable with which the items of Query will be intersected.
      key   -- Callable, that accepts one arg, which returns a value used to
               compare items.

    Returns:
      A Query that yields the distinct items of other that are equal to any
      item in source. Items are considered distinct if they produce different
      values when key is applied to them and are equal if they produce the
      same value. If more than one item in other has the same key value, the
      first one in is kept and the rest are dropped.

    Examples:
      >>> list(Query([1, 2, 3]).intersection([1]))
      [1]

      >>> list(Query([-3, -2, -1]).intersection([0, 1], key=abs))
      [1]
    """

    except_.__doc__ = """Returns a Query that yields the items of source that aren't in other

    Arguments:
      other -- Iterable whose items are excluded from the result
      key   -- Callable, that accepts one arg, which returns a value used to
               compare items.

    Returns:
      A Query that yields the distinct items of source that aren't equal to
      any item in other. Items are considered distinct if they produce different
      values when key is applied to them and are equal if they produce the same
      value. If more than one item in other has the same key value, the first
      one in is kept and the rest are dropped.

    Examples:
      >>> list(Query([1, 2, 2, 3]).except_([1]))
      [2, 3]

      >>> list(Query([-1, 0, 1]).except_([0, 1], key=abs))
      []
    """

    tolist.__doc__ = """Returns a list containing souce items.

    Examples:
      >>> Query('abc').tolist()
      ['a', 'b', 'c']
    """

    tolookup.__doc__ = """Returns a Lookup instance containing the source items grouped by key.

    Arguments:
      key -- Callable, that accepts arg, which returns a value used to
             group items.

    Returns:
      A Lookup instance containing the source items grouped by key

    Examples:
      >>> lookup = Query([-2, -1, 0, 1, 2]).tolookup(abs)
      >>> list(lookup[0])
      [0]
      >>> list(lookup[1])
      [-1, 1]
      >>> list(lookup[2])
      [-2, 2]
    """

    first.__doc__ = """Returns the first element matching predicate

    Arguments:
      predicate -- Callable, accepting one argument, that items are matched
                   against.
      default   -- The value to return if no matching element is found.

    Returns:
      The first item that matches predicate or, if no match is found, default.

    Examples:
      >>> Query([1, 2, 3]).first(lambda x: 2 * x > 5)
      3

      >>> Query([1, 2, 3]).first(lambda x: x > 5, 'No matches')
      'No matches'
    """

    last.__doc__ = """Returns the last element matching predicate

    Arguments:
      predicate -- Callable, accepting one argument, that items are matched
                   against.
      default   -- The value to return if no matching element is found.

    Returns:
      The last item that matches predicate or, if no match is found, default.

    Examples:
      >>> Query([1, 2, 3]).last(lambda x: 2 * x < 5)
      2

      >>> Query([1, 2, 3]).first(lambda x: x > 5, 'No matches')
      'No matches'
    """

    single.__doc__ = """Returns the only element to matching predicate

    Arguments:
      predicate -- Callable, accepting one argument, that items are matched
                   against
      default   -- The value to return if no matching element is found

    Returns:
      The only element that matches predicate or, if no match is found,
      default or, if more than one match is found, raises LookUpError.

    Examples:
      >>> Query([1, 2, 3]).single(lambda x: 2 * x == 4)
      2

      >>> Query([1, 2, 3]).single(lambda x: x < 0, 'No matches')
      'No matches'

      >>> # Query([1, 2, 3]).single(lambda x: x > 0)
      Traceback (most recent call last):
      ...
      LookupError: More than one item found.
    """

    ifempty.__doc__ = """If empty returns a Query containing default

    Arguments:
      default -- the value the returned Query will contains if there are no
                 items

    Returns:
      If there are no items, an instance containing default as the only item
      is returned, otherwise a new instance with the same items is returned.

    Examples:
      >>> list(Query([1, 2, 3]).ifempty('No items'))
      [1, 2, 3]

      >>> list(Query([]).ifempty('No items'))
      ['No items']
    """

    all.__doc__ = """Returns whether all elements match predicate

    Arguments:
      predicate -- Callable, accepting one argument, that items are matched
                   against.

    Returns:
      True if all elements match the predicate.
      False if any element does not.

    Raises:
      TypeError if predicate is not callable.

    Notes:
      If the instance has no elements, all returns True.
      This method 'short-circuits'; it will iterate through elements and return
      False immediately if an element doesn't match.

    Examples:
      >>> Query([1, 2, 3]).all(lambda x: x > 0)
      True

      >>> Query([1, 2, 3]).all(lambda x: x < 3)
      False
    """

    any.__doc__ = """Returns whether any elements match predicate

    Arguments:
      predicate -- Callable, accepting one argument, that items are matched
                   against.

    Returns:
      True if any element matches the predicate.
      False if none of them do.

    Notes:
      If the instance has no elements, any returns False.
      This method 'short-circuits'; it will iterate through elements and return
      True immediately if an element matches.

    Examples:
      >>> Query([1, 2, 3]).any(lambda x: x == 3)
      True
    """

    count.__doc__ = """Returns the number of items in source matching predicate.

    Arguments:
      predicate - Only items for which predicate(item) is true will be counted.
                  If predicate is None all the items are counted.

    Returns:
      The number of items in source matching predicate or the number of items
      if predicate is None.

    Raises:
      A TypeError is raised if predicate is neither None nor callable.

    Examples:
      >>> Query([1, 2, 3, 4, 5]).count(lambda item: item > 2)
      3

      >>> Query([1, 2, 3, 4, 5]).count()
      5
    """

    aggregate.__doc__ = """Applies an accumulator function to items.

    Arguments:
      seed           -- The initial value
      func           -- The accumulator - callable, accepting two arguments
      resultSelector -- Callable, accepting one argument, that is applied to
                        the result of the accumulator

    Returns:
      Beginning the seed and first element, func is applied to the result of
      it's last call and the next element until the elements run out. The
      resultSelector is then run on the output and returned.

    Examples:
      >>> Query(['a', 'b', 'c', 'd']).aggregate(
      ...     'x',
      ...     lambda acc, item: '(' + acc + '+' + item + ')',
      ...     lambda output: output.upper())
      '((((X+A)+B)+C)+D)'

      >>> Query([1, 2, 3]).aggregate(0, lambda acc, item: acc + item)
      6
    """

Query.empty.__doc__ = """Returns an empty Query with no items."""

Query.range.__doc__ = """Returns a Query which will yield count successive integers starting at start.

    Arguments:
      start -- The integer to start with
      count -- The number of integers to return

    Returns:
      Returns a Query which will yield count successive integers starting at start.

    Raises:
      A ValueError is raised if the value of count is negative or
      if the value of start + count > sys.maxint which results in an overflow.

    Examples:
      >>> list(Query.range(1,5))
      [1, 2, 3, 4, 5]
    """

Query.repeat.__doc__ = """Returns a Query that yields element count times.

    Arguments:
      element -- The object to yield
      count   -- The number of times to yield element

    Returns:
      A Query that yields element count times.

    Raises:
      A ValueError is raised if count is negative.

    Examples:
      >>> list(Query.repeat('SPAM!', 3))
      ['SPAM!', 'SPAM!', 'SPAM!']
    """

_empty = Query([])

class OrderedQuery(Query):
    def thenby(self, keySelector):
        return OrderedQuery(self, _sort_keys=(self._sort_keys + (keySelector,)))

    def thenbydesc(self, keySelector):
        return self.thenby(lambda x: _ReverseKey(keySelector(x)))


