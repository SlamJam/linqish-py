import collections
import inspect
import itertools

class Query(object):
    def _get_number_of_args(self, func):
        return len(inspect.getargspec(func)[0])

    def __init__(self, source):
        if not isinstance(source, collections.Iterable):
            raise TypeError('{!r} is not an Iterable'.format(source))
        self._source = iter(source)

    def where(self, predicate):
        number_of_args = self._get_number_of_args(predicate)
        if number_of_args == 1:
            return itertools.ifilter(predicate, self._source)
        elif number_of_args == 2:
            first, second = itertools.tee(self._source)
            return itertools.compress(first, itertools.starmap(predicate, enumerate(second)))
        else:
            raise ValueError('value of predicate has wrong number of args')

    def select(self, selector):
        number_of_args = self._get_number_of_args(selector)
        if number_of_args == 1:
            return itertools.imap(selector, self._source)
        elif number_of_args == 2:
            return itertools.starmap(selector, enumerate(self._source))
        else:
            raise ValueError('value of selector has wrong number of args')

    def selectmany(self, selector, resultSelector=None):
        number_of_args = self._get_number_of_args(selector)
        if number_of_args == 1:
            return itertools.chain.from_iterable(itertools.imap(selector, self._source))
        elif number_of_args == 2:
            return itertools.chain.from_iterable(itertools.starmap(selector, enumerate(self._source)))
        else:
            raise ValueError('value of selector has wrong number of args')

    def take(self, count):
        enumerator = enumerate(self._source)
        try:
            while True:
                index, item = next(enumerator)
                if index == count:
                    break
                yield item
        except StopIteration:
            pass

    def skip(self, count):
        enumerator = enumerate(self._source)
        try:
            while True:
                index, item = next(enumerator)
                if index == count:
                    break
            while True:
                index, item = next(enumerator)
                yield item
        except StopIteration:
            pass

    def takeWhile(self, predicate):
        iter = self._source.__iter__()
        try:
            while True:
                item = next(iter)
                if predicate(item):
                    break
        except StopIteration:
            pass

