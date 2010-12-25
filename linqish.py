import inspect
import itertools

class Query(object):
    def _get_number_of_args(self, func):
        return len(inspect.getargspec(func)[0])

    def __init__(self, source):
        if source is None:
            raise TypeError()
        self._source = source

    def where(self, predicate):
        number_of_args = self._get_number_of_args(predicate)
        if number_of_args == 1:
            return itertools.ifilter(predicate, self._source)
        elif number_of_args == 2:
            return itertools.compress(self._source, itertools.starmap(predicate, enumerate(self._source)))
        else:
            raise Exception()

    def select(self, selector):
        number_of_args = self._get_number_of_args(selector)
        if number_of_args == 1:
            return itertools.imap(selector, self._source)
        elif number_of_args == 2:
            return itertools.starmap(selector, enumerate(self._source))
        else:
            raise Exception()

    def selectMany(self, selector):
        for item in self._source:
            for inner_item in selector(item):
                yield inner_item

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

