class Query(object):
    def __init__(self, source):
        if source is None:
            raise TypeError()
        self._source = source

    def where(self, predicate):
        for item in self._source:
            if predicate(item):
                yield item

    def select(self, selector):
        for item in self._source:
            yield selector(item)

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

