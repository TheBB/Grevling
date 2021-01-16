def subclasses(cls, root=False):
    if root:
        yield cls
    for sub in cls.__subclasses__():
        yield sub
        yield from subclasses(sub, root=False)


def find_subclass(cls, name, root=False, attr='__tag__'):
    for sub in subclasses(cls, root=root):
        if hasattr(sub, attr) and getattr(sub, attr) == name:
            return sub
    return None


def subindex_set(target, key, value):
    *path, last = key.split('/')
    for p in path:
        target = target[p]
    target[last] = value


class NestedDict(dict):

    def __contains__(self, key: str):
        first, _, rest = key.partition('/')
        if rest:
            return super().__contains__(first) and rest in super().__getitem__(first)
        return super().__contains__(first)

    def __setitem__(self, key: str, value):
        first, _, rest = key.partition('/')
        if rest:
            if not super().__contains__(first):
                super().__setitem__(first, NestedDict())
            super().__getitem__(first)[rest] = value
        else:
            super().__setitem__(key, value)

    def __getitem__(self, key: str):
        first, _, rest = key.partition('/')
        if rest:
            return super().__getitem__(first)[rest]
        return super().__getitem__(first)

    def keys(self):
        for key, val in super().items():
            if isinstance(val, NestedDict):
                for subkey in val.keys():
                    yield f'{key}/{subkey}'
            else:
                yield key

    def values(self):
        for _, val in super().items():
            if isinstance(val, NestedDict):
                yield from val.values()
            else:
                yield val

    def items(self):
        for key, val in super().items():
            if isinstance(val, NestedDict):
                for subkey, subval in val.items():
                    yield f'{key}/{subkey}', subval
            else:
                yield key, val

    def map(self, func):
        retval = NestedDict()
        for key, val in self.items():
            retval[key] = func(val)
        return retval

    def as_list_of_tuples(self):
        return [
            (key, value.as_list_of_tuples() if isinstance(value, NestedDict) else value)
            for key, value in super().items()
        ]
