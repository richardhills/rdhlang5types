

class Type(object):

    def is_copyable_from(self, other):
        raise NotImplementedError()


class AnyType(Type):

    def is_copyable_from(self, other):
        return True

    
class NoValueType(Type):

    def is_copyable_from(self, other):
        return isinstance(other, NoValueType)


class UnitType(Type):
    def __init__(self, value):
        self.value = value

    def is_copyable_from(self, other):
        if not isinstance(other, UnitType):
            return False
        return other.value == self.value


class StringType(Type):
    def is_copyable_from(self, other):
        return isinstance(other, StringType) or (isinstance(other, UnitType) and isinstance(other.value, str))


class IntegerType(Type):
    def is_copyable_from(self, other):
        return isinstance(other, IntegerType) or (isinstance(other, UnitType) and isinstance(other.value, int))

def unwrap_types(type):
    if isinstance(type, OneOfType):
        return type.types
    return [ type ]

def merge_types(types, mode):
    to_drop = []
    for i1, t1 in enumerate(types):
        for i2, t2 in enumerate(types):
            if i1 > i2 and t1.is_copyable_from(t2) and t2.is_copyable_from(t1):
                to_drop.append(i1)

    types = [t for i, t in enumerate(types) if i not in to_drop]

    for i1, t1 in enumerate(types):
        for i2, t2 in enumerate(types):
            if i1 != i2 and t1.is_copyable_from(t2):
                if mode == "super":
                    to_drop.append(i2)
                elif mode == "sub":
                    to_drop.append(i1)
    types = [t for i, t in enumerate(types) if i not in to_drop]

    if len(types) == 0:
        return NoValueType()
    elif len(types) == 1:
        return types[0]
    else:
        return OneOfType(types)


class OneOfType(Type):
    def __init__(self, types):
        self.types = types

    def is_copyable_from(self, other):
        for t in self.types:
            if t.is_copyable_from(other):
                return True
        return False


class Const(object):
    def __init__(self, wrapped):
        self.wrapped = wrapped

