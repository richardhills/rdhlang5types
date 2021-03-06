from UserDict import DictMixin
from collections import OrderedDict

from rdhlang5_types.composites import bind_type_to_value, unbind_type_to_value, \
    DefaultFactoryType, CompositeType, CompositeObjectManager, Composite
from rdhlang5_types.core_types import merge_types, OneOfType, AnyType, Const
from rdhlang5_types.exceptions import FatalError, MicroOpConflict, raise_if_safe, \
    InvalidAssignmentType, InvalidDereferenceKey, InvalidDereferenceType, \
    MicroOpTypeConflict, MissingMicroOp, InvalidAssignmentKey
from rdhlang5_types.managers import get_manager, get_type_of_value
from rdhlang5_types.micro_ops import MicroOpType, MicroOp, raise_micro_op_conflicts

WILDCARD = object()
MISSING = object()


def get_key_and_type(micro_op_type):
    if isinstance(micro_op_type, (DictWildcardGetterType, DictWildcardSetterType, DictWildcardDeletterType)):
        key = WILDCARD
    elif isinstance(micro_op_type, (DictGetterType, DictSetterType, DictDeletterType)):
        key = micro_op_type.key
    else:
        raise FatalError()

    if isinstance(micro_op_type, (DictWildcardGetterType, DictGetterType, DictWildcardSetterType, DictSetterType)):
        type = micro_op_type.type
    else:
        type = MISSING

    return key, type


def get_key_and_new_value(micro_op, args):
    if isinstance(micro_op, (DictWildcardGetter, DictWildcardDeletter)):
        key, = args
        new_value = MISSING
    elif isinstance(micro_op, (DictGetter, DictDeletter)):
        key = micro_op.key
        new_value = MISSING
    elif isinstance(micro_op, DictWildcardSetter):
        key, new_value = args
    elif isinstance(micro_op, DictSetter):
        key = micro_op.key
        new_value = args[0]
    else:
        raise FatalError()
    if new_value is not None:
        get_manager(new_value)
    return key, new_value


class DictMicroOpType(MicroOpType):
    def check_for_conflicts_with_existing_micro_ops(self, obj, micro_op_types):
        if not isinstance(obj, RDHDict):
            raise MicroOpTypeConflict()
        return super(DictMicroOpType, self).check_for_conflicts_with_existing_micro_ops(obj, micro_op_types)


class DictWildcardGetterType(DictMicroOpType):
    def __init__(self, type, key_error, type_error):
        if isinstance(type, dict) or isinstance(type, RDHDict):
            pass
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def create(self, target, through_type):
        return DictWildcardGetter(target, through_type, self.type, self.key_error, self.type_error)

    def bind(self, key, target):
        if key is not None:
            keys = [ key ]
        else:
            keys = target.keys()
        for k in keys:
            value = target.wrapped[k]
            get_manager(value)
            bind_type_to_value(target, key, self.type, value)

    def unbind(self, key, target):
        if key is not None:
            keys = [ key ]
        else:
            keys = target.wrapped.keys()
        for k in keys:
            unbind_type_to_value(target, key, self.type, target.wrapped[k])

    def check_for_conflicts_with_existing_micro_ops(self, obj, micro_op_types):
        default_factory = micro_op_types.get(("default-factory",), None)
        has_default_factory = default_factory is not None

        if not self.key_error:
            if not has_default_factory:
                raise MicroOpTypeConflict()
            if not self.type.is_copyable_from(default_factory.type):
                raise MicroOpTypeConflict()

        return super(DictWildcardGetterType, self).check_for_conflicts_with_existing_micro_ops(obj, micro_op_types)

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type, other_micro_op_types):
        has_default_factory = ("default-factory",) in other_micro_op_types
        if not self.key_error:
            if not has_default_factory:
                return True

        if isinstance(other_micro_op_type, (DictGetterType, DictWildcardGetterType)):
            return False
        if isinstance(other_micro_op_type, (DictSetterType, DictWildcardSetterType)):
            if not self.type_error and not self.type.is_copyable_from(other_micro_op_type.type):
                return True
        if isinstance(other_micro_op_type, (DictDeletterType, DictWildcardDeletterType)):
            if not self.key_error and not has_default_factory:
                return True
        return False

    def raise_on_micro_op_conflict(self, other_micro_op, args):
        if isinstance(other_micro_op, (DictSetter, DictWildcardSetter)):
            _, new_value = get_key_and_new_value(other_micro_op, args)
            if not self.type_error and not self.type.is_copyable_from(get_type_of_value(new_value)):
                raise_if_safe(InvalidAssignmentType, other_micro_op.type_error)

        if isinstance(other_micro_op, (DictDeletter, DictWildcardDeletter)):
            if not self.key_error:
                raise_if_safe(InvalidAssignmentType, other_micro_op.key_error)
        return False

    def check_for_data_conflict(self, obj):
        if not self.key_error and get_manager(obj).default_factory is None:
            return True

        if not self.type_error:
            for value in obj.wrapped.values():
                get_manager(value)
                if isinstance(self.type, dict):
                    pass
                if not self.type.is_copyable_from(get_type_of_value(value)):
                    return True

        return False

    def merge(self, other_micro_op_type):
        return DictWildcardGetterType(
            merge_types([ self.type, other_micro_op_type.type ], "sub"),
            self.key_error or other_micro_op_type.key_error,
            self.type_error or other_micro_op_type.type_error
        )


class DictWildcardGetter(MicroOp):

    def __init__(self, target, through_type, type, key_error, type_error):
        self.target = target
        self.through_type = through_type
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def invoke(self, key):
        raise_micro_op_conflicts(self, [ key ], get_manager(self.target).get_flattened_micro_op_types())

        if key in self.target.wrapped:
            value = self.target.__getitem__(key, raw=True)
        else:
            default_factory_op_type = get_manager(self.target).get_micro_op_type(self.through_type, ("default-factory",))

            if not default_factory_op_type:
                raise_if_safe(InvalidDereferenceKey, self.key_error)

            default_factory_op = default_factory_op_type.create(self.target, self.through_type)
            value = default_factory_op.invoke(key)

        get_manager(value)

        type_of_value = get_type_of_value(value)
        if not self.type.is_copyable_from(type_of_value):
            raise raise_if_safe(InvalidDereferenceType, self.type_error)
        return value


class DictGetterType(DictMicroOpType):
    def __init__(self, key, type, key_error, type_error):
        self.key = key
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def create(self, target, through_type):
        return DictGetter(target, through_type, self.key, self.type, self.key_error, self.type_error)

    def bind(self, key, target):
        if key is not None and key != self.key:
            return
        value = target.wrapped[self.key]
        get_manager(value)
        bind_type_to_value(target, self.key, self.type, value)

    def unbind(self, key, target):
        if key is not None and key != self.key:
            return
        unbind_type_to_value(target, self.key, self.type, target.wrapped[key])

    def check_for_conflicts_with_existing_micro_ops(self, obj, micro_op_types):
        default_factory = micro_op_types.get(("default-factory",), None)
        has_default_factory = default_factory is not None
        has_value_in_place = self.key in obj.wrapped

        if not self.key_error and not has_value_in_place:
            if not has_default_factory:
                raise MicroOpTypeConflict()
            if not self.type.is_copyable_from(default_factory.type):
                raise MicroOpTypeConflict()

        return super(DictGetterType, self).check_for_conflicts_with_existing_micro_ops(obj, micro_op_types)

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type, other_micro_op_types):
        if isinstance(other_micro_op_type, (DictGetterType, DictWildcardGetterType)):
            return False
        if isinstance(other_micro_op_type, (DictSetterType, DictWildcardSetterType)):
            other_key, other_type = get_key_and_type(other_micro_op_type)
            if other_key is not WILDCARD and other_key != self.key:
                return False
            if not self.type_error and not other_micro_op_type.type_error and not self.type.is_copyable_from(other_type):
                return True
        if isinstance(other_micro_op_type, (DictDeletterType, DictWildcardDeletterType)):
            other_key, _ = get_key_and_type(other_micro_op_type)
            if other_key is not WILDCARD and other_key != self.key:
                return False

            default_factory = other_micro_op_types.get(("default-factory",), None)
            has_default_factory = default_factory is not None

            if not self.key_error and not has_default_factory:
                return True
        return False

    def raise_on_micro_op_conflict(self, other_micro_op, args):
        if isinstance(other_micro_op, (DictGetter, DictWildcardGetter)):
            return
        if isinstance(other_micro_op, (DictSetter, DictWildcardSetter)):
            other_key, other_new_value = get_key_and_new_value(other_micro_op, args)
            if other_key != self.key:
                return
            if not self.type_error and not self.type.is_copyable_from(get_type_of_value(other_new_value)):
                raise_if_safe(InvalidAssignmentType, other_micro_op.type_error)
        if isinstance(other_micro_op, (DictDeletter, DictWildcardDeletter)):
            other_key, _ = get_key_and_new_value(other_micro_op, args)
            if not self.key_error and other_key == self.key:
                raise raise_if_safe(InvalidDereferenceKey, other_micro_op.key_error)

    def check_for_data_conflict(self, obj):
        value_in_place = obj.wrapped[self.key]
        get_manager(value_in_place)
        type_of_value = get_type_of_value(value_in_place)
        if not self.type.is_copyable_from(type_of_value):
            return True

        return False

    def merge(self, other_micro_op_type):
        if other_micro_op_type.key != self.key:
            raise FatalError()
        return DictGetterType(
            self.key,
            merge_types([ self.type, other_micro_op_type.type ], "sub"),
            self.key_error or other_micro_op_type.key_error,
            self.type_error or other_micro_op_type.type_error
        )


class DictGetter(MicroOp):
    def __init__(self, target, through_type, key, type, key_error, type_error):
        self.target = target
        self.through_type = through_type
        self.key = key
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def invoke(self):
        raise_micro_op_conflicts(self, [], get_manager(self.target).get_flattened_micro_op_types())

        if self.key in self.target.wrapped:
            value = self.target.wrapped[self.key]
        else:
            default_factory_op = get_manager(self.target).get_micro_op_type(self.through_type, ("default-factory",))

            if default_factory_op:
                value = default_factory_op.invoke(self.key)
            else:
                raise_if_safe(InvalidDereferenceKey, self.key_error)

        get_manager(value)

        type_of_value = get_type_of_value(value)

        if not self.type.is_copyable_from(type_of_value):
            raise raise_if_safe(InvalidDereferenceType, self.type_error)

        return value


class DictWildcardSetterType(DictMicroOpType):
    def __init__(self, type, key_error, type_error):
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def create(self, target, through_type):
        return DictWildcardSetter(target, self.type, self.key_error, self.type_error)

    def bind(self, key, target):
        pass

    def unbind(self, key, target):
        pass

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type, other_micro_op_types):
        if isinstance(other_micro_op_type, (DictGetterType, DictWildcardGetterType)):
            if not self.type_error and not other_micro_op_type.type_error and not other_micro_op_type.type.is_copyable_from(self.type):
                return True
        return False

    def raise_on_micro_op_conflict(self, other_micro_op, args):
        pass

    def check_for_data_conflict(self, obj):
        return False

    def merge(self, other_micro_op_type):
        return DictWildcardSetterType(
            merge_types([ self.type, other_micro_op_type.type ], "super"),
            self.key_error or other_micro_op_type.key_error,
            self.type_error or other_micro_op_type.type_error
        )


class DictWildcardSetter(MicroOp):
    def __init__(self, target, type, key_error, type_error):
        self.target = target
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def invoke(self, key, new_value):
        get_manager(new_value)
        raise_micro_op_conflicts(self, [ key, new_value ], get_manager(self.target).get_flattened_micro_op_types())

        new_value_type = get_type_of_value(new_value)
        if not self.type.is_copyable_from(new_value_type):
            raise FatalError()

        for other_micro_op_type in get_manager(self.target).get_flattened_micro_op_types():
            other_micro_op_type.unbind(key, self.target)

        self.target.wrapped[key] = new_value

        for other_micro_op_type in get_manager(self.target).get_flattened_micro_op_types():
            other_micro_op_type.bind(key, self.target)


class DictSetterType(DictMicroOpType):
    def __init__(self, key, type, key_error, type_error):
        self.key = key
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def create(self, target, through_type):
        return DictSetter(target, self.key, self.type, self.key_error, self.type_error)

    def bind(self, key, target):
        pass

    def unbind(self, key, target):
        pass

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type, other_micro_op_types):
        if isinstance(other_micro_op_type, (DictGetterType, DictWildcardGetterType)):
            other_key, other_type = get_key_and_type(other_micro_op_type)
            if other_key is not WILDCARD and other_key != self.key:
                return False
            if not self.type_error and not other_micro_op_type.type_error and not other_type.is_copyable_from(self.type):
                return True
        return False

    def raise_on_micro_op_conflict(self, other_micro_op, args):
        pass

    def check_for_data_conflict(self, obj):
        return False

    def merge(self, other_micro_op_type):
        if other_micro_op_type.key != self.key:
            raise FatalError()
        return DictSetterType(
            self.key,
            merge_types([ self.type, other_micro_op_type.type ], "super"),
            self.key_error or other_micro_op_type.key_error,
            self.type_error or other_micro_op_type.type_error
        )


class DictSetter(MicroOp):

    def __init__(self, target, key, type, key_error, type_error):
        self.target = target
        self.key = key
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def invoke(self, new_value):
        get_manager(new_value)
        raise_micro_op_conflicts(self, [ new_value ], get_manager(self.target).get_flattened_micro_op_types())

        new_value_type = get_type_of_value(new_value)
        if not self.type.is_copyable_from(new_value_type):
            raise FatalError()

        for other_micro_op_type in get_manager(self.target).get_flattened_micro_op_types():
            other_micro_op_type.unbind(self.key, self.target)

        self.target.wrapped[self.key] = new_value

        for other_micro_op_type in get_manager(self.target).get_flattened_micro_op_types():
            other_micro_op_type.bind(self.key, self.target)


class InvalidDeletion(Exception):
    pass


class DictWildcardDeletterType(DictMicroOpType):

    def __init__(self, key_error):
        self.key_error = key_error

    def create(self, target, through_type):
        return DictWildcardDeletter(target, self.key_error)

    def bind(self, key, target):
        pass

    def unbind(self, key, target):
        pass

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type, other_micro_op_types):
        if isinstance(other_micro_op_type, (DictGetterType, DictWildcardGetterType)):
            default_factory = micro_op_types.get(("default-factory",), None)
            has_default_factory = default_factory is not None

            if not other_micro_op_type.key_error and not has_default_factory:
                return True
        return False

    def raise_on_micro_op_conflict(self, other_micro_op, args):
        return False

    def check_for_data_conflict(self, obj):
        return False

    def merge(self, other_micro_op_type):
        return DictWildcardDeletterType(
            self.key_error or other_micro_op_type.key_error
        )


class DictWildcardDeletter(MicroOp):

    def __init__(self, target, key_error):
        self.target = target
        self.key_error = key_error

    def invoke(self, key):
        raise_micro_op_conflicts(self, [ key ], get_manager(self.target).get_flattened_micro_op_types())

        for other_micro_op_type in get_manager(self.target).get_flattened_micro_op_types():
            other_micro_op_type.unbind(key, self.target)

        del self.target.wrapped[key]


class DictDeletterType(DictMicroOpType):
    def __init__(self, key, key_error):
        self.key = key
        self.key_error = key_error

    def create(self, target, through_type):
        return DictDeletter(target, self.key, self.key_error)

    def bind(self, key, target):
        pass

    def unbind(self, key, target):
        pass

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type, other_micro_op_types):
        if isinstance(other_micro_op_type, (DictGetterType, DictWildcardGetterType)):
            other_key, _ = get_key_and_type(other_micro_op_type)
            if other_key is not WILDCARD and other_key != self.key:
                return False
            if not other_micro_op_type.key_error:
                return True
        return False

    def raise_on_micro_op_conflict(self, other_micro_op, args):
        pass

    def check_for_data_conflict(self, obj):
        return False

    def merge(self, other_micro_op_type):
        return DictDeletterType(self.key, self.key_error or other_micro_op_type.key_error)


class DictDeletter(MicroOp):
    def __init__(self, target, key, key_error):
        self.target = target
        self.key = key
        self.key_error = key_error

    def invoke(self):
        raise_micro_op_conflicts(self, [ ], get_manager(self.target).get_flattened_micro_op_types())

        for other_micro_op_type in get_manager(self.target).get_flattened_micro_op_types():
            other_micro_op_type.unbind(self.key, self.target)

        del self.target.wrapped[self.key]


class RDHDictType(CompositeType):
    def __init__(self, wildcard_type=None):
        micro_ops = {}

        if wildcard_type:
            micro_ops[("get-wildcard",)] = DictWildcardGetterType(wildcard_type, True, False)
            micro_ops[("set-wildcard",)] = DictWildcardSetterType(wildcard_type, True, True)
            micro_ops[("delete-wildcard",)] = DictWildcardDeletterType(True)

        super(RDHDictType, self).__init__(micro_ops)


class RDHDict(Composite, DictMixin, object):
    def __init__(self, initial_data):
        self.wrapped = dict(initial_data)
        if isinstance(self.wrapped, RDHDict):
            raise FatalError()

    def __getitem__(self, key, raw=False):
        if self is self.wrapped:
            raise FatalError()
        if raw:
            return self.wrapped.__getitem__(key)

        try:
            manager = get_manager(self)
            default_type = manager.default_type
            if default_type is None:
                raise KeyError()
            micro_op_type = manager.get_micro_op_type(default_type, ("get", key))
            if micro_op_type is not None:
                micro_op = micro_op_type.create(self, default_type)
                return micro_op.invoke()
            else:
                micro_op_type = manager.get_micro_op_type(default_type, ("get-wildcard",))

                if micro_op_type is None:
                    raise MissingMicroOp()

                micro_op = micro_op_type.create(self, default_type)
                return micro_op.invoke(key)
        except InvalidDereferenceKey:
            raise KeyError()

    def __setitem__(self, key, value, raw=False):
        if self is self.wrapped:
            raise FatalError()
        if raw:
            return self.wrapped.__setitem__(key, value)

        try:
            manager = get_manager(self)
            default_type = manager.default_type
            if default_type is None:
                raise KeyError()
            micro_op_type = manager.get_micro_op_type(default_type, ("set", key))
            if micro_op_type is not None:
                micro_op = micro_op_type.create(self, default_type)
                micro_op.invoke(value)
            else:
                micro_op_type = manager.get_micro_op_type(default_type, ("set-wildcard",))

                if micro_op_type is None:
                    raise MissingMicroOp()

                micro_op = micro_op_type.create(self, default_type)
                micro_op.invoke(key, value)
        except InvalidAssignmentKey:
            raise KeyError()

    def __delitem__(self, key, raw=False):
        if self is self.wrapped:
            raise FatalError()
        if raw:
            return self.wrapped.__delitem__(key)

        manager = get_manager(self)
        default_type = manager.default_type
        if default_type is None:
            raise IndexError()
        micro_op_type = manager.get_micro_op_type(default_type, ("delete", key))
        if micro_op_type is not None:
            micro_op = micro_op_type.create(self, default_type)
            return micro_op.invoke()
        else:
            micro_op_type = manager.get_micro_op_type(default_type, ("delete-wildcard",))

            if micro_op_type is None:
                raise MissingMicroOp()

            micro_op = micro_op_type.create(self, default_type)
            return micro_op.invoke(key)

    def keys(self):
        if self is self.wrapped:
            raise FatalError()
        return self.wrapped.keys()
