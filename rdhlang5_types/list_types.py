from rdhlang5_types.composites import get_type_of_value, get_manager, \
    bind_type_to_value, CompositeType, DefaultFactoryType
from rdhlang5_types.core_types import Const, merge_types
from rdhlang5_types.exceptions import FatalError, MicroOpConflict, raise_if_safe, \
    MicroOpTypeConflict, InvalidAssignmentType, InvalidDereferenceKey, \
    InvalidDereferenceType, InvalidAssignmentKey
from rdhlang5_types.micro_ops import MicroOpType, MicroOp, \
    raise_micro_op_conflicts

WILDCARD = object()
MISSING = object()


def get_key_and_type(micro_op_type):
    if isinstance(micro_op_type, (ListWildcardGetterType, ListWildcardSetterType, ListWildcardDeletterType, ListWildcardInsertType)):
        key = WILDCARD
    elif isinstance(micro_op_type, (ListGetterType, ListSetterType, ListDeletterType, ListInsertType)):
        key = micro_op_type.key
    else:
        raise FatalError()

    if isinstance(micro_op_type, (ListWildcardGetterType, ListGetterType, ListWildcardSetterType, ListSetterType, ListWildcardInsertType, ListInsertType)):
        type = micro_op_type.type
    else:
        type = MISSING

    return key, type


def get_key_and_new_value(micro_op, args):
    if isinstance(micro_op, (ListWildcardGetter, ListWildcardDeletter)):
        key, new_value = MISSING
    elif isinstance(micro_op, (ListGetter, ListDeletter)):
        key = micro_op.key
        new_value = MISSING
    elif isinstance(micro_op, (ListWildcardSetter, ListWildcardInsert)):
        key, new_value = args
    elif isinstance(micro_op, (ListSetter, ListInsert)):
        key = micro_op.key
        new_value = args[0]
    else:
        raise FatalError()
    return key, new_value


class ListWildcardGetterType(MicroOpType):
    def __init__(self, type, key_error, type_error):
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def create(self, target, through_type):
        return ListWildcardGetter(target, through_type, self.type, self.key_error, self.type_error)

    def bind(self, key, target):
        pass

    def unbind(self, key, target):
        pass

    def check_for_conflicts_with_existing_micro_ops(self, obj, micro_op_types):
        default_factories = [ o for o in micro_op_types if isinstance(o, DefaultFactoryType)]
        has_default_factory = len(default_factories) > 0

        if not self.key_error:
            if not has_default_factory:
                raise MicroOpTypeConflict()
            default_factory = default_factories[0]
            if not self.type.is_copyable_from(default_factory.type):
                raise MicroOpTypeConflict()

        return super(ListWildcardGetterType, self).check_for_conflicts_with_existing_micro_ops(obj, micro_op_types)

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type, other_micro_op_types):
        if not self.key_error:
            if not any(isinstance(o, DefaultFactoryType) for o in other_micro_op_types):
                return True

        if isinstance(other_micro_op_type, (ListGetterType, ListWildcardGetterType)):
            return False
        if isinstance(other_micro_op_type, (ListSetterType, ListWildcardSetterType)):
            if not self.type_error and not self.type.is_copyable_from(other_micro_op_type.type):
                return True
        if isinstance(other_micro_op_type, (ListDeletterType, ListWildcardDeletterType)):
            has_default_factory = any(isinstance(o, DefaultFactoryType) for o in other_micro_op_types)
            if not self.key_error and not has_default_factory:
                return True
        if isinstance(other_micro_op_type, (ListInsertType, ListWildcardInsertType)):
            if not self.type_error and not self.type.is_copyable_from(other_micro_op_type.type):
                return True
        return False

    def raise_on_micro_op_conflict(self, other_micro_op, args):
        if isinstance(other_micro_op, (ListSetter, ListWildcardSetter)):
            _, other_new_value = get_key_and_new_value(other_micro_op, args)
            if not self.type_error and not self.type.is_copyable_from(get_type_of_value(other_new_value)):
                raise_if_safe(InvalidAssignmentType, other_micro_op.type_error)
        if isinstance(other_micro_op, (ListInsert, ListWildcardInsert)):
            _, other_new_value = get_key_and_new_value(other_micro_op, args)
            if not self.type_error and not self.type.is_copyable_from(get_type_of_value(other_new_value)):
                raise_if_safe(InvalidAssignmentType, other_micro_op.type_error)

        if isinstance(other_micro_op, (ListDeletter, ListWildcardDeletter)):
            if not self.key_error:
                raise_if_safe(InvalidAssignmentType, other_micro_op.key_error)
        return False

    def check_for_data_conflict(self, obj):
        if not self.key_error and get_manager(obj).default_factory is None:
            return True

        if not self.type_error:
            for value in obj.wrapped:
                if not self.type.is_copyable_from(get_type_of_value(value)):
                    return True

        return False

    def merge(self, other_micro_op_type):
        return ListWildcardGetterType(
            merge_types([ self.type, other_micro_op_type.type ]),
            self.key_error or other_micro_op_type.key_error,
            self.type_error or other_micro_op_type.type_error
        )


class ListWildcardGetter(MicroOp):
    def __init__(self, target, through_type, type, key_error, type_error):
        self.target = target
        self.through_type = through_type
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def invoke(self, key):
        raise_micro_op_conflicts(self, [ key ], get_manager(self.target).get_flattened_micro_op_types())

        if key >= 0 and key < len(self.target):
            value = self.target.__getitem__(key, raw=True)
        else:
            default_factory_op_type = get_manager(self.target).get_micro_op_type(self.through_type, ("default-factory",))

            if not default_factory_op_type:
                raise_if_safe(InvalidDereferenceKey, self.key_error)

            default_factory_op = default_factory_op_type.create(self.target, self.through_type)
            value = default_factory_op.invoke(key)

        type_of_value = get_type_of_value(value)
        if not self.type.is_copyable_from(type_of_value):
            raise raise_if_safe(InvalidDereferenceType, self.type_error)
        return value


class ListGetterType(MicroOpType):
    def __init__(self, key, type, key_error, type_error):
        self.key = key
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def create(self, target, through_type):
        return ListGetter(target, through_type, self.key, self.type, self.key_error, self.type_error)

    def bind(self, key, target):
        pass

    def unbind(self, key, target):
        pass

    def check_for_conflicts_with_existing_micro_ops(self, obj, micro_op_types):
        default_factories = [ isinstance(o, DefaultFactoryType) for o in micro_op_types ]
        has_default_factory = len(default_factories) > 0
        has_value_in_place = self.key >= 0 and self.key < len(obj)

        if not self.key_error and not has_value_in_place:
            if not has_default_factory:
                raise MicroOpTypeConflict()
            default_factory = default_factories[0]
            if not self.type.is_copyable_from(default_factory.type):
                raise MicroOpTypeConflict()

        return super(ListGetterType, self).check_for_conflicts_with_existing_micro_ops(obj, micro_op_types)

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type, other_micro_op_types):
        if isinstance(other_micro_op_type, (ListSetterType, ListWildcardSetterType)):
            other_key, other_type = get_key_and_type(other_micro_op_type)
            if other_key is not WILDCARD and self.key != other_key:
                return False
            if not self.type_error and not other_micro_op_type.type_error and not self.type.is_copyable_from(other_type):
                return True
        if isinstance(other_micro_op_type, (ListInsertType, ListWildcardInsertType)):
            other_key, other_type = get_key_and_type(other_micro_op_type)
            if other_key is not WILDCARD:
                if self.key < other_key:
                    return False
                elif self.key == other_key:
                    return not self.type.is_copyable_from(other_type)
                else:
                    return True  # Here, we should check for other microops at key - 1
            else:
                other_key, other_type = get_key_and_type(other_micro_op_type)
                if not self.type.is_copyable_from(other_type):
                    return True
        if isinstance(other_micro_op_type, (ListDeletterType, ListWildcardDeletterType)):
            other_key, _ = get_key_and_type(other_micro_op_type)
            if other_key is not WILDCARD and self.key < other_key:
                return False

            return True  # Be more liberal: check for default factories and microops at key - 1
        return False

    def raise_on_micro_op_conflict(self, other_micro_op, args):
        if isinstance(other_micro_op, (ListGetter, ListWildcardGetter)):
            return
        if isinstance(other_micro_op, (ListSetter, ListWildcardSetter)):
            other_key, new_value = get_key_and_new_value(other_micro_op, args)
            if self.key != other_key:
                return
            if not self.type_error and not self.type.is_copyable_from(get_type_of_value(new_value)):
                raise_if_safe(InvalidAssignmentType, other_micro_op.type_error)
        if isinstance(other_micro_op, (ListInsert, ListWildcardInsert)):
            other_key, _ = get_key_and_new_value(other_micro_op, args)
            if self.key < other_key:
                return
            else:
                new_value = other_micro_op.target[self.key - 1]
                if not self.type.is_copyable_from(get_type_of_value(new_value)):
                    raise raise_if_safe(InvalidAssignmentType, other_micro_op.type_error)
        if isinstance(other_micro_op, (ListDeletter, ListWildcardDeletter)):
            other_key, _ = get_key_and_new_value(other_micro_op, args)
            if self.key < other_key:
                return
            else:
                new_value = other_micro_op.target[self.key + 1]
                if not self.key_error and not self.type.is_copyable_from(get_type_of_value(new_value)):
                    raise_if_safe(InvalidAssignmentType, other_micro_op.type_error)

    def check_for_data_conflict(self, obj):
        if self.key < 0 or self.key > len(obj):
            return True
        value = obj.__getitem__(self.key, raw=True)
        type_of_value = get_type_of_value(value)
        return not self.type.is_copyable_from(type_of_value)

    def merge(self, other_micro_op_type):
        return ListGetterType(
            self.key,
            merge_types([ self.type, other_micro_op_type.type ]),
            self.key_error or other_micro_op_type.key_error,
            self.type_error or other_micro_op_type.type_error
        )


class ListGetter(MicroOp):
    def __init__(self, target, through_type, key, type, key_error, type_error):
        self.target = target
        self.through_type = through_type
        self.key = key
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def invoke(self):
        raise_micro_op_conflicts(self, [], get_manager(self.target).get_flattened_micro_op_types())

        if self.key >= 0 and self.key < len(self.target):
            value = self.target.__getitem__(self.key, raw=True)
        else:
            default_factory_op = get_manager(self.target).get_micro_op_type(self.through_type, ("default-factory",))

            if default_factory_op:
                value = default_factory_op.invoke(self.key)
            else:
                raise_if_safe(InvalidDereferenceKey, self.key_error)

        type_of_value = get_type_of_value(value)

        if not self.type.is_copyable_from(type_of_value):
            raise_if_safe(InvalidDereferenceKey, self.can_fail)

        return value


class ListWildcardSetterType(MicroOpType):
    def __init__(self, type, key_error, type_error):
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def create(self, target, through_type):
        return ListWildcardSetter(target, self.type, self.key_error, self.type_error)

    def bind(self, key, target):
        pass

    def unbind(self, key, target):
        pass

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type, other_micro_op_types):
        if isinstance(other_micro_op_type, (ListGetterType, ListWildcardGetterType)):
            if not self.type_error and not other_micro_op_type.type_error and not other_micro_op_type.type.is_copyable_from(self.type):
                return True

        return False

    def raise_on_micro_op_conflict(self, other_micro_op, args):
        pass

    def check_for_data_conflict(self, obj):
        return False

    def merge(self, other_micro_op_type):
        return ListWildcardSetterType(
            merge_types([ self.type, other_micro_op_type.type ]),
            self.key_error or other_micro_op_type.key_error,
            self.type_error or other_micro_op_type.type_error
        )


class ListWildcardSetter(MicroOp):
    def __init__(self, target, type, key_error, type_error):
        self.target = target
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def invoke(self, key, new_value):
        raise_micro_op_conflicts(self, [ key, new_value ], get_manager(self.target).get_flattened_micro_op_types())

        new_value_type = get_type_of_value(new_value)
        if not self.type.is_copyable_from(new_value_type):
            raise FatalError()

        if key < 0 or key > len(self.target):
            raise raise_if_safe(InvalidAssignmentKey, self.can_fail)

        for other_micro_op_type in get_manager(self.target).get_flattened_micro_op_types():
            other_micro_op_type.unbind(key, self.target)

        self.target.__setitem__(key, new_value, raw=True)

        for other_micro_op_type in get_manager(self.target).get_flattened_micro_op_types():
            other_micro_op_type.bind(key, self.target)

class ListSetterType(MicroOpType):
    def __init__(self, key, type, key_error, type_error):
        self.key = key
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def create(self, target, through_type):
        return ListSetter(target, self.key, self.type, self.key_error, self.type_error)

    def bind(self, key, target):
        pass

    def unbind(self, key, target):
        pass

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type, other_micro_op_types):
        if isinstance(other_micro_op_type, (ListGetterType, ListWildcardGetterType)):
            other_key, other_type = get_key_and_type(other_micro_op_type)
            if other_key is not WILDCARD and self.key != other_key:
                return False
            if not self.type_error and not other_micro_op_type.type_error and not other_type.is_copyable_from(self.type):
                return True
        return False

    def raise_on_micro_op_conflict(self, other_micro_op, args):
        pass

    def check_for_data_conflict(self, obj):
        return False

    def merge(self, other_micro_op_type):
        return ListSetterType(
            self.key,
            merge_types([ self.type, other_micro_op_type.type ]),
            self.key_error or other_micro_op_type.key_error,
            self.type_error or other_micro_op_type.type_error
        )


class ListSetter(MicroOp):
    def __init__(self, target, key, type, key_error, type_error):
        self.target = target
        self.key = key
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def invoke(self, new_value):
        raise_micro_op_conflicts(self, [ new_value ], get_manager(self.target).get_flattened_micro_op_types())

        new_value_type = get_type_of_value(new_value)
        if not self.type.is_copyable_from(new_value_type):
            raise FatalError()

        if self.key < 0 or self.key > len(self.target):
            raise_if_safe(InvalidAssignmentKey, self.can_fail)

        for other_micro_op_type in get_manager(self.target).get_flattened_micro_op_types():
            other_micro_op_type.unbind(self.key, self.target)

        self.target.__setitem__(self.key, new_value, raw=True)

        for other_micro_op_type in get_manager(self.target).get_flattened_micro_op_types():
            other_micro_op_type.bind(self.key, self.target)


class ListWildcardDeletterType(MicroOpType):
    def __init__(self, key_error):
        self.key_error = key_error

    def bind(self, key, target):
        pass

    def unbind(self, key, target):
        pass

    def create(self, target, through_type):
        return ListWildcardDeletter(target, self.key_error)

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type, other_micro_op_types):
        if isinstance(other_micro_op_type, (ListGetterType, ListWildcardGetterType)):
            default_factories = [ o for o in other_micro_op_types if isinstance(o, DefaultFactoryType)]
            has_default_factory = len(default_factories) > 0

            if not other_micro_op_type.key_error and not has_default_factory:
                return True
        return False

    def raise_on_micro_op_conflict(self, other_micro_op, args):
        return False

    def check_for_data_conflict(self, obj):
        return False

    def merge(self, other_micro_op_type):
        return ListWildcardDeletterType(
            self.key_error or other_micro_op_type.key_error
        )


class ListWildcardDeletter(MicroOp):
    def __init__(self, target, key_error):
        self.target = target
        self.key_error = key_error

    def invoke(self, key):
        raise_micro_op_conflicts(self, [ key ], get_manager(self.target).get_flattened_micro_op_types())

        for other_micro_op_type in get_manager(self.target).get_flattened_micro_op_types():
            other_micro_op_type.unbind(self.key, self.target)

        self.target.__delitem__(raw=True)


class ListDeletterType(MicroOpType):
    def __init__(self, key, key_error):
        self.key = key
        self.key_error = key_error

    def create(self, target, through_type):
        return ListDeletter(target, self.key, self.key_error)

    def bind(self, key, target):
        pass

    def unbind(self, key, target):
        pass

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type, other_micro_op_types):
        if isinstance(other_micro_op_type, (ListGetterType, ListWildcardGetterType)):
            other_key, _ = get_key_and_type(other_micro_op_type)
            if other_key is not WILDCARD and self.key > other_key:
                return False
            if not self.key_error and not other_micro_op_type.key_error:
                return True
        return False

    def raise_on_micro_op_conflict(self, other_micro_op, args):
        pass

    def check_for_data_conflict(self, obj):
        return False

    def merge(self, other_micro_op_type):
        return ListDeletter(self.key, self.can_fail or other_micro_op_type.can_fail)


class ListDeletter(MicroOp):
    def __init__(self, target, key):
        self.target = target
        self.key = key

    def invoke(self):
        raise_micro_op_conflicts(self, [ ], get_manager(self.target).get_flattened_micro_op_types())

        for other_micro_op_type in get_manager(self.target).get_flattened_micro_op_types():
            other_micro_op_type.unbind(self.key, self.target)

        self.target.__delitem__(self.key, raw=True)


class ListWildcardInsertType(MicroOpType):
    def __init__(self, type, key_error, type_error):
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def create(self, target, through_type):
        return ListWildcardInsert(target, self.type, self.key_error, self.type_error)

    def bind(self, key, target):
        pass

    def unbind(self, key, target):
        pass

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type, other_micro_op_types):
        if isinstance(other_micro_op_type, (ListGetterType, ListWildcardGetterType)):
            _, other_type = get_key_and_type(other_micro_op_type)
            return not other_type.is_copyable_from(self.type)

        return False

    def raise_on_micro_op_conflict(self, other_micro_op, args):
        return False

    def check_for_data_conflict(self, obj):
        return False


class ListWildcardInsert(MicroOp):
    def __init__(self, target, type, key_error, type_error):
        self.target = target
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def invoke(self, key, new_value):
        raise_micro_op_conflicts(self, [ key, new_value ], get_manager(self.target).get_flattened_micro_op_types())

        new_value_type = get_type_of_value(new_value)
        if not self.type.is_copyable_from(new_value_type):
            raise FatalError()

        for other_micro_op_type in get_manager(self.target).get_flattened_micro_op_types():
            for after_key in range(key, len(self.target)):
                other_micro_op_type.unbind(after_key, self.target)

        self.target.insert(key, new_value, raw=True)

        for other_micro_op_type in get_manager(self.target).get_flattened_micro_op_types():
            for after_key in range(key, len(self.target)):
                other_micro_op_type.bind(after_key, self.target)


class ListInsertType(MicroOpType):
    def __init__(self, type, key, key_error, type_error):
        self.type = type
        self.key = key
        self.key_error = key_error
        self.type_error = type_error

    def create(self, target, through_type):
        return ListInsert(target, self.key, self.type, self.key_error, self.type_error)

    def bind(self, key, target):
        pass

    def unbind(self, key, target):
        pass

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type, other_micro_op_types):
        if isinstance(other_micro_op_type, (ListGetterType, ListWildcardGetterType)):
            other_key, other_type = get_key_and_type(other_micro_op_type)
            if other_key is not WILDCARD and self.key > other_key:
                return False
            else:
                return not other_type.is_copyable_from(self.type)

        return False

    def raise_on_micro_op_conflict(self, other_micro_op, args):
        return False

    def check_for_data_conflict(self, obj):
        return False


class ListInsert(MicroOp):
    def __init__(self, target, key, type, key_error, type_error):
        self.target = target
        self.key = key
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def invoke(self, new_value):
        raise_micro_op_conflicts(self, [ new_value ], get_manager(self.target).get_flattened_micro_op_types())

        new_value_type = get_type_of_value(new_value)
        if not self.type.is_copyable_from(new_value_type):
            raise FatalError()

        for other_micro_op_type in get_manager(self.target).get_flattened_micro_op_types():
            for after_key in range(self.key, len(self.target)):
                other_micro_op_type.unbind(after_key, self.target)

        self.target.insert(self.key, new_value, raw=True)

        for other_micro_op_type in get_manager(self.target).get_flattened_micro_op_types():
            for after_key in range(self.key, len(self.target)):
                other_micro_op_type.bind(after_key, self.target)


class ListType(CompositeType):
    def __init__(self, element_types, wildcard_type, allow_push=True, allow_wildcard_insert=True, allow_delete=True):
        micro_ops = {}

        for index, element_type in enumerate(element_types):
            const = False
            if isinstance(element_type, Const):
                const = True
                element_type = element_type.wrapped

            micro_ops[("get", index)] = ListGetterType(index, element_type, False, False)
            if not const:
                micro_ops[("set", index)] = ListSetterType(index, element_type, False, False)

        if wildcard_type:
            const = False
            if isinstance(wildcard_type, Const):
                const = True
                wildcard_type = wildcard_type.wrapped

            micro_ops[("get-wildcard",)] = ListWildcardGetterType(wildcard_type, True, False)
            if not const:
                micro_ops[("set-wildcard",)] = ListWildcardSetterType(wildcard_type, True, False)
            if allow_push:
                micro_ops[("insert", 0)] = ListInsertType(wildcard_type, 0, False, False)
            if allow_delete:
                micro_ops[("delete-wildcard",)] = ListWildcardDeletterType(True)
            if allow_wildcard_insert:
                micro_ops[("insert-wildcard",)] = ListWildcardInsertType(wildcard_type, True, False)

        super(ListType, self).__init__(micro_ops)
