from rdhlang5_types.exceptions import MicroOpConflict

from rdhlang5_types.composites import get_type_of_value, get_manager, bind_type_to_value, \
    CompositeType
from rdhlang5_types.core_types import merge_types, OneOfType, AnyType
from rdhlang5_types.micro_ops import MicroOpType, MicroOp, check_micro_op_conflicts


class InvalidDereference(object):
    pass


class CompositeObjectWildcardGetterType(MicroOpType):

    def __init__(self, type, can_fail):
        self.type = type
        self.can_fail = can_fail

    def create(self, target):
        return CompositeObjectWildcardGetter(target, self.type)

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type):
        if self.can_fail or other_micro_op_type.can_fail:
            return False
        if isinstance(other_micro_op_type, (CompositeObjectSetterType, CompositeObjectWildcardSetterType)):
            if not self.type.is_copyable_from(other_micro_op_type.type):
                return True
        if isinstance(other_micro_op_type, (CompositeObjectDeletterType, CompositeObjectWildcardDeletterType)):
            return True
        return False

    def check_for_micro_op_conflict(self, other_micro_op, args):
        if self.can_fail:
            return False
        if isinstance(other_micro_op, (CompositeObjectSetter, CompositeObjectWildcardSetter)):
            if isinstance(other_micro_op, CompositeObjectSetter):
                new_value = args[0]
            else:
                new_value = args[1]
            if not self.type.is_copyable_from(get_type_of_value(new_value)):
                return True

        if isinstance(other_micro_op, (CompositeObjectDeletter, CompositeObjectWildcardDeletter)):
            return True
        return False

    def check_for_data_conflict(self, obj):
        return True

    def merge(self, other_micro_op_type):
        return CompositeObjectWildcardGetterType(merge_types([ self.type, other_micro_op_type.type ]), self.can_fail or other_micro_op_type.can_fail)


class CompositeObjectWildcardGetter(MicroOp):

    def __init__(self, target, type):
        self.target = target
        self.type = type

    def invoke(self, key):
        try:
            check_micro_op_conflicts(self, [ key ], get_manager(self.target).get_flattened_micro_op_types())
        except MicroOpConflict:
            raise InvalidDereference()

        value = self.target.__dict__[key]
        type_of_value = get_type_of_value(value)
        if not self.type.is_copyable_from(type_of_value):
            raise InvalidDereference()
        return value

    def bind_to_in_place_value(self):
        for value in self.target.__dict__.values():
            bind_type_to_value(self.type, value)


class CompositeObjectGetterType(MicroOpType):

    def __init__(self, key, type, can_fail):
        self.key = key
        self.type = type
        self.can_fail = can_fail

    def create(self, target):
        return CompositeObjectGetter(target, self.key, self.type)

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type):
        if (isinstance(other_micro_op_type, (CompositeObjectGetterType, CompositeObjectSetterType, CompositeObjectDeletterType))
            and self.key != other_micro_op_type.key
        ):
            return False
        if self.can_fail or other_micro_op_type.can_fail:
            return False
        if isinstance(other_micro_op_type, (CompositeObjectSetterType, CompositeObjectWildcardSetterType)):
            if not self.type.is_copyable_from(other_micro_op_type.type):
                return True
        if isinstance(other_micro_op_type, (CompositeObjectDeletterType, CompositeObjectWildcardDeletterType)):
            return True
        return False

    def check_for_micro_op_conflict(self, other_micro_op, args):
        if self.can_fail:
            return False
        if (isinstance(other_micro_op, (CompositeObjectSetter, CompositeObjectDeletter))
            and self.key != other_micro_op.key
        ):
            return False
        if (isinstance(other_micro_op, (CompositeObjectWildcardSetter, CompositeObjectWildcardDeletter))
            and self.key != args[0]
        ):
            return False

        if isinstance(other_micro_op, (CompositeObjectSetter, CompositeObjectWildcardSetter)):
            if isinstance(other_micro_op, CompositeObjectSetter):
                new_value = args[0]
            else:
                new_value = args[1]
            if not self.type.is_copyable_from(get_type_of_value(new_value)):
                return True
        if isinstance(other_micro_op, (CompositeObjectDeletter, CompositeObjectWildcardDeletter)):
            return True
        return False

    def check_for_data_conflict(self, obj):
        missing = object()
        value = obj.__dict__.get(self.key, missing)
        if value is missing:
            return True
        type_of_value = get_type_of_value(value)
        return not self.type.is_copyable_from(type_of_value)

    def merge(self, other_micro_op_type):
        return CompositeObjectGetterType(self.key, merge_types([ self.type, other_micro_op_type.type ]), self.can_fail or other_micro_op_type.can_fail)


class CompositeObjectGetter(MicroOp):

    def __init__(self, target, key, type):
        self.target = target
        self.key = key
        self.type = type

    def invoke(self):
        try:
            check_micro_op_conflicts(self, [], get_manager(self.target).get_flattened_micro_op_types())
        except MicroOpConflict:
            raise InvalidDereference()

        value = self.target.__dict__[self.key]
        type_of_value = get_type_of_value(value)
        if not self.type.is_copyable_from(type_of_value):
            raise InvalidDereference()
        return value

    def bind_to_in_place_value(self):
        current_value = self.target.__dict__[self.key]
        bind_type_to_value(self.type, current_value)


class InvalidAssignment(Exception):
    pass


class CompositeObjectWildcardSetterType(MicroOpType):

    def __init__(self, type, can_fail):
        self.type = type
        self.can_fail = can_fail

    def create(self, target):
        return CompositeObjectWildcardSetter(target, self.type)

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type):
        if self.can_fail or other_micro_op_type.can_fail:
            return False
        if isinstance(other_micro_op_type, (CompositeObjectGetterType, CompositeObjectWildcardGetterType)):
            if not other_micro_op_type.type.is_copyable_from(self.type):
                return True
        return False

    def check_for_micro_op_conflict(self, other_micro_op, args):
        return False

    def check_for_data_conflict(self, obj):
        return False

    def merge(self, other_micro_op_type):
        return CompositeObjectWildcardSetterType(merge_types([ self.type, other_micro_op_type.type ]), self.can_fail or other_micro_op_type.can_fail)


class CompositeObjectWildcardSetter(MicroOp):

    def __init__(self, target, type):
        self.target = target
        self.type = type

    def invoke(self, key, new_value):
        try:
            check_micro_op_conflicts(self, [ key, new_value ], get_manager(self.target).get_flattened_micro_op_types())
        except MicroOpConflict:
            raise InvalidAssignment()

        new_value_type = get_type_of_value(new_value)
        if not self.type.is_copyable_from(new_value_type):
            raise InvalidAssignment()
        self.target.__dict__[key] = new_value

        bind_type_to_value(self.type, new_value)

    def bind_to_in_place_value(self):
        pass


class CompositeObjectSetterType(MicroOpType):

    def __init__(self, key, type, can_fail):
        self.key = key
        self.type = type
        self.can_fail = can_fail

    def create(self, target):
        return CompositeObjectSetter(target, self.key, self.type)

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type):
        if (isinstance(other_micro_op_type, (CompositeObjectGetterType, CompositeObjectSetterType, CompositeObjectDeletterType))
            and self.key != other_micro_op_type.key
        ):
            return False
        if self.can_fail or other_micro_op_type.can_fail:
            return False
        if isinstance(other_micro_op_type, (CompositeObjectGetterType, CompositeObjectWildcardGetterType)):
            if not other_micro_op_type.type.is_copyable_from(self.type):
                return True
        return False

    def check_for_micro_op_conflict(self, other_micro_op, args):
        return False

    def check_for_data_conflict(self, obj):
        return False

    def merge(self, other_micro_op_type):
        return CompositeObjectSetterType(self.key, merge_types([ self.type, other_micro_op_type.type ]), self.can_fail or other_micro_op_type.can_fail)


class CompositeObjectSetter(MicroOp):

    def __init__(self, target, key, type):
        self.target = target
        self.key = key
        self.type = type

    def invoke(self, new_value):
        try:
            check_micro_op_conflicts(self, [ new_value ], get_manager(self.target).get_flattened_micro_op_types())
        except MicroOpConflict:
            raise InvalidAssignment()

        new_value_type = get_type_of_value(new_value)
        if not self.type.is_copyable_from(new_value_type):
            raise InvalidAssignment()
        self.target.__dict__[self.key] = new_value

        bind_type_to_value(self.type, new_value)

    def bind_to_in_place_value(self):
        pass


class InvalidDeletion(Exception):
    pass


class CompositeObjectWildcardDeletterType(MicroOpType):

    def __init__(self, can_fail):
        self.can_fail = can_fail

    def create(self, target):
        return CompositeObjectWildcardDeletter(target)

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type):
        if self.can_fail or other_micro_op_type.can_fail:
            return False
        if isinstance(other_micro_op_type, (CompositeObjectGetterType, CompositeObjectWildcardGetterType)):
            return True
        return False

    def check_for_micro_op_conflict(self, other_micro_op, args):
        return False

    def check_for_data_conflict(self, obj):
        return False

    def bind_to_in_place_value(self):
        pass

    def merge(self, other_micro_op_type):
        return CompositeObjectWildcardDeletterType(self.can_fail or other_micro_op_type.can_fail)


class CompositeObjectWildcardDeletter(MicroOp):

    def __init__(self, target):
        self.target = target

    def invoke(self, key):
        try:
            check_micro_op_conflicts(self, [ key ], get_manager(self.target).get_flattened_micro_op_types())
        except MicroOpConflict:
            raise InvalidDeletion()

        del self.target.__dict__[key]

    def bind_to_in_place_value(self):
        pass


class CompositeObjectDeletterType(MicroOpType):

    def __init__(self, key, can_fail):
        self.key = key
        self.can_fail = can_fail

    def create(self, target):
        return CompositeObjectDeletter(target, self.key)

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type):
        if (isinstance(other_micro_op_type, (CompositeObjectGetterType, CompositeObjectSetterType, CompositeObjectDeletterType))
            and self.key != other_micro_op_type.key
        ):
            return False
        if self.can_fail or other_micro_op_type.can_fail:
            return False
        if isinstance(other_micro_op_type, (CompositeObjectGetterType, CompositeObjectWildcardGetterType)):
            return True
        return False

    def check_for_micro_op_conflict(self, other_micro_op, args):
        return False

    def check_for_data_conflict(self, obj):
        return False

    def merge(self, other_micro_op_type):
        return CompositeObjectDeletterType(self.key, self.can_fail or other_micro_op_type.can_fail)


class CompositeObjectDeletter(MicroOp):

    def __init__(self, target, key):
        self.target = target
        self.key = key

    def invoke(self):
        try:
            check_micro_op_conflicts(self, [], get_manager(self.target).get_flattened_micro_op_types())
        except MicroOpConflict:
            raise InvalidDeletion()

        del self.target.__dict__[self.key]

    def bind_to_in_place_value(self):
        pass


class Const(object):

    def __init__(self, wrapped):
        self.wrapped = wrapped


class ObjectType(CompositeType):
    def __init__(self, properties):
        micro_ops = {}
        for name, type in properties.items():
            const = False
            if isinstance(type, Const):
                const = True
                type = type.wrapped

            micro_ops[("get", name)] = CompositeObjectGetterType(name, type, False)
            if not const:
                micro_ops[("set", name)] = CompositeObjectSetterType(name, type, False)

        no_properties_object_type = None
        if len(properties) == 0:
            no_properties_object_type = self
        else:
            no_properties_object_type = ObjectType({})

        micro_ops[("get-wildcard",)] = CompositeObjectWildcardGetterType(OneOfType([ no_properties_object_type, AnyType() ]), True)
        micro_ops[("set-wildcard",)] = CompositeObjectWildcardSetterType(OneOfType([ no_properties_object_type, AnyType() ]), True)

        super(ObjectType, self).__init__(micro_ops)


class PythonObjectType(CompositeType):
    def __init__(self):
        micro_ops = {}

        micro_ops[("get-wildcard",)] = CompositeObjectWildcardGetterType(OneOfType([ self, AnyType() ]), True)
        micro_ops[("set-wildcard",)] = CompositeObjectWildcardSetterType(OneOfType([ self, AnyType() ]), False)
        micro_ops[("delete-wildcard",)] = CompositeObjectWildcardDeletterType(True)

        super(PythonObjectType, self).__init__(micro_ops)

