from _collections import defaultdict
from virtualenv.config.convert import get_type
import weakref

MISSING = object()


class FatalError(Exception):
    pass


class MicroOpType(object):
    def create(self, target):
        raise NotImplementedError()

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type):
        raise NotImplementedError()

    def check_for_micro_op_conflict(self, micro_op, args):
        raise NotImplementedError()

    def check_for_data_conflict(self, obj):
        raise NotImplementedError()     

    def merge(self, other_micro_op_type):
        raise NotImplementedError()


def check_micro_op_conflicts(micro_op, args, other_micro_op_types):
    for other_micro_op_type in other_micro_op_types:
        if other_micro_op_type.check_for_micro_op_conflict(micro_op, args):
            if micro_op.can_fail:
                raise MicroOpConflict()
            else:
                raise FatalError()


class MicroOp(object):

    def invoke(self, *args):
        raise NotImplementedError()

    def bind_to_in_place_value(self):
        raise NotImplementedError()


def bind_type_to_value(type, value):
    if not isinstance(value, CompositeObject):
        return
    for sub_type in unwrap_types(type):
        if isinstance(sub_type, CompositeType):
            try:
                get_manager(value).add_composite_type(sub_type)
            except MicroOpTypeConflict:
                pass


class OperableObjectManager(object):

    def __init__(self, obj):
        self.obj = obj
        self.micro_op_types = defaultdict(dict)
        self.default_type = None

    def flatten_tag(self, tag):
        raise NotImplementedError()

    def get_micro_op_type(self, type, tag):
        if type is None:
            type = self.default_type
        return self.micro_op_types.get(id(type)).get(tag, None)

    def add_micro_op_tag(self, tag, *args):
        raise NotImplementedError()


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
        value = obj.__dict__.get(self.key, MISSING)
        if value is MISSING:
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


class MicroOpTypeConflict(Exception):
    pass


class MicroOpConflict(Exception):
    pass


class MissingMicroOp(Exception):
    pass


def build_composite_object_micro_op(tag, args):
    name = tag[0]
    if name == "get-wildcard":
        return CompositeObjectWildcardGetterType(*args)
    if name == "get":
        key = tag[1]
        return CompositeObjectGetterType(key, *args)
    if name == "set-wildcard":
        return CompositeObjectWildcardSetterType(*args)
    if name == "set":
        key = tag[1]
        return CompositeObjectSetterType(key, *args)
    if name == "delete-wildcard":
        return CompositeObjectWildcardDeletterType(*args)
    if name == "delete":
        key = tag[1]
        return CompositeObjectDeletterType(key, *args)


class CompositeObjectManager(OperableObjectManager):
    def add_composite_type(self, type):
        if self.default_type is None:
            self.default_type = type
        for tag, micro_op in type.micro_op_types.items():
            self.add_micro_op_type(type, tag, micro_op)

    def add_micro_op_tag(self, type, tag, *args):
        if not isinstance(tag, tuple):
            raise FatalError()

        new_micro_op_type = build_composite_object_micro_op(tag, args)

        if new_micro_op_type is None:
            raise FatalError()

        self.add_micro_op_type(type, tag, new_micro_op_type)

    def add_micro_op_type(self, type, tag, micro_op_type):
        if not micro_op_type.can_fail:
            self.check_for_conflicts_with_existing_micro_ops(micro_op_type)

        if not isinstance(tag, tuple):
            raise FatalError()

        self.micro_op_types[id(type)][tag] = micro_op_type

        micro_op = micro_op_type.create(self.obj)
        micro_op.bind_to_in_place_value()

    def get_flattened_micro_op_types(self):
        result = []
        for micro_op_types in self.micro_op_types.values():
            for micro_op_type in micro_op_types.values():
                result.append(micro_op_type)
        return result

    def get_merged_micro_op_types(self):
        result = {}
        for micro_op_types in self.micro_op_types.values():
            for tag, micro_op_type in micro_op_types.items():
                if tag in result:
                    result[tag] = result[tag].merge(micro_op_type)
                else:
                    result[tag] = micro_op_type
        return result

    def check_for_conflicts_with_existing_micro_ops(self, new_micro_op):
        if new_micro_op.check_for_data_conflict(self.obj):
            pass

        if new_micro_op.check_for_data_conflict(self.obj):
            raise MicroOpTypeConflict()

        potentially_blocking_other_micro_op_types = [o for o in self.get_flattened_micro_op_types() if not o.can_fail]

        for other_micro_op_type in potentially_blocking_other_micro_op_types:
            if other_micro_op_type.check_for_new_micro_op_type_conflict(new_micro_op):
                raise MicroOpTypeConflict()


class CompositeObject(object):
    def __init__(self, initial_data=None):
        if initial_data is None:
            initial_data = {}
        for key, value in initial_data.items():
            self.__dict__[key] = value

    def __setattr__(self, key, value):
        manager = get_manager(self)
        micro_op_type = manager.get_micro_op_type(None, ("set", key))
        if micro_op_type is not None:
            micro_op = micro_op_type.create(self)
            micro_op.invoke(value)
        else:
            micro_op_type = manager.get_micro_op_type(None, ("set-wildcard",))

            if micro_op_type is None:
                manager.get_micro_op_type(None, ("set-wildcard",))
                raise MissingMicroOp()

            micro_op = micro_op_type.create(self)
            micro_op.invoke(key, value)

    def __getattribute__(self, key):
        if key in ("__dict__",):
            return super(CompositeObject, self).__getattribute__(key)

        manager = get_manager(self)
        micro_op_type = manager.get_micro_op_type(None, ("get", key))
        if micro_op_type is not None:
            micro_op = micro_op_type.create(self)
            return micro_op.invoke()
        else:
            micro_op_type = manager.get_micro_op_type(None, ("get-wildcard",))

            if micro_op_type is None:
                raise MissingMicroOp()

            micro_op = micro_op_type.create(self)
            return micro_op.invoke(key)

    def __delattr__(self, key):
        manager = get_manager(self)
        micro_op_type = manager.get_micro_op_type(None, ("delete", key))
        if micro_op_type is not None:
            micro_op = micro_op_type.create(self)
            return micro_op.invoke()
        else:
            micro_op_type = manager.get_micro_op_type(None, ("delete-wildcard",))

            if micro_op_type is None:
                raise MissingMicroOp()

            micro_op = micro_op_type.create(self)
            return micro_op.invoke(key)


class Object(CompositeObject):
    pass


weak_objs_by_id = {}
managers_by_id = {}


def get_manager(obj):
    manager = managers_by_id.get(id(obj), None)
    if not manager:
        weak_objs_by_id[id(obj)] = weakref.ref(obj)
        manager = CompositeObjectManager(obj)
        managers_by_id[id(obj)] = manager
    return manager


def obj_cleared_callback(obj):
    del weak_objs_by_id[id(obj)]
    del managers_by_id[id(obj)]


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

def merge_types(types):
    types = [unwrap_types(t) for t in types]
    types = [item for sublist in types for item in sublist]

    to_drop = []
    for i1, t1 in enumerate(types):
        for i2, t2 in enumerate(types):
            if i1 > i2 and t1.is_copyable_from(t2):
                to_drop.append(i2)
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


class CompositeType(Type):
    def __init__(self, micro_op_types, initial_data=None):
        for tag in micro_op_types.keys():
            if not isinstance(tag, tuple):
                raise FatalError()
        self.micro_op_types = micro_op_types
        self.initial_data = initial_data

    def is_copyable_from(self, other):
        if not isinstance(other, CompositeType):
            return False
        for ours in self.micro_op_types.values():
            for theirs in other.micro_op_types.values():
                if ours.check_for_new_micro_op_type_conflict(theirs):
                    return False
        for our_tag, our_micro_op in self.micro_op_types.items():
            if our_micro_op.can_fail:
                continue
            their_micro_op = other.micro_op_types.get(our_tag, None)
            initial_data_conflict = not other.initial_data or our_micro_op.check_for_data_conflict(other.initial_data)
            if their_micro_op is None and initial_data_conflict:
                return False
        return True


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


class InvalidData(Exception):
    pass


def get_type_of_value(value):
    if isinstance(value, (str, int)):
        return UnitType(value)
    if isinstance(value, CompositeObject):
        return CompositeType(get_manager(value).get_merged_micro_op_types(), value)
    raise InvalidData()
