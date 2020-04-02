from _collections import defaultdict
from rdhlang5_types.exceptions import FatalError, MicroOpTypeConflict, MissingMicroOp, \
    InvalidData
import weakref

from rdhlang5_types.core_types import unwrap_types, Type, UnitType


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

def bind_type_to_value(type, value):
    if not isinstance(value, CompositeObject):
        return
    for sub_type in unwrap_types(type):
        if isinstance(sub_type, CompositeType):
            try:
                get_manager(value).add_composite_type(sub_type)
            except MicroOpTypeConflict:
                pass



class CompositeObjectManager(object):
    def __init__(self, obj):
        self.obj = obj
        self.micro_op_types = defaultdict(dict)
        self.default_type = None

    def add_micro_op_tag(self, type, tag, *args):
        if not isinstance(tag, tuple):
            raise FatalError()

        new_micro_op_type = build_composite_object_micro_op(tag, args)

        if new_micro_op_type is None:
            raise FatalError()

        self.add_micro_op_type(type, tag, new_micro_op_type)

    def add_composite_type(self, type):
        if self.default_type is None:
            self.default_type = type
        for tag, micro_op in type.micro_op_types.items():
            self.add_micro_op_type(type, tag, micro_op)

    def add_micro_op_type(self, type, tag, micro_op_type):
        if not micro_op_type.can_fail:
            self.check_for_conflicts_with_existing_micro_ops(micro_op_type)

        if not isinstance(tag, tuple):
            raise FatalError()

        self.micro_op_types[id(type)][tag] = micro_op_type

        micro_op = micro_op_type.create(self.obj)
        micro_op.bind_to_in_place_value()

    def get_micro_op_type(self, type, tag):
        if type is None:
            type = self.default_type
        return self.micro_op_types.get(id(type)).get(tag, None)

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


def build_composite_object_micro_op(tag, args):
    from rdhlang5_types.object_types import *
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


def get_type_of_value(value):
    if isinstance(value, (str, int)):
        return UnitType(value)
    if isinstance(value, CompositeObject):
        return CompositeType(get_manager(value).get_merged_micro_op_types(), value)
    raise InvalidData()

