from collections import defaultdict, MutableSequence
import weakref

from rdhlang5_types.core_types import unwrap_types, Type, UnitType, NoValueType
from rdhlang5_types.exceptions import FatalError, MicroOpTypeConflict, MissingMicroOp, \
    InvalidData, InvalidDereferenceKey, InvalidAssignmentKey
from rdhlang5_types.micro_ops import MicroOpType, MicroOp, merge_micro_op_types
from rdhlang5_types.runtime import replace_all_refs


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
                if ours.check_for_new_micro_op_type_conflict(theirs, self.micro_op_types):
                    return False
        for our_tag, our_micro_op in self.micro_op_types.items():
            if our_micro_op.key_error:
                continue
            their_micro_op = other.micro_op_types.get(our_tag, None)
            initial_data_conflict = not other.initial_data or our_micro_op.check_for_data_conflict(other.initial_data)
            if their_micro_op is None and initial_data_conflict:
                return False
        return True


def bind_type_to_value(type, value):
    if not isinstance(value, (RDHObject, RDHList)):
        return
    something_worked = False
    for sub_type in unwrap_types(type):
        if isinstance(sub_type, CompositeType):
            try:
                get_manager(value).add_composite_type(sub_type)
                something_worked = True
            except MicroOpTypeConflict:
                pass
        else:
            something_worked = True
    if not something_worked:
        raise FatalError()

def unbind_type_to_value(type, value):
    if not isinstance(value, (RDHObject, RDHList)):
        return
    for sub_type in unwrap_types(type):
        if isinstance(sub_type, CompositeType):
            get_manager(value).remove_composite_type(sub_type)

class CompositeObjectManager(object):
    def __init__(self, obj):
        self.obj = obj
        self.micro_op_types = defaultdict(dict)
        self.type_references = defaultdict(int)
        self.default_type = None
        self.default_factory = None

    def get_merged_micro_op_types(self):
        return merge_micro_op_types(self.micro_op_types.values())

    def add_composite_type(self, type):
        if self.default_type is None:
            self.default_type = type

        self.type_references[id(type)] += 1

        if id(type) in self.micro_op_types:
            return

        for tag, micro_op_type in type.micro_op_types.items():
            micro_op_type.bind(None, self.obj)

            micro_op_type.check_for_conflicts_with_existing_micro_ops(
                self.obj, self.get_merged_micro_op_types()
            )

            self.micro_op_types[id(type)][tag] = micro_op_type

    def remove_composite_type(self, type):
        type_id = id(type)
        if self.type_references[type_id] > 0:
            self.type_references[type_id] -= 1
        if self.type_references[type_id] == 0:
            del self.micro_op_types[type_id]

    def get_micro_op_type(self, type, tag):
        return self.micro_op_types.get(id(type)).get(tag, None)

    def get_flattened_micro_op_types(self):
        result = []
        for micro_op_types in self.micro_op_types.values():
            for micro_op_type in micro_op_types.values():
                result.append(micro_op_type)
        return result

class ObjectManager(CompositeObjectManager):
    pass


class ListManager(CompositeObjectManager):
    pass

class RDHObject(object):
    def __init__(self, initial_data=None, default_factory=None):
        if initial_data is None:
            initial_data = {}
        for key, value in initial_data.items():
            self.__dict__[key] = value
        get_manager(self).default_factory = default_factory

    def __setattr__(self, key, value):
        manager = get_manager(self)
        default_type = manager.default_type
        micro_op_type = manager.get_micro_op_type(default_type, ("set", key))
        if micro_op_type is not None:
            micro_op = micro_op_type.create(self, default_type)
            micro_op.invoke(value)
        else:
            micro_op_type = manager.get_micro_op_type(default_type, ("set-wildcard",))

            if micro_op_type is None:
                manager.get_micro_op_type(default_type, ("set-wildcard",))
                raise MissingMicroOp()

            micro_op = micro_op_type.create(self, default_type)
            micro_op.invoke(key, value)

    def __getattribute__(self, key):
        if key in ("__dict__", "__class__"):
            return super(RDHObject, self).__getattribute__(key)

        try:
            manager = get_manager(self)
            default_type = manager.default_type
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
            raise AttributeError()

    def __delattr__(self, key):
        manager = get_manager(self)
        default_type = manager.default_type
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

def create_rdh_object_type(base_class):
    return type(base_class.__name__, (base_class, RDHObject,), {})

SPARSE_ELEMENT = object()

class RDHList(MutableSequence):
    def __init__(self, initial_data):
        self.wrapped = {
            index: value for index, value in enumerate(initial_data)
        }
        self.length = len(initial_data)

    def __len__(self):
        return self.length

    def insert(self, index, element, raw=False):
        if raw:
            for i in reversed(range(index, self.length)):
                self.wrapped[i + 1] = self.wrapped[i]
            self.wrapped[index] = element
            self.length = max(index + 1, self.length + 1)
            return

        manager = get_manager(self)
        default_type = manager.default_type
        micro_op_type = manager.get_micro_op_type(default_type, ("insert", index))

        if micro_op_type is not None:
            micro_op = micro_op_type.create(self, default_type)
            return micro_op.invoke(element)
        else:
            micro_op_type = manager.get_micro_op_type(default_type, ("insert-wildcard",))

            if micro_op_type is None:
                raise MissingMicroOp()

            micro_op = micro_op_type.create(self, default_type)
            micro_op.invoke(index, element)

    def __setitem__(self, key, value, raw=False):
        if raw:
            self.wrapped[key] = value
            self.length = max(self.length, key + 1)
            return

        try:
            manager = get_manager(self)
            default_type = manager.default_type
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
            raise IndexError()

    def __getitem__(self, key, raw=False):
        if key in ("__dict__",):
            return super(RDHObject, self).__getattribute__(key)

        if raw:
            if key < 0 or key >= self.length:
                raise FatalError()
            return self.wrapped.get(key, SPARSE_ELEMENT)

        try:
            manager = get_manager(self)
            default_type = manager.default_type
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
            if key >= 0 and key < self.length:
                return None
            raise IndexError()

    def __delitem__(self, key, raw=False):
        if raw:
            for i in reversed(range(key, self.length)):
                self.wrapped[i - 1] = self.wrapped[i]
            self.length -= 1
            return

        manager = get_manager(self)
        default_type = manager.default_type
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


weak_objs_by_id = {}
managers_by_id = {}


def get_manager(obj):
    if not isinstance(obj, (list, RDHList, RDHObject)) and not hasattr(obj, "__dict__"):
        return None

    manager = managers_by_id.get(id(obj), None)
    if not manager:
        old_obj = obj
        if isinstance(obj, list) and not isinstance(obj, RDHList):
            obj = RDHList(obj)
            replace_all_refs(old_obj, obj)            
            manager = ListManager(obj)
        elif isinstance(obj, RDHObject):
            manager = ObjectManager(obj)
        elif isinstance(obj, RDHList):
            manager = ListManager(obj)
        elif isinstance(obj, object) and hasattr(obj, "__dict__"):
            obj = create_rdh_object_type(obj.__class__)(obj.__dict__)
            replace_all_refs(old_obj, obj)
            manager = ObjectManager(obj)
        else:
            raise FatalError()
        weak_objs_by_id[id(obj)] = weakref.ref(obj)
        managers_by_id[id(obj)] = manager
    return manager


def obj_cleared_callback(obj):
    del weak_objs_by_id[id(obj)]
    del managers_by_id[id(obj)]


def get_type_of_value(value):
    if isinstance(value, (str, int)):
        return UnitType(value)
    if isinstance(value, (RDHObject, RDHList)):
        return CompositeType(get_manager(value).get_merged_micro_op_types(), value)
    if value is None:
        return NoValueType()
    raise InvalidData(value)

class DefaultFactoryType(MicroOpType):
    def __init__(self, type):
        self.type = type

    def create(self, target, through_type):
        return DefaultFactory(target)

    def bind(self, key, target):
        pass

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type, other_micro_op_types):
        return False

    def raise_on_micro_op_conflict(self, other_micro_op, args):
        pass

    def check_for_data_conflict(self, obj):
        if get_manager(obj).default_factory is None:
            return True
        return False

class DefaultFactory(MicroOp):
    def __init__(self, target):
        self.target = target

    def invoke(self, key):
        return get_manager(self.target).default_factory(self.target, key)
