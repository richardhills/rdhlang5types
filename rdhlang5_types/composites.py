from collections import defaultdict

from rdhlang5_types.core_types import unwrap_types, Type
from rdhlang5_types.exceptions import FatalError, MicroOpTypeConflict
from rdhlang5_types.managers import get_manager
from rdhlang5_types.micro_ops import MicroOpType, MicroOp, merge_micro_op_types


class CompositeType(Type):
    def __init__(self, micro_op_types, initial_data=None):
        if not isinstance(micro_op_types, dict):
            raise FatalError()
        for tag in micro_op_types.keys():
            if not isinstance(tag, tuple):
                raise FatalError()
        self.micro_op_types = micro_op_types
        self.initial_data = initial_data

    def get_micro_op_type(self, tag):
        return self.micro_op_types.get(tag, None)

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

    def get_crystal_value(self):
        return self.initial_data
#        result = self.initial_data
#        for micro_op in self.micro_op_types.values():
#            result = micro_op.apply_crystal_value(result)
#        return result

    def __repr__(self):
        return "Composite<{}; {}>".format(", ".join([str(m) for m in self.micro_op_types.values()]), self.initial_data)

class Composite(object):
    pass

def bind_type_to_value(source, key, type, value):
    if not isinstance(value, Composite):
        return
    
    from rdhlang5_types.list_types import RDHListType
    if isinstance(type, RDHListType):
        pass
    manager = get_manager(value)

    source_manager = get_manager(source)

    something_worked = False
    for sub_type in unwrap_types(type):
        if isinstance(sub_type, CompositeType):
            try:
                manager.add_composite_type(sub_type)
                source_manager.child_type_references[key].append(sub_type)
                something_worked = True
            except MicroOpTypeConflict:
                pass
        else:
            something_worked = True
    if not something_worked:
        raise FatalError()

def unbind_type_to_value(source, key, type, value):
    if not isinstance(value, Composite):
        return
    source_manager = get_manager(source)
    for sub_type in source_manager.child_type_references[key]:
        get_manager(value).remove_composite_type(sub_type)
    source_manager.child_type_references[key] = []

class CompositeObjectManager(object):
    def __init__(self, obj):
        self.obj = obj
        self.micro_op_types = defaultdict(dict)
        self.type_references = defaultdict(int)
        # A dictionary of key (names) to a list of types bound to the remote object
        self.child_type_references = defaultdict(list)

        self.default_type = None
        self.default_factory = None

    def get_merged_micro_op_types(self, new_micro_op_types={}):
        return merge_micro_op_types(self.micro_op_types.values() + [ new_micro_op_types ])

    def add_composite_type(self, type):
        for micro_op_type in type.micro_op_types.values():
            micro_op_type.check_for_conflicts_with_existing_micro_ops(
                self.obj, self.get_merged_micro_op_types(type.micro_op_types)
            )

        if self.default_type is None:
            self.default_type = type

        self.type_references[id(type)] += 1

        if id(type) in self.micro_op_types:
            return

        self.micro_op_types[id(type)] = dict(type.micro_op_types)

        for tag, micro_op_type in type.micro_op_types.items():
            micro_op_type.bind(None, self.obj)

    def remove_composite_type(self, type):
        type_id = id(type)
        if self.type_references[type_id] > 0:
            self.type_references[type_id] -= 1
        if self.type_references[type_id] == 0:
            del self.micro_op_types[type_id]

    def get_micro_op_type(self, type, tag):
        if id(type) not in self.micro_op_types:
            raise FatalError()
        return self.micro_op_types.get(id(type)).get(tag, None)

    def get_flattened_micro_op_types(self):
        result = []
        for micro_op_types in self.micro_op_types.values():
            for micro_op_type in micro_op_types.values():
                result.append(micro_op_type)
        return result


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
