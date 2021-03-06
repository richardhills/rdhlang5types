from collections import OrderedDict

from rdhlang5_types.composites import bind_type_to_value, unbind_type_to_value, \
    DefaultFactoryType, CompositeType, Composite, CompositeObjectManager
from rdhlang5_types.core_types import merge_types, OneOfType, AnyType, Const,\
    CrystalValueCanNotBeGenerated
from rdhlang5_types.exceptions import FatalError, MicroOpConflict, raise_if_safe, \
    InvalidAssignmentType, InvalidDereferenceKey, InvalidDereferenceType, \
    MicroOpTypeConflict, MissingMicroOp
from rdhlang5_types.managers import get_manager, get_type_of_value
from rdhlang5_types.micro_ops import MicroOpType, MicroOp, raise_micro_op_conflicts


WILDCARD = object()
MISSING = object()

def get_key_and_type(micro_op_type):
    if isinstance(micro_op_type, (ObjectWildcardGetterType, ObjectWildcardSetterType, ObjectWildcardDeletterType)):
        key = WILDCARD
    elif isinstance(micro_op_type, (ObjectGetterType, ObjectSetterType, ObjectDeletterType)):
        key = micro_op_type.key
    else:
        raise FatalError()

    if isinstance(micro_op_type, (ObjectWildcardGetterType, ObjectGetterType, ObjectWildcardSetterType, ObjectSetterType)):
        type = micro_op_type.type
    else:
        type = MISSING

    return key, type


def get_key_and_new_value(micro_op, args):
    if isinstance(micro_op, (ObjectWildcardGetter, ObjectWildcardDeletter)):
        key, = args
        new_value = MISSING
    elif isinstance(micro_op, (ObjectGetter, ObjectDeletter)):
        key = micro_op.key
        new_value = MISSING
    elif isinstance(micro_op, ObjectWildcardSetter):
        key, new_value = args
    elif isinstance(micro_op, ObjectSetter):
        key = micro_op.key
        new_value = args[0]
    else:
        raise FatalError()
    if new_value is not None:
        get_manager(new_value)
    return key, new_value

class ObjectMicroOpType(MicroOpType):
    def apply_crystal_value(self, target):
        if target is None:
            target = RDHObject({})
        return target

    def check_for_conflicts_with_existing_micro_ops(self, obj, micro_op_types):
        if not isinstance(obj, RDHObject):
            raise MicroOpTypeConflict()
        return super(ObjectMicroOpType, self).check_for_conflicts_with_existing_micro_ops(obj, micro_op_types)

    def check_for_data_conflict(self, obj):
        if not isinstance(obj, RDHObject):
            return True


class ObjectWildcardGetterType(ObjectMicroOpType):
    def __init__(self, type, key_error, type_error):
        if type is None:
            raise FatalError()
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def create(self, target, through_type):
        return ObjectWildcardGetter(target, through_type, self.type, self.key_error, self.type_error)

    def bind(self, key, target):
        if key is not None:
            keys = [ key ]
        else:
            keys = target.__dict__.keys()
        for k in keys:
            value = target.__dict__[k]
            get_manager(value)
            bind_type_to_value(target, k, self.type, value)

    def unbind(self, key, target):
        if key is not None:
            keys = [ key ]
        else:
            keys = target.__dict__.keys()
        for k in keys:
            unbind_type_to_value(target, k, self.type, target.__dict__[k])

    def apply_crystal_value(self, target):
        target = super(ObjectWildcardGetterType, self).apply_crystal_value(target)

        try:
            our_crystal_value = self.type.get_crystal_value()
            target.__dict__[self.key] = our_crystal_value
        except CrystalValueCanNotBeGenerated:
            pass

        return target

    def check_for_conflicts_with_existing_micro_ops(self, obj, micro_op_types):
        default_factories = [ o for o in micro_op_types.values() if isinstance(o, DefaultFactoryType)]
        has_default_factory = len(default_factories) > 0

        if not self.key_error:
            if not has_default_factory:
                raise MicroOpTypeConflict()
            default_factory = default_factories[0]
            if not self.type.is_copyable_from(default_factory.type):
                raise MicroOpTypeConflict()

        return super(ObjectWildcardGetterType, self).check_for_conflicts_with_existing_micro_ops(obj, micro_op_types)

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type, other_micro_op_types):
        default_factory = other_micro_op_types.get(("default-factory",), None)
        has_default_factory = default_factory is not None
        if not self.key_error:
            if not has_default_factory:
                return True

        if isinstance(other_micro_op_type, (ObjectGetterType, ObjectWildcardGetterType)):
            return False
        if isinstance(other_micro_op_type, (ObjectSetterType, ObjectWildcardSetterType)):
            if not self.type_error and not self.type.is_copyable_from(other_micro_op_type.type):
                return True
        if isinstance(other_micro_op_type, (ObjectDeletterType, ObjectWildcardDeletterType)):
            if not self.key_error and not other_micro_op_type.key_error and not has_default_factory:
                return True
        return False

    def raise_on_micro_op_conflict(self, other_micro_op, args):
        if isinstance(other_micro_op, (ObjectSetter, ObjectWildcardSetter)):
            _, new_value = get_key_and_new_value(other_micro_op, args)
            if not self.type_error and not self.type.is_copyable_from(get_type_of_value(new_value)):
                raise_if_safe(InvalidAssignmentType, other_micro_op.type_error)

        if isinstance(other_micro_op, (ObjectDeletter, ObjectWildcardDeletter)):
            if not self.key_error:
                raise_if_safe(InvalidAssignmentType, other_micro_op.key_error)
        return False

    def check_for_data_conflict(self, obj):
        if super(ObjectWildcardGetterType, self).check_for_data_conflict(obj):
            return True

        if not self.key_error and get_manager(obj).default_factory is None:
            return True

        if not self.type_error:
            for value in obj.__dict__.values():
                get_manager(value)
                if not self.type.is_copyable_from(get_type_of_value(value)):
                    return True

        return False

    def merge(self, other_micro_op_type):
        return ObjectWildcardGetterType(
            merge_types([ self.type, other_micro_op_type.type ], "sub"),
            self.key_error or other_micro_op_type.key_error,
            self.type_error or other_micro_op_type.type_error
        )

    def __repr__(self):
        return "get.*.{}".format(self.type)


class ObjectWildcardGetter(MicroOp):

    def __init__(self, target, through_type, type, key_error, type_error):
        self.target = target
        self.through_type = through_type
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def invoke(self, key):
        raise_micro_op_conflicts(self, [ key ], get_manager(self.target).get_flattened_micro_op_types())

        if key in self.target.__dict__:
            value = self.target.__dict__[key]
        else:
            default_factory_op_type = get_manager(self.target).get_micro_op_type(self.through_type, ("default-factory", ))

            if not default_factory_op_type:
                raise_if_safe(InvalidDereferenceKey, self.key_error)

            default_factory_op = default_factory_op_type.create(self.target, self.through_type)
            value = default_factory_op.invoke(key)

        get_manager(value)

        type_of_value = get_type_of_value(value)
        if not self.type.is_copyable_from(type_of_value):
            raise raise_if_safe(InvalidDereferenceType, self.type_error)
        return value

class ObjectGetterType(ObjectMicroOpType):
    def __init__(self, key, type, key_error, type_error):
        if type is None:
            raise FatalError()
        self.key = key
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def create(self, target, through_type):
        return ObjectGetter(target, through_type, self.key, self.type, self.key_error, self.type_error)

    def bind(self, key, target):
        if key is not None and key != self.key:
            return
        value = target.__dict__[self.key]
        get_manager(value)
        bind_type_to_value(target, key, self.type, value)

    def unbind(self, key, target):
        if key is not None and key != self.key:
            return
        unbind_type_to_value(target, key, self.type, target.__dict__[key])

    def check_for_conflicts_with_existing_micro_ops(self, obj, micro_op_types):
        default_factory = micro_op_types.get(("default-factory",), None)
        has_default_factory = default_factory is not None
        has_value_in_place = self.key in obj.__dict__

        if not self.key_error and not has_value_in_place:
            if not has_default_factory:
                raise MicroOpTypeConflict()
            if not self.type.is_copyable_from(default_factory.type):
                raise MicroOpTypeConflict()

        return super(ObjectGetterType, self).check_for_conflicts_with_existing_micro_ops(obj, micro_op_types)

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type, other_micro_op_types):
        if isinstance(other_micro_op_type, (ObjectGetterType, ObjectWildcardGetterType)):
            return False
        if isinstance(other_micro_op_type, (ObjectSetterType, ObjectWildcardSetterType)):
            other_key, other_type = get_key_and_type(other_micro_op_type)
            if other_key is not WILDCARD and other_key != self.key:
                return False
            if not self.type_error and not other_micro_op_type.type_error and not self.type.is_copyable_from(other_type):
                return True
        if isinstance(other_micro_op_type, (ObjectDeletterType, ObjectWildcardDeletterType)):
            other_key, _ = get_key_and_type(other_micro_op_type)
            if other_key is not WILDCARD and other_key != self.key:
                return False

            has_default_factory = any(isinstance(o, DefaultFactoryType) for o in other_micro_op_types.values())
            if not self.key_error and not other_micro_op_type.key_error and not has_default_factory:
                return True
        return False

    def raise_on_micro_op_conflict(self, other_micro_op, args):
        if isinstance(other_micro_op, (ObjectGetter, ObjectWildcardGetter)):
            return
        if isinstance(other_micro_op, (ObjectSetter, ObjectWildcardSetter)):
            other_key, other_new_value = get_key_and_new_value(other_micro_op, args)
            if other_key != self.key:
                return
            if not self.type_error and not self.type.is_copyable_from(get_type_of_value(other_new_value)):
                raise_if_safe(InvalidAssignmentType, other_micro_op.type_error)
        if isinstance(other_micro_op, (ObjectDeletter, ObjectWildcardDeletter)):
            other_key, _ = get_key_and_new_value(other_micro_op, args)
            if not self.key_error and other_key == self.key:
                raise raise_if_safe(InvalidDereferenceKey, other_micro_op.key_error)

    def check_for_data_conflict(self, obj):
        if super(ObjectGetterType, self).check_for_data_conflict(obj):
            return True
        if self.key not in obj.__dict__:
            return True
        value_in_place = obj.__dict__[self.key]
        get_manager(value_in_place)
        type_of_value = get_type_of_value(value_in_place)
        if not self.type.is_copyable_from(type_of_value):
            return True

        return False

    def merge(self, other_micro_op_type):
        if other_micro_op_type.key != self.key:
            raise FatalError()
        return ObjectGetterType(
            self.key,
            merge_types([ self.type, other_micro_op_type.type ], "sub"),
            self.key_error or other_micro_op_type.key_error,
            self.type_error or other_micro_op_type.type_error
        )

    def __repr__(self):
        return "get.{}.{}".format(self.key, self.type)

class ObjectGetter(MicroOp):
    def __init__(self, target, through_type, key, type, key_error, type_error):
        self.target = target
        self.through_type = through_type
        self.key = key
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def invoke(self):
        raise_micro_op_conflicts(self, [], get_manager(self.target).get_flattened_micro_op_types())

        if self.key in self.target.__dict__:
            value = self.target.__dict__[self.key]
        else:
            default_factory_op = get_manager(self.target).get_micro_op_type(self.through_type, ("default-factory", ))

            if default_factory_op:
                value = default_factory_op.invoke(self.key)
            else:
                raise_if_safe(InvalidDereferenceKey, self.key_error)

        get_manager(value)

        type_of_value = get_type_of_value(value)

        if not self.type.is_copyable_from(type_of_value):
            raise raise_if_safe(InvalidDereferenceType, self.type_error)

        return value


class ObjectWildcardSetterType(ObjectMicroOpType):
    def __init__(self, type, key_error, type_error):
        if type is None:
            raise FatalError()
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def create(self, target, through_type):
        return ObjectWildcardSetter(target, self.type, self.key_error, self.type_error)

    def bind(self, key, target):
        pass

    def unbind(self, key, target):
        pass

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type, other_micro_op_types):
        if isinstance(other_micro_op_type, (ObjectGetterType, ObjectWildcardGetterType)):
            if not self.type_error and not other_micro_op_type.type_error and not other_micro_op_type.type.is_copyable_from(self.type):
                return True
        return False

    def raise_on_micro_op_conflict(self, other_micro_op, args):
        pass

    def check_for_data_conflict(self, obj):
        if super(ObjectWildcardSetterType, self).check_for_data_conflict(obj):
            return True

        return False

    def merge(self, other_micro_op_type):
        return ObjectWildcardSetterType(
            merge_types([ self.type, other_micro_op_type.type ], "super"),
            self.key_error or other_micro_op_type.key_error,
            self.type_error or other_micro_op_type.type_error
        )

    def __repr__(self):
        return "set.*.{}".format(self.type)

class ObjectWildcardSetter(MicroOp):
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

        self.target.__dict__[key] = new_value

        for other_micro_op_type in get_manager(self.target).get_flattened_micro_op_types():
            other_micro_op_type.bind(key, self.target)

class ObjectSetterType(ObjectMicroOpType):
    def __init__(self, key, type, key_error, type_error):
        if type is None:
            raise FatalError()
        self.key = key
        self.type = type
        self.key_error = key_error
        self.type_error = type_error

    def create(self, target, through_type):
        return ObjectSetter(target, self.key, self.type, self.key_error, self.type_error)

    def bind(self, key, target):
        pass

    def unbind(self, key, target):
        pass

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type, other_micro_op_types):
        if isinstance(other_micro_op_type, (ObjectGetterType, ObjectWildcardGetterType)):
            other_key, other_type = get_key_and_type(other_micro_op_type)
            if other_key is not WILDCARD and other_key != self.key:
                return False
            if not self.type_error and not other_micro_op_type.type_error and not other_type.is_copyable_from(self.type):
                return True
        return False

    def raise_on_micro_op_conflict(self, other_micro_op, args):
        pass

    def check_for_data_conflict(self, obj):
        if super(ObjectSetterType, self).check_for_data_conflict(obj):
            return True

        return False

    def merge(self, other_micro_op_type):
        if other_micro_op_type.key != self.key:
            raise FatalError()
        return ObjectSetterType(
            self.key,
            merge_types([ self.type, other_micro_op_type.type ], "super"),
            self.key_error or other_micro_op_type.key_error,
            self.type_error or other_micro_op_type.type_error
        )

    def __repr__(self):
        return "set.{}.{}".format(self.key, self.type)

class ObjectSetter(MicroOp):

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

        self.target.__dict__[self.key] = new_value

        for other_micro_op_type in get_manager(self.target).get_flattened_micro_op_types():
            other_micro_op_type.bind(self.key, self.target)


class InvalidDeletion(Exception):
    pass


class ObjectWildcardDeletterType(ObjectMicroOpType):

    def __init__(self, key_error):
        self.key_error = key_error

    def create(self, target, through_type):
        return ObjectWildcardDeletter(target, self.key_error)

    def bind(self, key, target):
        pass

    def unbind(self, key, target):
        pass

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type, other_micro_op_types):
        if isinstance(other_micro_op_type, (ObjectGetterType, ObjectWildcardGetterType)):
            default_factory = other_micro_op_types.get(("default-factory",), None)
            has_default_factory = default_factory is not None

            if not self.key_error and not other_micro_op_type.key_error and not has_default_factory:
                return True
        return False

    def raise_on_micro_op_conflict(self, other_micro_op, args):
        return False

    def check_for_data_conflict(self, obj):
        if super(ObjectWildcardDeletterType, self).check_for_data_conflict(obj):
            return True

        return False

    def merge(self, other_micro_op_type):
        return ObjectWildcardDeletterType(
            self.key_error or other_micro_op_type.key_error
        )


class ObjectWildcardDeletter(MicroOp):

    def __init__(self, target, key_error):
        self.target = target
        self.key_error = key_error

    def invoke(self, key):
        raise_micro_op_conflicts(self, [ key ], get_manager(self.target).get_flattened_micro_op_types())

        for other_micro_op_type in get_manager(self.target).get_flattened_micro_op_types():
            other_micro_op_type.unbind(key, self.target)

        del self.target.__dict__[key]


class ObjectDeletterType(ObjectMicroOpType):
    def __init__(self, key, key_error):
        self.key = key
        self.key_error = key_error

    def create(self, target, through_type):
        return ObjectDeletter(target, self.key, self.key_error)

    def bind(self, key, target):
        pass

    def unbind(self, key, target):
        pass

    def check_for_new_micro_op_type_conflict(self, other_micro_op_type, other_micro_op_types):
        if isinstance(other_micro_op_type, (ObjectGetterType, ObjectWildcardGetterType)):
            other_key, _ = get_key_and_type(other_micro_op_type)
            if other_key is not WILDCARD and other_key != self.key:
                return False
            if not self.key_error and not other_micro_op_type.key_error:
                return True
        return False

    def raise_on_micro_op_conflict(self, other_micro_op, args):
        pass

    def check_for_data_conflict(self, obj):
        if super(ObjectDeletterType, self).check_for_data_conflict(obj):
            return True

        return False

    def merge(self, other_micro_op_type):
        return ObjectDeletterType(self.key, self.key_error or other_micro_op_type.key_error)


class ObjectDeletter(MicroOp):
    def __init__(self, target, key, key_error):
        self.target = target
        self.key = key
        self.key_error = key_error

    def invoke(self):
        raise_micro_op_conflicts(self, [ ], get_manager(self.target).get_flattened_micro_op_types())

        for other_micro_op_type in get_manager(self.target).get_flattened_micro_op_types():
            other_micro_op_type.unbind(self.key, self.target)

        del self.target.__dict__[self.key]

def RDHObjectType(properties=None, wildcard_type=None, initial_data=None):
    if not properties:
        properties = {}
    micro_ops = OrderedDict({})

    for name, type in properties.items():
        const = False
        if isinstance(type, Const):
            const = True
            type = type.wrapped
    
        micro_ops[("get", name)] = ObjectGetterType(name, type, False, False)
        if not const:
            micro_ops[("set", name)] = ObjectSetterType(name, type, False, False)
    
    if wildcard_type:
        micro_ops[("get-wildcard",)] = ObjectWildcardGetterType(wildcard_type, True, False)
        micro_ops[("set-wildcard",)] = ObjectWildcardSetterType(wildcard_type, True, True)
    
    return CompositeType(micro_ops, initial_data=initial_data)

class PythonObjectType(CompositeType):
    def __init__(self):
        micro_ops = {}

        micro_ops[("get-wildcard",)] = ObjectWildcardGetterType(OneOfType([ self, AnyType() ]), True, False)
        micro_ops[("set-wildcard",)] = ObjectWildcardSetterType(OneOfType([ self, AnyType() ]), False, False)
        micro_ops[("delete-wildcard",)] = ObjectWildcardDeletterType(True)

        super(PythonObjectType, self).__init__(micro_ops)

class DefaultDictType(CompositeType):
    def __init__(self, type):
        # Use an ordered dict because the default-factory needs to be in place
        # for the later ops to work
        micro_ops = OrderedDict()

        micro_ops[("default-factory",)] = DefaultFactoryType(type)
        micro_ops[("get-wildcard",)] = ObjectWildcardGetterType(type, False, False)
        micro_ops[("set-wildcard",)] = ObjectWildcardSetterType(type, False, False)
        micro_ops[("delete-wildcard",)] = ObjectWildcardDeletterType(False)

        super(DefaultDictType, self).__init__(micro_ops)

class RDHObject(Composite, object):
    def __init__(self, initial_data=None, default_factory=None, bind=None):
        if initial_data is None:
            initial_data = {}
        for key, value in initial_data.items():
            self.__dict__[key] = value
        get_manager(self).default_factory = default_factory
        if bind:
            get_manager(self).add_composite_type(bind)

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
            if default_type is None:
                raise AttributeError(key)
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
            raise AttributeError(key)

    def __delattr__(self, key):
        manager = get_manager(self)
        default_type = manager.default_type
        if default_type is None:
            raise AttributeError()
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

    def __repr__(self):
        return repr(self.__dict__)

    def __str__(self):
        return str(self.__dict__)

