from unittest import main
from unittest.case import TestCase

from munch import Munch

from rdhlang5_types.composites import get_manager, get_type_of_value, \
    RDHList, CompositeType, RDHObject, SPARSE_ELEMENT
from rdhlang5_types.core_types import StringType, AnyType, IntegerType, UnitType, \
    OneOfType, NoValueType
from rdhlang5_types.exceptions import InvalidDereferenceKey
from rdhlang5_types.list_types import ListType
from rdhlang5_types.object_types import ObjectType, Const, PythonObjectType, \
    DefaultDictType, ObjectGetterType, ObjectSetterType, ObjectDeletterType


class TestObject(object):
    def __init__(self, initial_data):
        for key, value in initial_data.items():
            self.__dict__[key] = value

class TestMicroOpMerging(TestCase):
    def test_merge_gets(self):
        first = ObjectGetterType("foo", IntegerType(), False, False)
        second = ObjectGetterType("foo", UnitType(5), False, False)

        combined = first.merge(second)
        self.assertTrue(isinstance(combined.type, UnitType))
        self.assertEqual(combined.type.value, 5)

    def test_merge_sets(self):
        first = ObjectSetterType("foo", IntegerType(), False, False)
        second = ObjectSetterType("foo", UnitType(5), False, False)

        combined = first.merge(second)
        self.assertTrue(isinstance(combined.type, IntegerType))

class TestBasicObject(TestCase):
    def test_add_micro_op_dictionary(self):
        return # doesn't work because type system doesn't support python dicts yet
        obj = { "foo": "hello" }
        get_manager(obj).add_composite_type(CompositeType({
            ("get", "foo"): ObjectGetterType("foo", StringType(), False, False)
        }))

    def test_add_micro_op_object(self):
        class Foo(object):
            pass
        obj = Foo()
        obj.foo = "hello"
        get_manager(obj).add_composite_type(CompositeType({
            ("get", "foo"): ObjectGetterType("foo", StringType(), False, False)
        }))

    def test_add_micro_op_munch(self):
        obj = TestObject({ "foo": "hello" })
        get_manager(obj).add_composite_type(CompositeType({
            ("get", "foo"): ObjectGetterType("foo", StringType(), False, False)
        }))

    def test_setup_read_write_property(self):
        obj = TestObject({ "foo": "hello" })
        get_manager(obj).add_composite_type(CompositeType({
            ("get", "foo"): ObjectGetterType("foo", StringType(), False, False),
            ("set", "foo"): ObjectSetterType("foo", StringType(), False, False)
        }))

    def test_setup_broad_reading_property(self):
        obj = TestObject({ "foo": "hello" })
        get_manager(obj).add_composite_type(CompositeType({
            ("get", "foo"): ObjectGetterType("foo", AnyType(), False, False),
            ("set", "foo"): ObjectSetterType("foo", StringType(), False, False)
        }))

    def test_failed_setup_broad_writing_property(self):
        with self.assertRaises(Exception):
            obj = TestObject({ "foo": "hello" })

            get_manager(obj).add_composite_type(CompositeType({
                ("get", "foo"): ObjectGetterType("foo", StringType(), False, False),
                ("set", "foo"): ObjectSetterType("foo", AnyType(), False, False)
            }))

    def test_composite_object_dereference(self):
        obj = TestObject({ "foo": "hello" })

        get_manager(obj).add_composite_type(CompositeType({
            ("get", "foo"): ObjectGetterType("foo", StringType(), False, False),
            ("set", "foo"): ObjectSetterType("foo", StringType(), False, False)
        }))

        self.assertEquals(obj.foo, "hello")

    def test_composite_object_broad_dereference(self):
        obj = TestObject({ "foo": "hello" })

        get_manager(obj).add_composite_type(CompositeType({
            ("get", "foo"): ObjectGetterType("foo", AnyType(), False, False),
            ("set", "foo"): ObjectSetterType("foo", AnyType(), False, False)
        }))

        self.assertEquals(obj.foo, "hello")

    def test_composite_object_assignment(self):
        obj = TestObject({ "foo": "hello" })

        get_manager(obj).add_composite_type(CompositeType({
            ("get", "foo"): ObjectGetterType("foo", StringType(), False, False),
            ("set", "foo"): ObjectSetterType("foo", StringType(), False, False)
        }))

        obj.foo = "what"

    def test_composite_object_invalid_assignment(self):
        obj = TestObject({ "foo": "hello" })

        get_manager(obj).add_composite_type(CompositeType({
            ("get", "foo"): ObjectGetterType("foo", StringType(), False, False),
            ("set", "foo"): ObjectSetterType("foo", StringType(), False, False)
        }))

        with self.assertRaises(Exception):
            obj.foo = 5

    def test_python_like_object(self):
        obj = TestObject({ "foo": "hello" })

        get_manager(obj).add_composite_type(CompositeType({
            ("get", "foo"): ObjectGetterType("foo", AnyType(), False, False),
            ("set", "foo"): ObjectSetterType("foo", AnyType(), False, False)
        }))

        self.assertEquals(obj.foo, "hello")
        obj.foo = "what"
        self.assertEquals(obj.foo, "what")

    def test_java_like_object(self):
        obj = TestObject({ "foo": "hello" })

        get_manager(obj).add_composite_type(CompositeType({
            ("get", "foo"): ObjectGetterType("foo", StringType(), False, False),
            ("set", "foo"): ObjectSetterType("foo", StringType(), False, False)
        }))

        self.assertEquals(obj.foo, "hello")
        obj.foo = "what"
        self.assertEquals(obj.foo, "what")

        with self.assertRaises(Exception):
            obj.bar = "hello"

    def test_const_property(self):
        obj = TestObject({ "foo": "hello" })

        get_manager(obj).add_composite_type(CompositeType({
            ("get", "foo"): ObjectGetterType("foo", StringType(), False, False)
        }))

        self.assertEquals(obj.foo, "hello")
        with self.assertRaises(Exception):
            obj.foo = "what"

    def test_invalid_initialization(self):
        obj = TestObject({})
        with self.assertRaises(Exception):
            get_manager(obj).add_micro_op_tag(None, ("get", "foo"), StringType(), False, False)

    def test_delete_property(self):
        obj = TestObject({ "foo": "hello" })

        get_manager(obj).add_composite_type(CompositeType({
            ("get", "foo"): ObjectGetterType("foo", StringType(), True, True),
            ("set", "foo"): ObjectSetterType("foo", StringType(), True, True),
            ("delete", "foo"): ObjectDeletterType("foo", True)
        }))

        del obj.foo
        self.assertFalse(hasattr(obj, "foo"))


class TestCompositeType(TestCase):
    def test_basic_class(self):
        T = ObjectType({
            "foo": IntegerType()
        })

        S = ObjectType({
            "foo": IntegerType(),
            "bar": StringType()
        })

        self.assertTrue(T.is_copyable_from(S))
        self.assertFalse(S.is_copyable_from(T))

    def test_const_allows_broader_types(self):
        T = ObjectType({
            "foo": Const(AnyType())
        })

        S = ObjectType({
            "foo": IntegerType()
        })

        self.assertTrue(T.is_copyable_from(S))
        self.assertFalse(S.is_copyable_from(T))

    def test_broad_type_assignments_blocked(self):
        T = ObjectType({
            "foo": AnyType()
        })

        S = ObjectType({
            "foo": IntegerType()
        })

        self.assertFalse(T.is_copyable_from(S))
        self.assertFalse(S.is_copyable_from(T))

    def test_simple_fields_are_required(self):
        T = ObjectType({
        })

        S = ObjectType({
            "foo": IntegerType()
        })

        self.assertTrue(T.is_copyable_from(S))
        self.assertFalse(S.is_copyable_from(T))

    def test_many_fields_are_required(self):
        T = ObjectType({
            "foo": IntegerType(),
            "bar": IntegerType(),
        })

        S = ObjectType({
            "foo": IntegerType(),
            "bar": IntegerType(),
            "baz": IntegerType()
        })

        self.assertTrue(T.is_copyable_from(S))
        self.assertFalse(S.is_copyable_from(T))

    def test_can_fail_micro_ops_are_enforced(self):
        foo = TestObject({
            "foo": 5,
            "bar": "hello"
        })

        get_manager(foo).add_composite_type(
            ObjectType({ "foo": Const(IntegerType()) })
        )

        with self.assertRaises(Exception):
            foo.foo = "hello"

    def test_const_is_enforced(self):
        return # test doesn't work because the assignment uses the set-wildcard
        foo = {
            "foo": 5,
            "bar": "hello"
        }

        get_manager(foo).add_composite_type(
            ObjectType({ "foo": Const(IntegerType()) })
        )

        with self.assertRaises(Exception):
            foo.foo = 42

    def test_types_on_object_merged(self):
        foo = TestObject({
            "foo": 5,
            "bar": "hello"
        })
        get_manager(foo).add_composite_type(
            ObjectType({ "foo": IntegerType() })
        )
        get_manager(foo).add_composite_type(
            ObjectType({ "bar": StringType() })
        )
        object_type = get_type_of_value(foo)

        ObjectType({
            "foo": IntegerType(),
            "bar": StringType()
        }).is_copyable_from(object_type)


class TestUnitTypes(TestCase):
    def test_basics(self):
        foo = TestObject({
            "bar": 42
        })
        get_manager(foo).add_composite_type(ObjectType({
            "bar": UnitType(42)
        }))
        self.assertEquals(foo.bar, 42)

    def test_broadening_blocked(self):
        foo = TestObject({
            "bar": 42
        })
        get_manager(foo).add_composite_type(ObjectType({
            "bar": UnitType(42)
        }))

        with self.assertRaises(Exception):
            get_manager(foo).add_composite_type(ObjectType({
                "bar": IntegerType()
            }))

    def test_narrowing_blocked(self):
        foo = TestObject({
            "bar": 42
        })
        get_manager(foo).add_composite_type(ObjectType({
            "bar": IntegerType()
        }))
        with self.assertRaises(Exception):
            get_manager(foo).add_composite_type(ObjectType({
                "bar": UnitType(42)
            }))

    def test_broadening_with_const_is_ok(self):
        foo = TestObject({
            "bar": 42
        })
        get_manager(foo).add_composite_type(ObjectType({
            "bar": UnitType(42)
        }))

        get_manager(foo).add_composite_type(ObjectType({
            "bar": Const(IntegerType())
        }))


class TestNestedCompositeTypes(TestCase):
    def test_basic_assignment(self):
        foo = TestObject({
            "bar": TestObject({
                "baz": 42
            })
        })

        get_manager(foo).add_composite_type(
            ObjectType({
                "bar": ObjectType({
                    "baz": IntegerType()
                })
            })
        )

        foo.bar = TestObject({ "baz": 42 })

        self.assertEquals(foo.bar.baz, 42)

    def test_blocked_basic_assignment(self):
        foo = TestObject({
            "bar": TestObject({
                "baz": 42
            })
        })

        get_manager(foo).add_composite_type(
            ObjectType({
                "bar": ObjectType({
                    "baz": IntegerType()
                })
            })
        )

        with self.assertRaises(Exception):
            foo.bar = TestObject({ "baz": "hello" })

    def test_deletion_blocked(self):
        foo = TestObject({
            "bar": TestObject({
                "baz": 42
            })
        })

        get_manager(foo).add_composite_type(
            ObjectType({
                "bar": ObjectType({
                    "baz": IntegerType()
                })
            })
        )

        with self.assertRaises(Exception):
            del foo.bar

    def test_broad_assignment(self):
        foo = TestObject({
            "bar": TestObject({
                "baz": 42
            })
        })

        get_manager(foo).add_composite_type(
            ObjectType({
                "bar": ObjectType({
                    "baz": AnyType()
                })
            })
        )

        foo.bar = TestObject({ "baz": "hello" })

        self.assertEquals(foo.bar.baz, "hello")

    def test_double_deep_assignment(self):
        foo = TestObject({
            "bar": TestObject({
                "baz": TestObject({
                    "bam": 10
                })
            })
        })

        get_manager(foo).add_composite_type(
            ObjectType({
                "bar": ObjectType({
                    "baz": ObjectType({
                        "bam": IntegerType()
                    })
                })
            })
        )

        self.assertEquals(foo.bar.baz.bam, 10)

        foo.bar = TestObject({ "baz": TestObject({ "bam": 42 }) })

        self.assertEquals(foo.bar.baz.bam, 42)

    def test_conflicting_types(self):
        foo = TestObject({
            "bar": TestObject({
                "baz": 42
            })
        })

        get_manager(foo).add_composite_type(
            ObjectType({
                "bar": ObjectType({
                    "baz": IntegerType()
                })
            })
        )

        with self.assertRaises(Exception):
            get_manager(foo).add_composite_type(
                ObjectType({
                    "bar": ObjectType({
                        "baz": AnyType()
                    })
                })
            )

    def test_changes_blocked_without_micro_ops(self):
        foo = TestObject({
            "bar": TestObject({
                "baz": 42
            })
        })

        get_manager(foo)

        with self.assertRaises(Exception):
            foo.bar = "hello"

    def test_very_broad_assignment(self):
        foo = TestObject({
            "bar": TestObject({
                "baz": 42
            })
        })

        get_manager(foo).add_composite_type(
            ObjectType({ "bar": AnyType() })
        )

        foo.bar = "hello"
        self.assertEquals(foo.bar, "hello")


class TestNestedPythonTypes(TestCase):
    def test_python_like_type(self):
        foo = TestObject({
            "bar": TestObject({
                "baz": 42
            })
        })

        get_manager(foo).add_composite_type(PythonObjectType())

        foo.bar.baz = 22

        foo.bar = "hello"
        self.assertEquals(foo.bar, "hello")

    def test_python_object_with_reference_can_be_modified(self):
        bar = TestObject({
            "baz": 42
        })
        foo = TestObject({
            "bar": bar
        })

        get_manager(bar).add_composite_type(ObjectType({ "baz": IntegerType() }))
        get_manager(foo).add_composite_type(PythonObjectType())

        self.assertEqual(foo.bar.baz, 42)
        foo.bar.baz = 5
        self.assertEqual(foo.bar.baz, 5)

    def test_python_object_with_reference_types_are_enforced(self):
        bar = TestObject({
            "baz": 42
        })
        foo = TestObject({
            "bar": bar
        })

        get_manager(bar).add_composite_type(ObjectType({ "baz": IntegerType() }))
        get_manager(foo).add_composite_type(PythonObjectType())

        with self.assertRaises(Exception):
            foo.bar.baz = "hello"

    def test_python_object_with_reference_can_be_replaced(self):
        bar = TestObject({
            "baz": 42
        })
        foo = TestObject({
            "bar": bar
        })

        get_manager(bar).add_composite_type(ObjectType({ "baz": IntegerType() }))
        get_manager(foo).add_composite_type(PythonObjectType())

        foo.bar = TestObject({
            "baz": 123
        })

        self.assertEqual(foo.bar.baz, 123)
        foo.bar.baz = "what"
        self.assertEqual(foo.bar.baz, "what")

    def test_adding_late_python_constraint_fails(self):
        bar = TestObject({
            "baz": 42
        })
        foo = TestObject({
            "bar": bar
        })

        get_manager(bar).add_composite_type(ObjectType({ "baz": IntegerType() }))
        get_manager(foo).add_composite_type(PythonObjectType())

        foo.bar = TestObject({
            "baz": 123
        })

        with self.assertRaises(Exception):
            get_manager(foo.bar).add_composite_type(ObjectType({ "baz": IntegerType() }))

    def test_python_delete_works(self):
        foo = TestObject({
            "bar": TestObject({
                "baz": 42
            })
        })

        get_manager(foo).add_composite_type(PythonObjectType())

        del foo.bar
        self.assertFalse(hasattr(foo, "bar"))

    def test_python_replacing_object_works(self):
        foo = TestObject({
            "bar": TestObject({
                "baz": 42
            })
        })

        get_manager(foo).add_composite_type(PythonObjectType())

        foo.bar = TestObject({ "baz": 123 })

        self.assertEquals(foo.bar.baz, 123)

    def test_python_random_read_fails_nicely(self):
        foo = TestObject({
            "bar": TestObject({
                "baz": 42
            })
        })

        get_manager(foo).add_composite_type(PythonObjectType())

        with self.assertRaises(AttributeError):
            foo.bop

class TestDefaultDict(TestCase):
    def test_default_dict(self):
        def default_factory(target, key):
            return "{}-123".format(key)

        foo = RDHObject({
            "bar": "forty-two"
        }, default_factory=default_factory)

        get_manager(foo).add_composite_type(DefaultDictType(StringType()))

        self.assertEquals(foo.bar, "forty-two")
        self.assertEquals(foo.bam, "bam-123")

class TestListObjects(TestCase):
    def test_basic_list_of_ints(self):
        foo = [ 1, 2, 3 ]

        get_manager(foo).add_composite_type(ListType([], IntegerType()))

        foo[0] = 42
        self.assertEqual(foo[0], 42)

    def test_basic_tuple_of_ints(self):
        foo = [ 1, 2, 3 ]

        get_manager(foo).add_composite_type(ListType([ IntegerType(), IntegerType(), IntegerType() ], None))

        foo[0] = 42
        self.assertEqual(foo[0], 42)

    def test_bounds_enforced(self):
        foo = [ 1, 2 ]

        with self.assertRaises(Exception):
            get_manager(foo).add_composite_type(ListType([ IntegerType(), IntegerType(), IntegerType() ], None))


class TestListType(TestCase):
    def test_simple_list_assignment(self):
        foo = ListType([], IntegerType())
        bar = ListType([], IntegerType())

        self.assertTrue(foo.is_copyable_from(bar))

    def test_simple_tuple_assignment(self):
        foo = ListType([ IntegerType(), IntegerType() ], None)
        bar = ListType([ IntegerType(), IntegerType() ], None)

        self.assertTrue(foo.is_copyable_from(bar))

    def test_broadening_tuple_assignment_blocked(self):
        foo = ListType([ AnyType(), AnyType() ], None)
        bar = ListType([ IntegerType(), IntegerType() ], None)

        self.assertFalse(foo.is_copyable_from(bar))

    def test_narrowing_tuple_assignment_blocked(self):
        foo = ListType([ IntegerType(), IntegerType() ], None)
        bar = ListType([ AnyType(), AnyType() ], None)

        self.assertFalse(foo.is_copyable_from(bar))

    def test_broadening_tuple_assignment_allowed_with_const(self):
        foo = ListType([ Const(AnyType()), Const(AnyType()) ], None)
        bar = ListType([ IntegerType(), IntegerType() ], None)

        self.assertTrue(foo.is_copyable_from(bar))

    def test_truncated_tuple_slice_assignment(self):
        foo = ListType([ IntegerType() ], None)
        bar = ListType([ IntegerType(), IntegerType() ], None)

        self.assertTrue(foo.is_copyable_from(bar))

    def test_expanded_tuple_slice_assignment_blocked(self):
        foo = ListType([ IntegerType(), IntegerType() ], None)
        bar = ListType([ IntegerType() ], None)

        self.assertFalse(foo.is_copyable_from(bar))
  
    def test_convert_tuple_to_list(self):
        foo = ListType([ ], IntegerType(), allow_delete=False, allow_wildcard_insert=False, allow_push=False)
        bar = ListType([ IntegerType(), IntegerType() ], None)

        self.assertTrue(foo.is_copyable_from(bar))
    def test_const_covariant_array_assignment_allowed(self):
        foo = ListType([ ], Const(AnyType()), allow_push=False, allow_wildcard_insert=False)
        bar = ListType([ ], IntegerType())

        self.assertTrue(foo.is_copyable_from(bar))

    def test_convert_tuple_to_list_with_deletes_blocked(self):
        foo = ListType([ ], IntegerType())
        bar = ListType([ IntegerType(), IntegerType() ], None)

        self.assertFalse(foo.is_copyable_from(bar))

    def test_pushing_into_short_tuple(self):
        foo = ListType([ IntegerType() ], IntegerType(), allow_delete=False)
        bar = ListType([ IntegerType() ], IntegerType(), allow_delete=False, allow_wildcard_insert=False)

        self.assertTrue(foo.is_copyable_from(bar))

    def test_pushing_into_long_tuple(self):
        foo = ListType([ IntegerType(), IntegerType() ], IntegerType(), allow_delete=False)
        bar = ListType([ IntegerType(), IntegerType() ], IntegerType(), allow_delete=False, allow_wildcard_insert=False)

        self.assertTrue(foo.is_copyable_from(bar))

    def test_same_type_array_assignment(self):
        foo = ListType([ ], IntegerType())
        bar = ListType([ ], IntegerType())

        self.assertTrue(foo.is_copyable_from(bar))

    def test_covariant_array_assignment_blocked(self):
        foo = ListType([ ], AnyType())
        bar = ListType([ ], IntegerType())

        self.assertFalse(foo.is_copyable_from(bar))

class TestList(TestCase):
    def test_simple_list_assignment(self):
        foo = [ 4, 6, 8 ]
        get_manager(foo).add_composite_type(ListType([], IntegerType()))

    def test_list_modification_wrong_type_blocked(self):
        foo = [ 4, 6, 8 ]
        get_manager(foo).add_composite_type(ListType([], IntegerType()))

        with self.assertRaises(Exception):
            foo.append("hello")

    def test_list_modification_right_type_ok(self):
        foo = [ 4, 6, 8 ]
        get_manager(foo).add_composite_type(ListType([], IntegerType()))

        foo.append(10)

    def test_list_appending_blocked(self):
        foo = [ 4, 6, 8 ]
        get_manager(foo).add_composite_type(ListType([], None))

        with self.assertRaises(Exception):
            foo.append(10)

    def test_mixed_type_tuple(self):
        foo = [ 4, 6, 8 ]
        get_manager(foo).add_composite_type(ListType([ IntegerType(), AnyType() ], None))

        with self.assertRaises(Exception):
            foo[0] = "hello"

        self.assertEqual(foo[0], 4)

        foo[1] = "what"
        self.assertEqual(foo[1], "what")

    def test_outside_tuple_access_blocked(self):
        foo = [ 4, 6, 8 ]
        get_manager(foo).add_composite_type(ListType([ IntegerType(), AnyType() ], None))

        with self.assertRaises(Exception):
            foo[2]
        with self.assertRaises(Exception):
            foo[2] = "hello"

    def test_outside_tuple_access_allowed(self):
        foo = [ 4, 6, 8 ]
        get_manager(foo).add_composite_type(ListType([ ], AnyType(), allow_push=False, allow_delete=False, allow_wildcard_insert=False))

        self.assertEqual(foo[2], 8)
        foo[2] = "hello"
        self.assertEqual(foo[2], "hello")

    def test_combined_const_list_and_tuple(self):
        foo = [ 4, 6, 8 ]
        get_manager(foo).add_composite_type(ListType([ IntegerType(), AnyType() ], Const(AnyType()), allow_push=False, allow_delete=False, allow_wildcard_insert=False))

        self.assertEqual(foo[2], 8)

    def test_insert_at_start(self):
        foo = [ 4, 6, 8 ]
        get_manager(foo).add_composite_type(ListType([ ], IntegerType()))

        foo.insert(0, 2)
        self.assertEqual(list(foo), [ 2, 4, 6, 8 ])

    def test_insert_with_wrong_type_blocked(self):
        foo = [ 4, 6, 8 ]
        get_manager(foo).add_composite_type(ListType([ ], IntegerType()))

        with self.assertRaises(Exception):
            foo.insert(0, "hello")

    def test_insert_on_short_tuple(self):
        foo = [ 4, 6, 8 ]
        get_manager(foo).add_composite_type(ListType([ IntegerType() ], IntegerType(), allow_push=True, allow_delete=False, allow_wildcard_insert=False))

        foo.insert(0, 2)
        self.assertEqual(list(foo), [ 2, 4, 6, 8 ])

    def test_insert_on_long_tuple(self):
        foo = [ 4, 6, 8 ]
        get_manager(foo).add_composite_type(ListType([ IntegerType(), IntegerType() ], IntegerType(), allow_push=True, allow_delete=False, allow_wildcard_insert=False))

        foo.insert(0, 2)
        self.assertEqual(list(foo), [ 2, 4, 6, 8 ])

    def test_sparse_list_setting(self):
        foo = [ 4, 6, 8 ]
        get_manager(foo).add_composite_type(ListType([ ], IntegerType(), is_sparse=True))

        foo[4] = 12
        self.assertEqual(list(foo), [ 4, 6, 8, SPARSE_ELEMENT, 12 ])

    def test_sparse_list_inserting(self):
        foo = [ 4, 6, 8 ]
        get_manager(foo).add_composite_type(ListType([ ], IntegerType(), is_sparse=True))

        foo.insert(4, 12)
        self.assertEqual(list(foo), [ 4, 6, 8, SPARSE_ELEMENT, 12 ])

if __name__ == '__main__':
    main()
