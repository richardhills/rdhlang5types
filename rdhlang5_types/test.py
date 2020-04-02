from unittest import main
from unittest.case import TestCase

from rdhlang5_types.composites import Object, get_manager, get_type_of_value
from rdhlang5_types.core_types import StringType, AnyType, IntegerType, UnitType
from rdhlang5_types.object_types import ObjectType, Const, PythonObjectType


class TestObject(TestCase):
    def test_add_micro_op(self):
        obj = Object({ "foo": "hello" })
        get_manager(obj).add_micro_op_tag(None, ( "get", "foo" ), StringType(), False)

    def test_setup_read_write_property(self):
        obj = Object({ "foo": "hello" })
        get_manager(obj).add_micro_op_tag(None, ( "get", "foo" ), StringType(), False)
        get_manager(obj).add_micro_op_tag(None, ( "set", "foo" ), StringType(), False)

    def test_setup_broad_reading_property(self):
        obj = Object({ "foo": "hello" })
        get_manager(obj).add_micro_op_tag(None, ( "get", "foo" ), AnyType(), False)
        get_manager(obj).add_micro_op_tag(None, ( "set", "foo" ), StringType(), False)

    def test_failed_setup_broad_writing_property(self):
        with self.assertRaises(Exception):
            obj = Object({ "foo": "hello" })
            get_manager(obj).add_micro_op_tag(None, ( "get", "foo" ), StringType(), False)
            get_manager(obj).add_micro_op_tag(None, ( "set", "foo" ), AnyType(), False)

    def test_composite_object_dereference(self):
        obj = Object({ "foo": "hello" })
        get_manager(obj).add_micro_op_tag(None, ( "get", "foo" ), StringType(), False)
        get_manager(obj).add_micro_op_tag(None, ( "set", "foo" ), StringType(), False)

        self.assertEquals(obj.foo, "hello")

    def test_composite_object_broad_dereference(self):
        obj = Object({ "foo": "hello" })
        get_manager(obj).add_micro_op_tag(None, ( "get", "foo" ), AnyType(), False)
        get_manager(obj).add_micro_op_tag(None, ( "set", "foo" ), AnyType(), False)

        self.assertEquals(obj.foo, "hello")

    def test_composite_object_assignment(self):
        obj = Object({ "foo": "hello" })
        get_manager(obj).add_micro_op_tag(None, ( "get", "foo" ), StringType(), False)
        get_manager(obj).add_micro_op_tag(None, ( "set", "foo" ), StringType(), False)

        obj.foo = "what"

    def test_composite_object_invalid_assignment(self):
        obj = Object({ "foo": "hello" })
        get_manager(obj).add_micro_op_tag(None, ( "get", "foo" ), StringType(), False)
        get_manager(obj).add_micro_op_tag(None, ( "set", "foo" ), StringType(), False)

        with self.assertRaises(Exception):
            obj.foo = 5

    def test_python_like_object(self):
        obj = Object({ "foo": "hello" })
        get_manager(obj).add_micro_op_tag(None, ( "get-wildcard", ), AnyType(), True)
        get_manager(obj).add_micro_op_tag(None, ( "set-wildcard", ), AnyType(), False)

        self.assertEquals(obj.foo, "hello")
        obj.foo = "what"
        self.assertEquals(obj.foo, "what")

    def test_java_like_object(self):
        obj = Object({ "foo": "hello" })
        get_manager(obj).add_micro_op_tag(None, ( "get", "foo" ), StringType(), False)
        get_manager(obj).add_micro_op_tag(None, ( "set", "foo" ), StringType(), False)

        self.assertEquals(obj.foo, "hello")
        obj.foo = "what"
        self.assertEquals(obj.foo, "what")

        with self.assertRaises(Exception):
            obj.bar = "hello"

    def test_const_property(self):
        obj = Object({ "foo": "hello" })
        get_manager(obj).add_micro_op_tag(None, ( "get", "foo" ), StringType(), False)

        self.assertEquals(obj.foo, "hello")
        with self.assertRaises(Exception):
            obj.foo = "what"

    def test_invalid_initialization(self):
        obj = Object()
        with self.assertRaises(Exception):
            get_manager(obj).add_micro_op_tag(None, ( "get", "foo" ), StringType(), False)

    def test_delete_property(self):
        obj = Object({ "foo": "hello" })
        get_manager(obj).add_micro_op_tag(None, ( "get-wildcard", ), StringType(), True)
        get_manager(obj).add_micro_op_tag(None, ( "set-wildcard", ), StringType(), True)
        get_manager(obj).add_micro_op_tag(None, ( "delete-wildcard", ), True)

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
        foo = Object({
            "foo": 5,
            "bar": "hello"
        })

        get_manager(foo).add_composite_type(
            ObjectType({ "foo": Const(IntegerType()) })
        )

        with self.assertRaises(Exception):
            foo.foo = "hello"

    def test_const_is_enforced(self):
        return
        foo = Object({
            "foo": 5,
            "bar": "hello"
        })

        get_manager(foo).add_composite_type(
            ObjectType({ "foo": Const(IntegerType()) })
        )

        with self.assertRaises(Exception):
            foo.foo = 42

    def test_types_on_object_merged(self):
        foo = Object({
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
        foo = Object({
            "bar": 42
        })
        get_manager(foo).add_composite_type(ObjectType({
            "bar": UnitType(42)
        }))
        self.assertEquals(foo.bar, 42)

    def test_broadening_blocked(self):
        foo = Object({
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
        foo = Object({
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
        foo = Object({
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
        foo = Object({
            "bar": Object({
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

        foo.bar = Object({ "baz": 42 })

        self.assertEquals(foo.bar.baz, 42)

    def test_blocked_basic_assignment(self):
        foo = Object({
            "bar": Object({
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
            foo.bar = Object({ "baz": "hello" })

    def test_deletion_blocked(self):
        foo = Object({
            "bar": Object({
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
        foo = Object({
            "bar": Object({
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

        foo.bar = Object({ "baz": "hello" })

        self.assertEquals(foo.bar.baz, "hello")

    def test_conflicting_types(self):
        foo = Object({
            "bar": Object({
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
        foo = Object({
            "bar": Object({
                "baz": 42
            })
        })

        with self.assertRaises(Exception):
            foo.bar = "hello"

    def test_very_broad_assignment(self):
        foo = Object({
            "bar": Object({
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
        foo = Object({
            "bar": Object({
                "baz": 42
            })
        })

        get_manager(foo).add_composite_type(PythonObjectType())

        foo.bar.baz = 22

        foo.bar = "hello"
        self.assertEquals(foo.bar, "hello")

    def test_python_object_with_reference_can_be_modified(self):
        bar = Object({
            "baz": 42
        })
        foo = Object({
            "bar": bar
        })

        get_manager(bar).add_composite_type(ObjectType({ "baz": IntegerType() }))
        get_manager(foo).add_composite_type(PythonObjectType())

        self.assertEqual(foo.bar.baz, 42)
        foo.bar.baz = 5
        self.assertEqual(foo.bar.baz, 5)

    def test_python_object_with_reference_types_are_enforced(self):
        bar = Object({
            "baz": 42
        })
        foo = Object({
            "bar": bar
        })

        get_manager(bar).add_composite_type(ObjectType({ "baz": IntegerType() }))
        get_manager(foo).add_composite_type(PythonObjectType())

        with self.assertRaises(Exception):
            foo.bar.baz = "hello"

    def test_python_object_with_reference_can_be_replaced(self):
        bar = Object({
            "baz": 42
        })
        foo = Object({
            "bar": bar
        })

        get_manager(bar).add_composite_type(ObjectType({ "baz": IntegerType() }))
        get_manager(foo).add_composite_type(PythonObjectType())

        foo.bar = Object({
            "baz": 123
        })

        self.assertEqual(foo.bar.baz, 123)
        foo.bar.baz = "what"
        self.assertEqual(foo.bar.baz, "what")

    def test_adding_late_python_constraint_fails(self):
        bar = Object({
            "baz": 42
        })
        foo = Object({
            "bar": bar
        })

        get_manager(bar).add_composite_type(ObjectType({ "baz": IntegerType() }))
        get_manager(foo).add_composite_type(PythonObjectType())

        foo.bar = Object({
            "baz": 123
        })

        with self.assertRaises(Exception):
            get_manager(foo.bar).add_composite_type(ObjectType({ "baz": IntegerType() }))


    def test_python_delete_works(self):
        foo = Object({
            "bar": Object({
                "baz": 42
            })
        })

        get_manager(foo).add_composite_type(PythonObjectType())

        del foo.bar
        self.assertFalse(hasattr(foo, "bar"))

    def test_python_replacing_object_works(self):
        foo = Object({
            "bar": Object({
                "baz": 42
            })
        })

        get_manager(foo).add_composite_type(PythonObjectType())

        foo.bar = Object({ "baz": 123 })

        self.assertEquals(foo.bar.baz, 123)

if __name__ == '__main__':
    main()
