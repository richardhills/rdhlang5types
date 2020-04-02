from rdhlang5_types.exceptions import MicroOpConflict, FatalError


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


class MicroOp(object):
    def invoke(self, *args):
        raise NotImplementedError()

    def bind_to_in_place_value(self):
        raise NotImplementedError()


def check_micro_op_conflicts(micro_op, args, other_micro_op_types):
    for other_micro_op_type in other_micro_op_types:
        if other_micro_op_type.check_for_micro_op_conflict(micro_op, args):
            if micro_op.can_fail:
                raise MicroOpConflict()
            else:
                raise FatalError()

