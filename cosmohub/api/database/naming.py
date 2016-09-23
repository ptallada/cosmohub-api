def _column_list(constraint, table):
    return '__'.join(constraint._pending_colargs)

def _referenced_table(constraint, table):
    return constraint.elements[0]._column_tokens[1]

def _referencing_table(constraint, table):
    return table.name

naming_convention = {
    'column_list'       : _column_list,
    'referenced_table'  : _referenced_table,
    'referencing_table' : _referencing_table,
    'pk' : 'pk__%(referencing_table)s',
    'fk' : 'fk__%(referencing_table)s__%(referenced_table)s',
    'uq' : 'uq__%(referencing_table)s__%(column_list)s',
    'ix' : 'ix__%(referencing_table)s__%(column_list)s',
}
