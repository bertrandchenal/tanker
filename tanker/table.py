from collections import defaultdict
from datetime import datetime, timedelta, date
from itertools import chain
import json

from .utils import basestring, COLUMN_TYPE, strptime, ctx, pandas


EPOCH = datetime(1970, 1, 1)
skip_none = lambda fn: (
    lambda x: None if x is None or (pandas and pandas.isnull(x)) else fn(x)
)


class Column:
    def __init__(self, name, ctype, default=None):
        if ' ' in ctype:
            full_ctype = ctype
            ctype, self.fk = ctype.split()
            if '.' not in self.fk:
                msg = 'Malformed column definition "%s" for %s'
                raise ValueError(msg % (full_ctype, name))
            self.foreign_table, self.foreign_col = self.fk.split('.')
        else:
            self.fk = None
            self.foreign_table = self.foreign_col = None
        self.name = name
        self.default = default

        # Build ctype, array_dim and base_type
        self.ctype = ctype.upper()
        self.base_type = self.ctype
        self.array_dim = 0
        while self.base_type.endswith('[]'):
            self.base_type = self.base_type[:-2]
            self.array_dim += 1
        if self.array_dim and self.base_type in ('O2M', 'M2O'):
            msg = 'Array type is not supported on "%s" (for column "%s")'
            raise ValueError(msg % (self.base_type, name))
        if self.base_type not in COLUMN_TYPE:
            raise ValueError('Unexpected type %s for column %s' % (ctype, name))

    def sql_definition(self):
        if self.name == 'id':
            if ctx.flavor == 'sqlite':
                return 'INTEGER PRIMARY KEY'

            id_def = 'BIGSERIAL' if self.ctype == 'BIGINT' else 'SERIAL'
            if self.table.name in ctx.referenced:
                # (index on 'id' col is not needed if not part of fk)
                id_def += ' PRIMARY KEY'
            return id_def

        # Simple field
        if not self.fk:
            if self.default:
                return '%s DEFAULT %s' % (self.ctype, self.default)
            return self.ctype
        # O2M
        if self.ctype == 'O2M':
            return None
        # M2O
        if ctx.flavor == 'crdb':
            # TODO crdb does support this: ALTER TABLE orders ADD
            # CONSTRAINT customer_fk FOREIGN KEY (customer_id)
            # REFERENCES customers (id) ON DELETE CASCADE; So we
            # should call it after the columns is added
            return 'INTEGER'
        else:
            table = Table.get(self.foreign_table).name
            return 'INTEGER REFERENCES "%s" ("%s") ON DELETE CASCADE' % (
                table,
                self.foreign_col,
            )

    def get_foreign_table(self):
        if not self.foreign_table:
            raise ValueError(
                'The "%s" column of "%s" is not a foreign key'
                % (self.name, self.table.name)
            )
        return Table.get(self.foreign_table)

    def format_array(self, array, astype, array_dim):
        if array is None:
            return None
        if array_dim == 1:
            items = self.format(array, astype=astype, array_dim=0)
            items = map(lambda x: 'null' if x is None else str(x), items)
        else:
            items = (
                self.format_array(v, astype=astype, array_dim=array_dim - 1)
                for v in array
            )
        items = ','.join(items)
        # XXX https://github.com/cockroachdb/cockroach/issues/33429:
        # cockroach seems to choke on arrays
        return '{%s}' % items

    def format(self, values, astype=None, array_dim=None):
        '''
        Sanitize a column of values wrt the column type of the current
        field.
        '''
        astype = astype or self.base_type
        array_dim = self.array_dim if array_dim is None else array_dim

        if array_dim:
            for array in values:
                yield self.format_array(array, astype, array_dim)

        elif astype in ('TIMESTAMP', 'TIMESTAMPTZ'):
            for value in values:
                if value is None:
                    yield None
                elif isinstance(value, datetime):
                    yield value
                elif hasattr(value, 'timetuple'):
                    value = datetime(*value.timetuple()[:7])
                    yield value
                elif hasattr(value, 'tolist'):
                    # tolist is a numpy.datetime64 method that
                    # returns nanosecond from 1970. EPOCH + delta(val)
                    # suppors values far in the past (or future)
                    ts = value.tolist()
                    if ts is None:
                        value = None
                    else:
                        value = EPOCH + timedelta(seconds=ts / 1e9)
                        if astype == 'TIMESTAMPTZ':
                            # tolist as given us utc naive timestamp
                            from pytz import utc

                            value = value.replace(tzinfo=utc)
                    yield value
                elif isinstance(value, basestring):
                    yield strptime(value, astype)
                else:
                    raise ValueError(
                        'Unexpected value "%s" for type "%s"' % (value, astype)
                    )

        elif astype == 'DATE':
            for value in values:
                if value is None:
                    yield None
                elif isinstance(value, date):
                    yield value
                elif hasattr(value, 'timetuple'):
                    value = date(*value.timetuple()[:3])
                    yield value
                elif hasattr(value, 'tolist'):
                    ts = value.tolist()
                    if ts is None:
                        value = None
                    else:
                        dt = EPOCH + timedelta(seconds=ts / 1e9)
                        value = date(*dt.timetuple()[:3])
                    yield value
                elif isinstance(value, basestring):
                    yield strptime(value, astype)
                else:
                    raise ValueError(
                        'Unexpected value "%s" for type "%s"' % (value, astype)
                    )
        elif astype == 'JSONB':
            for value in values:
                if value is None:
                    yield None
                elif isinstance(value, basestring):
                    yield value
                else:
                    yield json.dumps(value)
        else:
            for v in values:
                yield v

    def __repr__(self):
        return '<Column %s %s>' % (self.name, self.ctype)


class Table:
    def __init__(
        self, name, columns, key=None, unique=None, values=None, use_index=None
    ):
        self.name = name
        self.columns = columns[:]
        self.unique = unique or []
        self.values = values
        self.use_index = use_index.upper() if use_index else 'BTREE'
        if not self.use_index in ('BRIN', 'BTREE'):
            msg = 'Value "%s" not supported for use-index'
            raise ValueError(msg % use_index)

        # Add implicit id column
        if 'id' not in [c.name for c in self.columns]:
            self.columns.insert(0, Column('id', 'INTEGER'))
        self.own_columns = [
            c for c in self.columns if c.name != 'id' and c.ctype != 'O2M'
        ]

        # Set table attribute on columns object
        for col in self.columns:
            col.table = self

        # set key
        if key is None:
            if len(self.columns) == 2:
                # If there is only one column (other than id), use it
                # as key
                key = tuple(c.name for c in self.columns if c.name != 'id')
            else:
                raise ValueError('No key defined on %s' % name)
        self.key = [key] if isinstance(key, basestring) else key
        # Test key columns are members of the table
        self._column_dict = dict((col.name, col) for col in self.columns)
        for col in self.key:
            if col not in self._column_dict:
                raise ValueError('Key column "%s" does not exist' % col)
        # # Forbid array types in key
        # for col in self.key:
        #     if col.array_dim:
        #         msg = 'Array type is not allowed on key column '\
        #               '("%s" in table "%s")'
        #         raise ValueError(msg % (col, self.name))

    def get_column(self, name):
        try:
            return self._column_dict[name]
        except KeyError:
            raise KeyError(
                'Column "%s" not found in table "%s"' % (name, self.name)
            )

    @classmethod
    def get(cls, table_name):
        return ctx.registry[table_name]

    def __repr__(self):
        return '<Table %s>' % self.name

    def default_fields(self):
        for col in self.own_columns:
            if col.ctype == 'M2O':
                ft = col.get_foreign_table()
                for i in ft.key:
                    yield '.'.join((col.name, i))
            else:
                yield col.name

    def link(self, dest):
        '''
        Returns all the possible set of relations between self and dest
        '''
        wave = [self]
        paths = defaultdict(list)

        while True:
            new_wave = []
            for tbl in wave:
                visited = set(chain.from_iterable(paths[tbl]))
                for col in tbl.columns:
                    # Follow non-visited relations
                    if col.ctype not in ('M2O', 'O2M'):
                        continue
                    if col in visited:
                        continue

                    # Add column to ancestor paths
                    foreign_table = col.get_foreign_table()
                    if paths[tbl]:
                        foreign_paths = [p + [col] for p in paths[tbl]]
                        paths[foreign_table].extend(foreign_paths)
                    else:
                        paths[foreign_table] = [[col]]
                    new_wave.append(foreign_table)
            if not new_wave:
                # No table to visit anymore
                break
            wave = new_wave
        return sorted(paths[dest], key=len)
