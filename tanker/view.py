from itertools import chain
from collections import defaultdict
from contextlib import contextmanager
import uuid

from .context import execute, executemany, TankerCursor, execute_values
from .expression import ReferenceSet, Expression, AST
from .table import Table
from .utils import basestring, interleave, pandas
from .utils import ctx, LRU, LRU_PAGE_SIZE, paginate

all_none = lambda xs: all(x is None for x in xs)


class ViewField:
    def __init__(self, name, desc, table):
        self.name = name
        self.desc = desc
        self.ref = None
        self.ctx = ctx

        if desc.startswith('('):
            ftype = ctype = 'EXPRESSION'
            self.col = None

        elif '.' in desc:
            ftype = 'INTEGER'
            exp = Expression(table)
            self.ref = ReferenceSet(exp).get_ref(desc)
            remote_col = self.ref.remote_table.get_column(self.ref.remote_field)
            ctype = remote_col.ctype
            self.col = table.get_column(desc.split('.')[0])

        elif desc.startswith('{'):
            ftype = ctype = 'ALIAS'
            self.col = None

        else:
            self.col = table.get_column(desc)
            ctype = self.col.ctype
            if ctype == 'M2O':
                ctype = ftype = 'INTEGER'
            else:
                ftype = ctype

        self.ctype = ctype.upper()
        self.ftype = ftype.upper()

    def __repr__(self):
        if self.name != self.desc:
            return '<ViewField %s (%s)>' % (self.desc, self.name)
        return '<ViewField %s>' % self.desc


class View(object):

    _fk_cache = {}

    def __init__(self, table, fields=None):
        self.ctx = ctx
        self.table = Table.get(table)
        if not fields:
            fields = list(self.table.default_fields())
        if isinstance(fields, basestring):
            fields = [[fields, fields]]
        elif isinstance(fields, dict):
            fields = fields.items()
        elif isinstance(fields, (list, tuple)) and isinstance(
            fields[0], basestring
        ):
            fields = zip(fields, fields)
        elif isinstance(fields, (list, tuple)) and isinstance(fields[0], tuple):
            fields = fields

        self.fields = [
            ViewField(name.strip(), desc, self.table) for name, desc in fields
        ]
        self.field_dict = dict((f.name, f) for f in self.fields)
        self.upd_filter_cnt = None
        self.ins_filter_cnt = None

        # field_map hold relation between fields given by the user and
        # the one from the db, field_idx keep their corresponding
        # positions
        self.field_map = defaultdict(list)
        self.field_idx = defaultdict(list)
        idx = 0
        for view_field in self.fields:
            if view_field.col is None:
                continue
            if self.field_map[view_field.col]:
                if view_field.col.ctype not in ('M2O', 'O2M'):
                    raise ValueError(
                        'Column %s is specified several time in view'
                        % view_field.col.name
                    )
            self.field_map[view_field.col].append(view_field)
            self.field_idx[view_field.col].append(idx)
            idx += 1

        # Key fields identify each line in the data
        self.key_fields = [
            f for f in self.fields if f.col and f.col.name in self.table.key
        ]
        # Key cols identify each row in the table
        id_col = self.table.get_column('id')
        if id_col in self.field_map:
            # Use id if present
            self.key_cols = [id_col.name]
        else:
            # Use natural key if not
            self.key_cols = self.table.key

    def get_field(self, name):
        return self.field_dict.get(name)

    def base_env(self):
        base_env = {}
        for field in self.fields:
            if field.name in self.table._column_dict:
                # Do not mask existing columns
                continue
            base_env[field.name] = field
        return base_env

    def read(
        self,
        filters=None,
        args=None,
        order=None,
        groupby=None,
        limit=None,
        distinct=False,
        offset=None,
        disable_acl=False,
    ):

        if isinstance(filters, basestring):
            filters = [filters]

        acl_filters = None
        if not disable_acl:
            acl_filters = self.ctx.cfg.get('acl-read', {}).get(self.table.name)

        # Inject fields name in base env and create expression
        exp = Expression(
            self.table, disable_acl=disable_acl, base_env=self.base_env()
        )

        # Add select fields
        statement = '(select-distinct %s)' if distinct else '(select %s)'
        select_ast = exp.parse(
            statement % ' '.join(f.desc for f in self.fields)
        )
        select_chunk = [select_ast]
        select_chunk.append('FROM "%s"' % self.table.name)

        # Identify aggregates
        aggregates = []
        for pos, atom in enumerate(select_ast.atoms[1:]):
            if not isinstance(atom, AST):
                continue
            if atom.is_aggregate():
                aggregates.append(pos)

        # Add filters
        filter_chunks = exp._build_filter_cond(filters, acl_filters)
        if filter_chunks:
            filter_chunks = ['WHERE'] + filter_chunks

        # Add group by
        groupby_chunks = []
        group_fields = []
        if groupby and isinstance(groupby, basestring):
            groupby = [groupby]
        elif aggregates and not groupby:
            groupby = []
            for pos, field in enumerate(self.fields):
                if pos in aggregates:
                    continue
                groupby.append(field.desc)

        if groupby:
            group_fields = [exp.parse(f) for f in groupby]
            groupby_chunks = ['GROUP BY'] + list(interleave(',', group_fields))

        if order:
            order_chunks = []
            if isinstance(order, (str, tuple)):
                order = [order]
            for item in order:
                if isinstance(item, (list, tuple)):
                    item, how = item
                else:
                    how = None
                chunk = [exp.parse(item)]

                if how:
                    if how.upper() not in ('ASC', 'DESC'):
                        msg = 'Unexpected value "%s" for sort direction' % how
                        raise ValueError(msg)
                    chunk.append(how)
                order_chunks += [chunk]
            order_chunks = ['ORDER BY'] + list(interleave(',', order_chunks))
        else:
            order_chunks = []

        join_chunks = [exp.ref_set]
        all_chunks = (
            select_chunk
            + join_chunks
            + filter_chunks
            + groupby_chunks
            + order_chunks
        )

        if limit is not None:
            all_chunks += ['LIMIT %s' % int(limit)]
        if offset is not None:
            all_chunks += ['OFFSET %s' % int(offset)]

        return TankerCursor(self, all_chunks, args=args)

    def format(self, data):
        for col in self.field_map:
            idx = self.field_idx[col]
            if col.ctype == 'M2O':
                fields = tuple(f for f in self.field_map[col])
                values = tuple(data[i] for i in idx)
                if len(fields) == 1 and fields[0].ref is None:
                    # Handle update of fk by id
                    yield map(int, data[idx[0]])
                else:
                    # Resolve foreign key reference
                    fmt_cols = lambda a: tuple(
                        a[0].col.format(a[1], astype=a[0].ctype)
                    )
                    values = map(fmt_cols, zip(fields, values))
                    yield View.resolve_fk(fields, values)
            else:
                yield col.format(data[idx[0]])

    def delete(self, filters=None, data=None, args=None, swap=False):
        '''
        Delete rows from table that:
        - match `filters` if set (or that doesn't match `filters` if
          swap is set
        - match `data` based on key columns (or doesn't match if swap is set)
        Only one of `filters` or `data` can be passed.

        `args` is a dict of values that allows to parameterize `filters`.
        '''
        self.validate_key(set(c.name for c in self.field_map))
        if not any((data, filters)):
            qr = 'DELETE FROM "%s"' % self.table.name
            return execute(qr)

        if data and filters:
            raise ValueError('Deletion by both data and filter not supported')

        exp = Expression(self.table, base_env=self.base_env())
        filter_chunks = exp._build_filter_cond(filters)

        if data:
            # Transform rows into columns
            data = list(zip(*data))
            data = list(self.format(data))
            with self._prepare_write(data) as join_cond:
                qr = (
                    'DELETE FROM "%(main)s" WHERE id %(op)s ('
                    'SELECT "%(main)s".id FROM "%(main)s" '
                    'INNER JOIN %(tmp_table)s on %(join_cond)s)'
                )
                qr = qr % {
                    'main': self.table.name,
                    'op': 'NOT IN' if swap else 'IN',
                    'tmp_table': self.tmp_table,
                    'join_cond': ' AND '.join(join_cond),
                }
                cur = execute(qr)

        else:
            qr = (
                'DELETE FROM "%(main_table)s" WHERE id %(op)s ('
                'SELECT "%(main_table)s".id FROM "%(main_table)s"'
            )
            qr = qr % {
                'main_table': self.table.name,
                'op': 'NOT IN' if swap else 'IN',
            }
            chunks = [qr, exp.ref_set]
            if filter_chunks:
                chunks += ['WHERE'] + filter_chunks
            chunks.append(')')
            cur = TankerCursor(self, chunks, args=args).execute()
        return cur.rowcount

    @contextmanager
    def _prepare_write(self, data, filters=None, disable_acl=False, args=None):
        # An id column is needed to enable filters (and for sqlite
        # REPLACE)
        extra_id = 'id' not in self.field_dict
        not_null = lambda fields: (
            'NOT NULL' if any(f in self.key_fields for f in fields) else ''
        )
        # Create tmp
        if ctx.flavor == 'crdb':
            self.tmp_table = 'tmp_' + uuid.uuid4().hex
            qr = 'CREATE TABLE %s (%s)'
        else:
            self.tmp_table = 'tmp'
            qr = 'CREATE TEMPORARY TABLE %s (%s)'
        col_defs = ', '.join(
            '"%s" %s %s' % (col.name, fields[0].ftype, not_null(fields))
            for col, fields in self.field_map.items()
        )
        if extra_id:
            id_type = 'INTEGER' if ctx.flavor == 'sqlite' else 'SERIAL'
            col_defs += ', id %s PRIMARY KEY' % id_type
        qr = qr % (self.tmp_table, col_defs)
        execute(qr)

        # Fill tmp
        if self.ctx.flavor == 'sqlite':
            qr = 'INSERT INTO %(tmp_table)s (%(fields)s) VALUES (%(values)s)'
            qr = qr % {
                'tmp_table': self.tmp_table,
                'fields': ', '.join('"%s"' % c.name for c in self.field_map),
                'values': ', '.join('%s' for _ in self.field_map),
            }
            executemany(qr, zip(*data))
        else:
            columns = ', '.join('"%s"' % c.name for c in self.field_map)
            qr = f'INSERT INTO {self.tmp_table} ({columns}) VALUES %s'
            # Append to writer by row
            nb_params = len(self.field_map)
            execute_values(qr, zip(*data), nb_params)

        # Create join conditions
        join_cond = []
        for name in self.key_cols:
            join_cond.append(
                '%s."%s" = "%s"."%s"'
                % (self.tmp_table, name, self.table.name, name)
            )

        # Apply filters if any
        if not disable_acl:
            filters = filters or []
            acl = self.ctx.cfg.get('acl-write', {})
            filters += acl.get(self.table.name, [])

        self.upd_filter_cnt = 0
        self.ins_filter_cnt = 0
        if filters:
            # Delete line from tmp that invalidate the filter
            self.ins_filter_cnt = self._purge(
                join_cond, filters, disable_acl=True, what='new', args=args
            )
            self.upd_filter_cnt = self._purge(
                join_cond, filters, disable_acl=True, what='old', args=args
            )
        yield join_cond

        # Clean tmp table
        execute('DROP TABLE %s' % self.tmp_table)

    def write(
        self,
        data,
        purge=False,
        insert=True,
        update=True,
        filters=None,
        disable_acl=False,
        args=None,
    ):
        '''
        Write given data to view table. If insert is true, new lines will
        be inserted.  if update is true, existing line will be
        updated. If purge is true existing line that are not present
        in data (and that match filters) will be deleted.

        Returns a dict containing the amount of line _not_ written
        (because of the filter) and the amount of deleted lines (ex:
        `{'filtered': 10, 'deleted': 3}`)
        '''

        # First we have to make sure that fields are properly set for write
        self.validate_key(set(c.name for c in self.field_map))

        # TODO use merge command, see
        # https://www.depesz.com/2018/04/10/waiting-for-postgresql-11-merge-sql-command-following-sql2016/

        # Handle list of dict and dataframes
        if isinstance(data, list) and data and isinstance(data[0], dict):
            data = [
                [record.get(f.name) for record in data] for f in self.fields
            ]
        elif isinstance(data, dict):
            data = [data.get(f.name) for f in self.fields]
        elif pandas and isinstance(data, pandas.DataFrame):
            fields = [f.name for f in self.fields]
            data = [data[f].values for f in fields]
        else:
            # Transform rows into columns
            data = list(zip(*data))
            # Zip wont create a list of empty list if given data is
            # empty:
            if not data:
                data = [[] for _ in self.fields]
        # Format values
        data = list(self.format(data))
        if isinstance(filters, basestring):
            filters = [filters]

        # Launch upsert
        rowcounts = {}
        kwargs = {
            'filters': filters,
            'disable_acl': disable_acl,
            'args': args,
        }
        with self._prepare_write(data, **kwargs) as join_cond:
            disable_upsert = (
                ctx.legacy_pg
                or (ctx.flavor == 'postgresql'
                    and self.table.use_index == 'BRIN'))
            if disable_upsert:
                if insert:
                    self._insert(join_cond)
                if update:
                    self._update(join_cond)
            else:
                # ON-CONFLICT is available since postgres 9.5
                self._upsert(join_cond, insert=insert, update=update)
            if purge:
                cnt = self._purge(
                    join_cond, filters, disable_acl, what='purge', args=args
                )
                rowcounts['deleted'] = cnt

        rowcounts['filtered'] = self.ins_filter_cnt + self.upd_filter_cnt

        self.reset_cache(self.table.name)
        return rowcounts

    def validate_key(self, columns):
        '''
        Make sure the set of columns cover the table key. If not the
        access is not univocal
        '''
        id_col = self.table.get_column('id')
        if not id_col.name in columns:
            missing_key = [c for c in self.table.key if c not in columns]
            if missing_key:
                msg = (
                    'You must reference all the columns composing the table key'
                    ' when you want to write, delete or reference rows (or'
                    ' pass the id column).  Table is "%s", missing columns'
                    ' are: %s' % (self.table.name, ','.join(missing_key))
                )

                raise ValueError(msg)

    def _upsert(self, join_cond, insert, update):
        tmp_fields = ', '.join(
            '%s."%s"' % (self.tmp_table, f.name) for f in self.field_map
        )
        main_fields = ', '.join('"%s"' % f.name for f in self.field_map)
        upd_fields = []
        for f in self.field_map:
            if f.name in self.key_cols:
                continue
            upd_fields.append('"%s" = EXCLUDED."%s"' % (f.name, f.name))

        qr = (
            'INSERT INTO "%(main)s" (%(main_fields)s) '
            'SELECT %(tmp_fields)s FROM %(tmp_table)s '
            '%(join_type)s JOIN "%(main)s" ON ( %(join_cond)s) '
        )
        if upd_fields and update:
            qr += 'ON CONFLICT (%(idx)s) DO UPDATE SET %(upd_fields)s'
        else:
            qr += 'ON CONFLICT (%(idx)s) DO NOTHING'

        qr = qr % {
            'main': self.table.name,
            'main_fields': main_fields,
            'tmp_fields': tmp_fields,
            'tmp_table': self.tmp_table,
            'join_cond': ' AND '.join(join_cond),
            'join_type': 'LEFT' if insert else 'INNER',
            'upd_fields': ', '.join(upd_fields),
            'idx': ', '.join('"%s"' % k for k in self.key_cols),
        }
        return TankerCursor(self, qr).execute()

    def _insert(self, join_cond):
        qr = 'INSERT INTO "%(main)s" (%(fields)s) %(select)s'
        select = (
            'SELECT %(tmp_fields)s FROM %(tmp_table)s '
            'LEFT JOIN "%(main)s" ON ( %(join_cond)s) '
            'WHERE %(where_cond)s'
        )

        # Consider only new rows
        where_cond = []
        for name in self.key_cols:
            where_cond.append('%s."%s" IS NULL' % (self.table.name, name))

        tmp_fields = ', '.join(
            '%s."%s"' % (self.tmp_table, f.name) for f in self.field_map
        )
        select = select % {
            'tmp_fields': tmp_fields,
            'tmp_table': self.tmp_table,
            'main': self.table.name,
            'join_cond': ' AND '.join(join_cond),
            'where_cond': ' AND '.join(where_cond),
        }
        qr = qr % {
            'main': self.table.name,
            'fields': ', '.join('"%s"' % f.name for f in self.field_map),
            'select': select,
        }
        cur = TankerCursor(self, qr).execute()
        return cur.rowcount

    def _update(self, join_cond):
        update_cols = [
            f.name for f in self.field_map if f.name not in self.key_cols
        ]
        if not update_cols:
            return 0

        where = ' AND '.join(join_cond)
        qr = 'UPDATE "%(main)s" SET '
        qr += ', '.join(
            '"%s" = %s."%s"' % (n, self.tmp_table, n) for n in update_cols
        )
        qr += ' FROM %(tmp_table)s WHERE %(where)s'
        qr = qr % {
            'tmp_table': self.tmp_table,
            'main': self.table.name,
            'where': where,
        }
        cur = TankerCursor(self, qr).execute()
        return cur and cur.rowcount or 0

    def _purge(
        self, join_cond, filters, disable_acl=False, what='purge', args=None
    ):
        '''
        Delete rows from main table that are not in tmp table and evaluate
        filters to true. If "what" is 'old' we delte from tmp lines
        that are also in main and that evaluate filter to false. If
        "what" is new we delete from tmp lines that evaluate to false.
        '''

        assert what in ('purge', 'old', 'new')
        new = what == 'new'
        old = what == 'old'
        purge = what == 'purge'
        main = self.table.name
        tmp = self.tmp_table
        if purge:
            main, tmp = tmp, main
        else:
            assert bool(filters), 'filters is needed to purge on tmp'

        # Prepare basic query
        head_qr = (
            'DELETE FROM "%(tmp)s" '
            'WHERE id %(filter_operator)s ('
            ' SELECT "%(tmp)s".id FROM "%(tmp)s" '
        )
        join_qr = '{} JOIN %(main)s on (%(join_cond)s) '.format(
            'INNER' if old else 'LEFT'
        )
        excl_cond = '%(main)s.%(field)s IS NULL' if purge else ''
        tail_qr = ')'

        # Format all parts of the query
        fmt = {
            'main': main,
            'tmp': tmp,
            'filter_operator': 'IN',  #'NOT IN' if update else
            'join_cond': ' AND '.join(join_cond),
            'field': self.key_cols[0],
        }
        head_qr = head_qr % fmt
        join_qr = join_qr % fmt

        excl_cond = excl_cond % fmt

        # Build filters
        acl_filters = None
        if not disable_acl:
            acl_filters = self.ctx.cfg.get('acl-write', {}).get(self.table.name)
        if new:
            # Build aliases (we want evaluate the actual "new" value
            # of tmp and not the "old" values in the main one)
            table_aliases = {c.name: 'tmp' for c in self.field_map}
        else:
            table_aliases = None
        exp = Expression(
            self.table, base_env=self.base_env(), table_aliases=table_aliases
        )
        filter_chunks = exp._build_filter_cond(filters, acl_filters)
        join_chunks = [exp.ref_set]
        if filter_chunks:
            qr = [head_qr] + [join_qr] + join_chunks
            if not purge:
                qr += ['WHERE NOT ('] + filter_chunks + [')']
            else:
                qr += ['WHERE'] + filter_chunks
            if excl_cond:
                qr += ['AND', excl_cond]
            qr += [tail_qr]
        else:
            qr = head_qr + join_qr
            if excl_cond:
                qr += ' WHERE ' + excl_cond
            qr += tail_qr
        cur = TankerCursor(self, qr, args=args).execute()

        return cur.rowcount

    @classmethod
    def reset_cache(cls, table=None):
        if table is None:
            cls._fk_cache = {}
        else:
            for key in list(cls._fk_cache):
                if key[0] == table:
                    del cls._fk_cache[key]

    @classmethod
    def resolve_fk(cls, fields, values):
        remote_table = fields[0].col.get_foreign_table().name
        key = (remote_table,) + fields
        mapping = cls._fk_cache.get(key)
        read_fields = list(cls._fk_fields(fields))
        view = View(remote_table, read_fields + ["id"])

        if mapping is None:
            if "id" not in read_fields:
                cols = set(c.name for c in view.field_map if c.name != "id")
                view.validate_key(cols)  # Make sure we will have a
                # one2one mapping

            db_values = view.read(
                disable_acl=True, limit=LRU_PAGE_SIZE, order=("id", "desc")
            )
            mapping = dict((val[:-1], val[-1]) for val in db_values)

            # Enable lru if fk mapping reach LRU_SIZE
            if len(mapping) == LRU_PAGE_SIZE:
                mapping = LRU(mapping)
            cls._fk_cache[key] = mapping

        if isinstance(mapping, LRU):
            base_filter = "(AND %s)" % " ".join(
                "(= %s {})" % f for f in read_fields
            )

            # Value is a list of column, paginate yield page that is a
            # small chunk of rows
            for page in paginate(values):
                missing = set(
                    val
                    for val in page
                    if not all_none(val) and val not in mapping
                )
                if missing:
                    fltr = "(OR %s)" % " ".join(base_filter for _ in missing)
                    rows = view.read(
                        fltr, args=list(chain(*missing)), disable_acl=True
                    )
                    for row in rows:
                        # row[-1] is id
                        mapping.set(row[:-1], row[-1])
                for val in cls._emit_fk(page, mapping, remote_table):
                    yield val

        else:
            for val in cls._emit_fk(zip(*values), mapping, remote_table):
                yield val

    @classmethod
    def _fk_fields(cls, fields):
        for field in fields:
            yield field.desc.split(".", 1)[1]

    @classmethod
    def _emit_fk(cls, values, mapping, remote_table):
        for val in values:
            if all_none(val):
                yield None
                continue
            res = mapping.get(val)
            if res is None:
                raise ValueError(
                    'Values (%s) are not known in table "%s"'
                    % (", ".join(map(repr, val)), remote_table)
                )
            yield res


def fetch(tablename, filter_by):
    columns = [c.name for c in Table.get(tablename).own_columns]
    view = View(tablename, ['id'] + columns)
    values = view.read(filters=filter_by).one()
    if values is None:
        return
    keys = (f.name for f in view.fields)
    return dict(zip(keys, values))


def save(tablename, data):
    fields = data.keys()
    view = View(tablename, list(fields))
    view.write([data])
