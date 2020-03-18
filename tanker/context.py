from collections import defaultdict, OrderedDict
from contextlib import contextmanager
from itertools import groupby, chain
from urllib.parse import urlparse, urlunparse
import json
import logging
import os
import re
import sqlite3
import textwrap
import threading

try:
    import psycopg2
    from psycopg2.pool import ThreadedConnectionPool
    from psycopg2 import extras
except ImportError:
    psycopg2 = None

from .expression import ExpressionSymbol, AST, ReferenceSet
from .table import Column, Table
from .utils import (logger, basestring, yaml_load, CTX_STACK, ctx, pandas,
                    COLUMN_TYPE)

QUOTE_SEPARATION = re.compile(r"(.*?)('.*?')", re.DOTALL)
NAMED_RE = re.compile(r"%\(([^\)]+)\)s")
PG_POOLS = {}
DEFAULT_DB_URI = "sqlite:///:memory:"


def convert_array(kind):
    def converter(s):
        # Strip { and }
        s = s[1:-1]
        return [kind(i) for i in s.decode("utf-8").split(",")]

    return converter


def log_sql(query, params=None, exception=False):
    if not exception and logger.getEffectiveLevel() > logging.DEBUG:
        return
    indent = "  "
    query = textwrap.fill(
        query, initial_indent=indent, subsequent_indent=indent
    )
    if params is None:
        args = ("SQL Query:\n%s", query)
    else:
        params = str(params)
        if len(params) > 1000:
            params = params[:1000] + "..."
        args = ("SQL Query:\n%s\nSQL Params:\n%s%s", query, indent, params)

    if exception:
        logger.error(*args)
    else:
        logger.debug(*args)


# Build tuple of backend exceptions we want to catch
DB_EXCEPTION = (sqlite3.OperationalError,)
if psycopg2 is not None:
    DB_EXCEPTION += (psycopg2.ProgrammingError,)


class DBError(Exception):
    pass


def execute(query, params=None):
    log_sql(query, params)
    query = ctx._prepare_query(query)
    cursor = ctx.connection.cursor()
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
    except DB_EXCEPTION as e:
        log_sql(query, params, exception=True)
        raise DBError(e)
    return cursor


def executemany(query, params):
    query = ctx._prepare_query(query)
    log_sql(query, params)
    cursor = ctx.connection.cursor()
    try:
        cursor.executemany(query, params)
    except DB_EXCEPTION as e:
        log_sql(query, params, exception=True)
        raise DBError(e)
    return cursor


def execute_values(query, values, nb_params):
    log_sql(query)
    cursor = ctx.connection.cursor()
    template = '(%s)' % ', '.join('%s' for _ in range(nb_params))
    try:
        extras.execute_values(
            cursor,
            query,
            values,
            page_size=1000,
            template=template,
        )
    except DB_EXCEPTION as e:
        log_sql(query, exception=True)
        raise DBError(e)
    return cursor


def copy_from(qr, buff):
    log_sql(qr)
    cursor = ctx.connection.cursor()
    cursor.copy_expert(qr, buff)
    return cursor


class TankerThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        if CTX_STACK._local.contexts:
            # Capture current context if any
            self.stack = [ctx.clone()]
        else:
            self.stack = []
        super(TankerThread, self).__init__(*args, **kwargs)

    def run(self):
        CTX_STACK.reset(self.stack)
        super(TankerThread, self).run()


class Pool:

    _pools = {}

    def __init__(self, cfg):
        db_uri = cfg.get("db_uri", DEFAULT_DB_URI)
        self.cfg = cfg
        uri = urlparse(db_uri)
        dbname = uri.path[1:]
        self.flavor = uri.scheme
        self.pg_schema = None
        if self.flavor == "sqlite":
            self.conn_args = [dbname]
            self.conn_kwargs = {
                "check_same_thread": False,
                "detect_types": sqlite3.PARSE_DECLTYPES,
                "isolation_level": "DEFERRED",
            }
            sqlite3.register_converter("JSONB", json.loads)
            sqlite3.register_converter("INTEGER[]", convert_array(int))
            sqlite3.register_converter("VARCHAR[]", convert_array(str))
            sqlite3.register_converter("FLOAT[]", convert_array(float))
            sqlite3.register_converter(
                "BOOL[]", convert_array(lambda x: x == "True")
            )

        elif self.flavor == "postgresql":
            self.pg_schema = uri.fragment
            if psycopg2 is None:
                raise ImportError(
                    'Cannot connect to "%s" without psycopg2 package '
                    "installed" % db_uri
                )

            con_info = "dbname='%s' " % dbname
            if uri.hostname:
                con_info += "host='%s' " % uri.hostname
            if uri.username:
                con_info += "user='%s' " % uri.username
            if uri.password:
                con_info += "password='%s' " % uri.password
            if uri.port:
                con_info += "port='%s' " % uri.port

            self.pg_pool = ThreadedConnectionPool(
                cfg.get("pg_min_pool_size", 1),
                cfg.get("pg_max_pool_size", 10),
                con_info,
            )
        elif self.flavor == "crdb":
            if psycopg2 is None:
                raise ImportError(
                    'Cannot connect to "%s" without psycopg2 package '
                    "installed" % db_uri
                )
            # transform crdb into postgreql in uri scheme to please
            # psycopg2
            uri_parts = list(uri)
            uri_parts[0] = "postgresql"
            self.db_uri = urlunparse(uri_parts)

        else:
            raise ValueError(
                'Unsupported scheme "%s" in uri "%s"' % (uri.scheme, uri)
            )

    def enter(self):
        if self.flavor == "sqlite":
            connection = sqlite3.connect(*self.conn_args, **self.conn_kwargs)
            connection.text_factory = str
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA journal_mode=wal")
        elif self.flavor == "crdb":
            connection = psycopg2.connect(self.db_uri)
        elif self.flavor == "postgresql":
            connection = self.pg_pool.getconn()
            if self.pg_schema:
                qr = "SET search_path TO %s" % self.pg_schema
                connection.cursor().execute(qr)

        else:
            raise ValueError('Unexpected flavor "%s"' % self.flavor)
        return connection

    def leave(self, connection, exc=None):
        if exc:
            logger.debug("ROLLBACK")
            connection.rollback()
        else:
            logger.debug("COMMIT")
            connection.commit()
        if self.flavor == "postgresql":
            self.pg_pool.putconn(connection)
        else:
            connection.close()

    @classmethod
    def disconnect(cls):
        for pool in cls._pools.values():
            if pool.flavor == "postgresql":
                pool.pg_pool.closeall()
        cls.clear()

    @classmethod
    def clear(cls):
        cls._pools = {}

    @classmethod
    def get_pool(cls, cfg):
        db_uri = cfg.get("db_uri", DEFAULT_DB_URI)
        pool = cls._pools.get(db_uri)
        if pool:
            # Return existing pool for current db if any
            return pool

        pool = Pool(cfg)
        cls._pools[db_uri] = pool
        return pool


class Context:

    _registries = {}

    def __init__(self, cfg):
        self.db_uri = cfg.get("db_uri", DEFAULT_DB_URI)
        self.encoding = cfg.get("encoding", "utf-8")
        self.cfg = cfg
        self.aliases = {"null": None}
        self.db_tables = set()
        self.db_columns = defaultdict(OrderedDict)
        self.db_constraints = set()
        self.db_indexes = set()
        self.referenced = set()

    def enter(self):
        # Share pool registry
        self.pool = Pool.get_pool(self.cfg)
        self.connection = self.pool.enter()
        self.flavor = self.pool.flavor
        self.pg_schema = self.pool.pg_schema
        self.legacy_pg = False
        if self.flavor == "postgresql":
            self.legacy_pg = self.connection.server_version < 90500

        self.registry = Context._registries.get(self.db_uri)
        if not self.registry:
            # Load schema as yaml if a string is given and as a file
            # if the path exists
            schema = self.cfg.get("schema")
            if isinstance(schema, basestring):
                if os.path.exists(schema):
                    schema = yaml_load(open(schema))
                else:
                    schema = yaml_load(schema)
            if not schema:
                schema = self.introspect_db(auto=True)
            # Register tables
            self.registry = OrderedDict()
            for table_def in schema:
                self.register(table_def)

            Context._registries[self.db_uri] = self.registry
        # Discover which table are referenced
        if not self.referenced:
            self.referenced = set(
                col.foreign_table
                for t in self.registry.values()
                for col in t.columns
                if col.ctype == "M2O"
            )

    def leave(self, exc=None):
        self.pool.leave(self.connection, exc)

    def clone(self):
        """
        Create a copy of self, will trigger instanciation of a new cursor
        (the connection is shared)
        """
        new_ctx = Context(self.cfg)
        new_ctx.aliases = self.aliases
        new_ctx.db_columns = self.db_columns
        new_ctx.db_tables = self.db_tables
        new_ctx.db_constraints = self.db_constraints
        new_ctx.db_indexes = self.db_indexes
        new_ctx.registry = self.registry
        new_ctx.referenced = self.referenced
        new_ctx.flavor = self.flavor
        new_ctx.connection = self.connection
        new_ctx.cfg = self.cfg.copy()
        return new_ctx

    def _prepare_query(self, query):
        if self.flavor != "sqlite":
            return query

        # Tranform named params: %(foo)s -> :foo
        query = NAMED_RE.sub(r":\1", query)

        # Transform positional params: %s -> ?. s/ilike/like.
        buf = ""
        for nquote, quote in QUOTE_SEPARATION.findall(query + "''"):
            nquote = nquote.replace("?", "??")
            nquote = nquote.replace("%s", "?")
            nquote = nquote.replace("ilike", "like")
            buf += nquote + quote
        query = buf[:-2]
        return query

    def register(self, table_def):
        table_name = table_def["table"]
        table = self.registry.get(table_name)
        if table is not None:
            return table

        values = table_def.get("values")
        defaults = table_def.get("defaults", {})
        columns = []
        for col_name, col_type in table_def["columns"].items():
            new_col = Column(col_name, col_type, default=defaults.get(col_name))
            columns.append(new_col)

        table = Table(
            name=table_name,
            columns=columns,
            key=table_def.get("key", table_def.get("index")),
            unique=table_def.get("unique"),
            values=values,
            use_index=table_def.get("use-index"),
        )
        self.registry[table_name] = table
        return table

    def introspect_db(self, auto=False):
        """
        Collect info from existing db. this populate self.db_table,
        self.db_indexes, self.db_columns and self.db_constraints.

        if `auto` is True, build automatically the schema (and so
        query the db to get foreign keys and unique indexes)
        """

        # Collect table info
        if self.flavor == "sqlite":
            qr = "SELECT name FROM sqlite_master WHERE type = 'table'"
        else:
            qr = (
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = '%s'" % (self.pg_schema or "public")
            )
        self.db_tables = set(name for name, in execute(qr))

        # Collect columns
        self.db_columns = {}
        if self.flavor == "sqlite":
            for table_name in self.db_tables:
                qr = 'PRAGMA table_info("%s")' % table_name
                cursor = execute(qr)
                current_cols = {x[1]: x[2].upper() for x in cursor}
                self.db_columns[table_name] = current_cols
        else:
            qr = """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns ORDER BY table_name
            """
            cursor = execute(qr)
            for t, cols in groupby(cursor, key=lambda x: x[0]):
                current_cols = {x[1]: x[2].upper() for x in cols}
                self.db_columns[t] = current_cols

        # Collect indexes
        if self.flavor == "sqlite":
            qr = "SELECT name FROM sqlite_master WHERE type = 'index'"
        else:
            schema = self.pg_schema or "public"
            qr = (
                "SELECT indexname FROM pg_indexes "
                "WHERE schemaname = '%s'" % schema
            )
        self.db_indexes = set(name for name, in execute(qr))

        # Collect constraints
        if self.flavor != "sqlite":
            qr = (
                "SELECT constraint_name "
                "FROM information_schema.table_constraints"
            )
            self.db_constraints = set(name for name, in execute(qr))

        if not auto:
            return

        foreign_keys = {}
        if self.flavor == "sqlite":
            # Example invocation of fk pragma:
            #  sqlite> PRAGMA foreign_key_list(member);
            #  id|seq|table|from|to|on_update|on_delete|match
            #  0|0|team|team|id|NO ACTION|NO ACTION|NONE
            qr = 'PRAGMA foreign_key_list("%s")'
            for table_name in self.db_tables:
                cur = execute(qr % table_name)
                foreign_keys.update(
                    {(table_name, r[3]): (r[2], r[4]) for r in cur}
                )

        else:
            # Extract fk
            qr = """
            SELECT
              tc.table_name,
              kcu.column_name,
              ccu.table_name AS foreign_table_name,
              ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage
              AS kcu ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage
              AS ccu ON ccu.constraint_name = tc.constraint_name
            WHERE constraint_type = 'FOREIGN KEY';
            """
            cur = execute(qr)
            foreign_keys.update({(r[0], r[1]): (r[2], r[3]) for r in cur})

        # Extract unique indexes
        if self.flavor == "sqlite":
            keys = defaultdict(list)
            list_qr = "PRAGMA index_list('%s')"
            info_qr = "PRAGMA index_info('%s')"
            for table in self.db_tables:
                for _, idx_name, uniq, _, _ in execute(list_qr % table):
                    if not uniq:
                        continue
                    by_pos = lambda x: x[0]
                    rows = sorted(execute(info_qr % idx_name), key=by_pos)
                    keys[table] = [r[2] for r in rows]
                    break

        else:
            qr = """
            SELECT
              t.relname as table_name,
              i.relname as index_name,
              a.attname as column_name,
              ix.indkey as idx_col,
              a.attnum as col_pos
            FROM
              pg_class t,
              pg_class i,
              pg_index ix,
              pg_attribute a
            WHERE
              t.oid = ix.indrelid
              AND i.oid = ix.indexrelid
              AND a.attrelid = t.oid
              AND a.attnum = ANY(ix.indkey)
              AND t.relkind = 'r'
              AND ix.indisunique
              AND not ix.indisprimary
            """
            rows = list(execute(qr))
            # Sort by index size and column position in index
            col_pos = lambda x: (
                len(x[3].split()),
                x[3].split().index(str(x[4])),
            )
            rows = sorted(rows, key=col_pos)
            keys = defaultdict(list)
            indexes = {}
            for table, index_name, col_name, _, _ in rows:
                if table in indexes:
                    # Keep only first unique index
                    if indexes[table] != index_name:
                        continue
                else:
                    indexes[table] = index_name
                keys[table].append(col_name)

        # Glue everything together in schema
        type_map = {
            "CHARACTER VARYING": "varchar",
            "TIMESTAMP WITHOUT TIME ZONE": "timestamp",
            "DOUBLE PRECISION": "float",
            "REAL": "float",
            "BOOLEAN": "bool",
            "TEXT": "varchar",
            "BIGINT": "bigint",
            "INTEGER": "integer",
            "DATE": "date",
            "REAL": "float",
            "SMALLINT": "integer",
            "NUMERIC": "float",
        }
        ## TODO convert ARRAY SOMETHING into SOMETHING[]

        schema = []
        for table_name in self.db_tables:
            table_cfg = {
                "table": table_name,
                "columns": OrderedDict(),
                "key": keys.get(table_name, "id"),
            }
            schema.append(table_cfg)
            for name, data_type in self.db_columns[table_name].items():
                if (table_name, name) in foreign_keys:
                    remote_table, remote_col = foreign_keys[table_name, name]
                    col_def = "M2O %s.%s" % (remote_table, remote_col)
                elif data_type in type_map:
                    col_def = type_map[data_type]
                elif data_type in COLUMN_TYPE:
                    col_def = data_type
                else:
                    continue  # We don't know what to do with this type
                table_cfg["columns"][name] = col_def

        return schema

    def create_table(self, table, full=True):
        """
        Create table in database (if it doesn't already exist) based on
        `table` object. If full is true, also create columns, indexes
        and sync values.
        """
        if table.name in self.db_tables:
            return

        self.db_tables.add(table.name)
        self.db_columns[table.name] = {}
        col_defs = []
        for col in table.columns:
            # TODO we may consider m2o here if the target table is
            # already in db
            if col.ctype in ("M2O", "O2M"):
                continue
            col_def = col.sql_definition()
            if col.name in table.key:
                col_def += " NOT NULL"  # XXX allow nullable but fall
                # back to pg_legacy writes to
                # avoid duplicates (and adapt
                # join_cond in _prepare_query
                # to use 'left = right or left
                # is null and right is null')
            col_defs.append('"%s" %s' % (col.name, col_def))
            self.db_columns[table.name][col.name] = col.ctype.upper()

        qr = 'CREATE TABLE "%s" (%s)' % (table.name, ", ".join(col_defs))
        execute(qr)
        logger.info('Table "%s" created', table.name)

        if not full:
            return

        self.add_columns(table)
        self.create_index(table)
        self.sync_data(table)

    def add_columns(self, table):
        """
        Alter database table to add missing columns (wrt to `table`
        object)
        """
        table_cols = self.db_columns[table.name]
        # Execute alter table queries
        table = self.registry[table.name]
        for col in table.own_columns:
            if col.name in table_cols:
                continue
            table_cols[col.name] = col.ctype
            qr = 'ALTER TABLE "%(table)s" ' 'ADD COLUMN "%(name)s" %(def)s'
            col_def = col.sql_definition()
            if col.name in table.key and self.flavor != "sqlite":
                # FIXME sqlite does not allow to add not null columns
                # without a default value (even on empty tables!)
                col_def += " NOT NULL"
            params = {
                "table": table.name,
                "name": col.name,
                "def": col_def,
            }
            execute(qr % params)

    def create_index(self, table):
        # Add unique constrains (not supported by sqlite)
        if self.flavor != "sqlite":
            unique_qr = 'ALTER TABLE "%s" ADD CONSTRAINT %s UNIQUE (%s)'
            for cols in table.unique:
                cons_name = "unique_" + "_".join(cols)
                if len(cons_name) > 63:
                    msg = 'Constrain name "%s" is too big'
                    ValueError(msg % cons_name)
                if cons_name in self.db_constraints:
                    continue
                self.db_constraints.add(cons_name)
                cons_cols = ", ".join(cols)
                execute(unique_qr % (table.name, cons_name, cons_cols))

        if not table.key:
            return

        # TODO don't automatically define table key as btree index,
        # but use a unique constraint and a brin idx, this potentially
        # work with upsert statement

        use_brin = (
            self.flavor == "postgresql"
            and not self.legacy_pg
            and table.use_index == "BRIN"
        )
        if use_brin:
            idx = "brin_index_%s" % table.name
        else:
            idx = "unique_index_%s" % table.name

        if idx in self.db_indexes:
            return
        self.db_indexes.add(idx)

        cols = ", ".join('"%s"' % c for c in table.key)
        if use_brin:
            tpl = 'CREATE INDEX "%s" ON "%s" USING BRIN (%s)'
        else:
            tpl = 'CREATE UNIQUE INDEX "%s" ON "%s" (%s)'
        qr = tpl % (idx, table.name, cols)
        execute(qr)

    def sync_data(self, table):
        from .view import View

        if not table.values:
            return
        logger.info("Populate %s" % table.name)
        view = View(table.name, fields=list(table.values[0].keys()))
        view.write(table.values, disable_acl=True)

    def create_tables(self):
        # Make sur schema exists
        if self.pg_schema:
            execute("CREATE SCHEMA IF NOT EXISTS %s" % self.pg_schema)

        # First we collect db info
        self.introspect_db()

        # Create tables and simple columns
        for table in self.registry.values():
            self.create_table(table, full=False)

        # Add columns
        for table in self.registry.values():
            self.add_columns(table)

        # Create indexes
        for table in self.registry.values():
            self.create_index(table)

        # Add pre-defined data
        for table in self.registry.values():
            self.sync_data(table)


class TankerCursor:
    def __init__(self, view, chunks, args=None):
        self.view = view
        self.db_cursor = None
        if isinstance(chunks, basestring):
            chunks = [chunks]
        self.chunks = chunks
        if isinstance(args, dict):
            self._kwargs = args
            self._args = None
        else:
            self._args = args and args[:]
            self._kwargs = None

    def args(self, *args, **kwargs):
        """
        Set args for current cursor
        """
        self._args = list(args)
        self._kwargs = kwargs
        # reset db_cursor to allow to call args & re-launch query
        self.db_cursor = None
        return self

    def execute(self):
        if self.db_cursor is not None:
            return self.db_cursor

        qr, args = self.expand()
        self.db_cursor = execute(qr, args)
        return self.db_cursor

    def executemany(self):
        if self.db_cursor is not None:
            return self.db_cursor

        qr, args = self.expand()
        self.db_cursor = executemany(qr, args)
        return self.db_cursor

    def __iter__(self):
        return self.execute()

    def split(self, x):
        if isinstance(x, ReferenceSet):
            # Delay evaluation of refset
            return " ".join(x.get_sql_joins()), None
        if isinstance(x, ExpressionSymbol):
            return x.eval(), None
        if isinstance(x, (AST)):
            # TODO kwargs should be evaled earlier
            kwargs = self.view.ctx.aliases.copy()
            kwargs.update(self._kwargs or {})
            cfg = ctx.cfg
            kwargs.update(cfg)
            return x.eval(self._args, kwargs), x.params
        if isinstance(x, tuple):
            return x
        if isinstance(x, basestring):
            return x, None

        raise ValueError('Unable to stringify "%s"' % x)

    def expand(self):
        queries, args = zip(*map(self.split, self.chunks))
        qr = " ".join(queries)
        chained_args = chain.from_iterable(a for a in args if a)
        return qr, tuple(chained_args)

    def __next__(self):
        return next(iter(self))

    def one(self):
        return next(iter(self), None)

    def next(self):
        return next(iter(self))

    def all(self):
        return list(self)

    def chain(self):
        items = iter(self)
        return chain(*items)

    def dict(self):
        keys = [f.name for f in self.view.fields]
        for row in self:
            yield dict(zip(keys, row))

    def df(self):
        if not pandas:
            raise ImportError("The pandas module is required by Cursor.df")
        read_columns = [f.name for f in self.view.fields]
        df = pandas.DataFrame.from_records(self, columns=read_columns)
        return df


def connect(cfg=None, action=None, _auto_rollback=False):
    if not action:

        @contextmanager
        def cm(cfg):
            new_ctx = CTX_STACK.push(cfg, Context(cfg))
            try:
                yield new_ctx
            except Exception as exc:
                CTX_STACK.pop(exc)
                raise
            else:
                CTX_STACK.pop(_auto_rollback)

        return cm(cfg)

    if action == "enter":
        return CTX_STACK.push(cfg, Context(cfg))

    elif action == "leave":
        CTX_STACK.pop()

    else:
        raise ValueError('Unexpected value "%s" for action parameter' % action)


def create_tables():
    ctx.create_tables()


# Little helpers
def enter(db_uri=None, schema=None):
    return connect({"db_uri": db_uri, "schema": schema}, "enter")


def leave(db_uri=None):
    return connect({"db_uri": db_uri}, "leave")
