from collections import OrderedDict
from string import Formatter
import shlex

from .table import Table
from .utils import interleave, basestring, ctx


class Reference:
    def __init__(self, remote_table, remote_field, rjoins, join_alias, column):
        self.remote_table = remote_table
        self.remote_field = remote_field
        self.rjoins = rjoins
        self.join_alias = join_alias
        self.column = column

    def __repr__(self):
        return "<Reference table=%s field=%s>" % (
            self.remote_table.name,
            self.remote_field,
        )


class ReferenceSet:
    def __init__(self, exp, table_aliases=None, parent=None, disable_acl=False):
        """
        A ReferenceSet helps to 'browse' across table by joining them. The
        ReferenceSet hold the set of joins that has to be done to
        resolve the cols that were added through the add() method.
        """
        self.exp = exp
        self.table = exp.table
        self.table_aliases = table_aliases or self.table.name
        self.joins = OrderedDict()
        self.references = []
        self.parent = parent
        self.children = []
        self.disable_acl = disable_acl
        if parent:
            parent.children.append(self)

    def table_alias(self, column=None):
        """
        Return which alias to use based on the given columns
        """
        if isinstance(self.table_aliases, str):
            return self.table_aliases
        if isinstance(self.table_aliases, dict):
            if not column:
                return self.table.name
            return self.table_aliases.get(column, self.table.name)

    def get_sql_joins(self):
        for key, alias in self.joins.items():
            left_table, right_table, left_col, right_col = key
            join = 'LEFT JOIN "%s" AS "%s"' % (right_table, alias)
            cond = '"%s"."%s" = "%s"."%s"' % (
                left_table,
                left_col,
                alias,
                right_col,
            )
            # # TODO inject acl_cond in join cond
            # if not self.disable_acl:
            #     acl_filters = ctx.cfg.get('acl-read', {}).get(right_table)
            #     exp = Expression(Table.get(right_table), parent=self.exp)
            #     acl_cond = exp._build_filter_cond(acl_filters)
            yield join + " ON (" + cond + ")"

    def add(self, desc):
        ref = self.get_ref(desc)
        self.references.append(ref)
        return ref

    def get_ref(self, desc, table=None, force_alias=None):
        table = table or self.table
        left_table = force_alias
        # Simple col, return
        if "." not in desc:
            col = table.get_column(desc)
            left_table = left_table or self.table_alias(col.name)
            return Reference(table, desc, self.joins, left_table, col)

        # Resolve column
        head, tail = desc.split(".", 1)
        rel = table.get_column(head)
        foreign_table = rel.get_foreign_table()

        # Compute join
        left_table = left_table or self.table_alias(head)
        right_table = foreign_table.name

        if rel.ctype == "M2O":
            left_col = head
            right_col = rel.foreign_col
        else:
            # O2M, defined like other_table.fk
            fk = rel.foreign_col
            # left_col is the column pointed by the fk
            left_col = foreign_table.get_column(fk).foreign_col
            right_col = fk

        key_alias = "%s_%s" % (right_table, self.get_nb_joins())
        key = (left_table, right_table, left_col, right_col)
        foreign_alias = self.joins.setdefault(key, key_alias)

        # Recurse
        return self.get_ref(
            tail, table=foreign_table, force_alias=foreign_alias
        )

    def get_nb_joins(self, up=True):
        if up and self.parent:
            return self.parent.get_nb_joins()
        cnt = len(self.joins)
        for child in self.children:
            cnt += child.get_nb_joins(up=False)
        return cnt

    def __iter__(self):
        return iter(self.references)

    def __repr__(self):
        return "<ReferenceSet [%s]>" % ", ".join(map(str, self.references))


class Expression(object):
    # Inspired by http://norvig.com/lispy.html

    builtins = {
        "+": lambda *xs: "(%s)" % " + ".join(xs),
        "-": lambda *xs: "- %s" % xs[0]
        if len(xs) == 1
        else "(%s)" % " - ".join(xs),
        "*": lambda *xs: "(%s)" % " * ".join(xs),
        "/": lambda *xs: "(%s)" % " / ".join(xs),
        "and": lambda *xs: "(%s)" % " AND ".join(xs),
        "or": lambda *xs: "(%s)" % " OR ".join(xs),
        ">=": lambda x, y: "%s >= %s" % (x, y),
        "<=": lambda x, y: "%s <= %s" % (x, y),
        "=": lambda x, y: "%s = %s" % (x, y),
        ">": lambda x, y: "%s > %s" % (x, y),
        "<": lambda x, y: "%s < %s" % (x, y),
        "!=": lambda x, y: "%s != %s" % (x, y),
        "->>": lambda x, y: "%s ->> %s" % (x, y),
        "like": lambda x, y: "%s like %s" % (x, y),
        "ilike": lambda x, y: "%s ilike %s" % (x, y),
        "in": lambda *xs: ("%%s in (%s)" % (", ".join("%s" for _ in xs[1:])))
        % xs,
        "notin": lambda *xs: (
            "%%s not in (%s)" % (", ".join("%s" for _ in xs[1:]))
        )
        % xs,
        "any": lambda x: "any(%s)" % x,
        "all": lambda x: "all(%s)" % x,
        "unnest": lambda x: "unnest(%s)" % x,
        "is": lambda x, y: "%s is %s" % (x, y),
        "isnot": lambda x, y: "%s is not %s" % (x, y),
        "not": lambda x: "not %s" % x,
        "exists": lambda x: "EXISTS (%s)" % x,
        "where": lambda *x: "WHERE " + " AND ".join(x),
        "select": lambda *x: "SELECT " + ", ".join(x),
        "select-distinct": lambda *x: "SELECT DISTINCT " + ", ".join(x),
        "cast": lambda x, y: "CAST (%s AS %s)" % (x, y),
        "date_trunc": lambda x, y: "date_trunc(%s, %s)" % (x, y),
        "extract": lambda x, y: "EXTRACT (%s FROM %s)" % (x, y),
        "floor": lambda x: "floor(%s)" % x,
        "true": lambda: "1" if ctx.flavor == "sqlite" else "true",
        "false": lambda: "0" if ctx.flavor == "sqlite" else "false",
        "strftime": lambda x, y: "strftime(%s, %s)" % (x, y),
    }

    aggregates = {
        "avg": lambda *x: "avg(%s)" % x,
        "count": lambda *x: "count(%s)" % ", ".join(x or ["*"]),
        "max": lambda *x: "max(%s)" % x,
        "min": lambda *x: "min(%s)" % x,
        "sum": lambda *x: "sum(%s)" % x,
        "bool_and": lambda *x: "bool_and(%s)" % x,
        "bool_or": lambda *x: "bool_or(%s)" % x,
        "every": lambda *x: "every(%s)" % x,
    }

    def __init__(
        self,
        table,
        ref_set=None,
        parent=None,
        table_aliases=None,
        disable_acl=False,
        base_env=None,
    ):
        assert isinstance(table, Table)
        self.table = table
        self.env = base_env or {}
        self.builtins = {"from": self._sub_select}
        self.builtins.update(Expression.builtins)
        self.builtins.update(Expression.aggregates)
        # Inject user-defined aliases
        self.parent = parent

        # Add refset
        if not ref_set:
            parent_rs = parent and parent.ref_set
            ref_set = ReferenceSet(
                self,
                table_aliases=table_aliases,
                parent=parent_rs,
                disable_acl=disable_acl,
            )
        self.ref_set = ref_set

    def _sub_select(self, *items):
        select = items[0]
        tail = " ".join(items[1:])
        from_ = 'FROM "%s"' % (self.ref_set.table_alias())
        joins = " ".join(self.ref_set.get_sql_joins())

        items = (select, from_, joins, tail)
        return " ".join(it for it in items if it)

    def parse(self, exp):
        lexer = shlex.shlex(exp)
        lexer.wordchars += ".!=<>:{}-"
        tokens = list(lexer)
        ast = self.read(tokens)
        return ast

    def read(self, tokens, top_level=True, first=False):
        if len(tokens) == 0:
            raise SyntaxError("unexpected EOF while reading")
        token = tokens.pop(0)
        if token == "(":
            L = []
            exp = self
            if tokens[0].upper() == "FROM":
                from_ = tokens.pop(0)  # pop off 'from'
                tbl_name = tokens.pop(0)
                exp = Expression(Table.get(tbl_name), parent=self)
                L.append(ExpressionSymbol(from_, exp, first=True))
            first = True
            while tokens[0] != ")":
                L.append(exp.read(tokens, top_level=False, first=first))
                first = False
            tokens.pop(0)  # pop off ')'
            if tokens and top_level:
                raise ValueError('Unexpected tokens after ending ")"')
            return AST(L, exp)
        elif token == ")":
            raise SyntaxError("unexpected )")
        elif token in self.env and not first:
            desc = self.env[token].desc
            if desc != token and desc[0] == "(":
                return self.parse(desc)

        return self.atom(token, first=first)

    def atom(self, token, first=False):
        """
        Parse the token and try to identify it as param, int, float or
        symbol. The 'first' argument tells if the token if the first item
        in the expression (aka just after a '(').
        """
        for q in ('"', "'"):
            if token[0] == q and token[-1] == q:
                return token[1:-1]

        if len(token) > 1 and token[0] == "{" and token[-1] == "}":
            return ExpressionParam(token[1:-1])

        try:
            return int(token)
        except ValueError:
            pass
        try:
            return float(token)
        except ValueError:
            return ExpressionSymbol(token, self, first=first)

    def _build_filter_cond(self, *filters):
        res = []
        for fltr in filters:
            if not fltr:
                continue

            # filters can be a dict
            if isinstance(fltr, dict):
                # Add simple equal conditions
                for key, val in fltr.items():
                    ast = self.parse("(= %s {}) " % key)
                    ast.args = [val]
                    res.append(ast)
                continue

            # Filters can be a query string or a list of query string
            if isinstance(fltr, basestring):
                fltr = [fltr]
            # Parse expression filters
            for line in fltr:
                ast = self.parse(line)
                res.append(ast)

        return list(interleave(" AND ", res))


class ExpressionSymbol:
    def __init__(self, token, exp, first=False):
        self.token = token
        self.params = []
        self.ref = None
        self.builtin = None
        ref = None
        if self.token.startswith("_parent."):  # XXX replace with '_.' ?
            tail = self.token
            parent = exp
            while tail.startswith("_parent."):
                head, tail = tail.split(".", 1)
                parent = parent.parent
            try:
                ref = parent.ref_set.add(tail)
            except KeyError:
                pass
        elif first:
            self.builtin = exp.builtins.get(self.token.lower(), self.token)
            return
        elif self.token in exp.env:
            val = exp.env[self.token]
            ref = exp.ref_set.add(val.desc)
        else:
            try:
                ref = exp.ref_set.add(self.token)
            except KeyError:
                self.builtin = self.token

        self.ref = ref

    def eval(self):
        if self.ref:
            return '"%s"."%s"' % (self.ref.join_alias, self.ref.remote_field)
        return self.builtin

    def __repr__(self):
        return '<ExpressionSymbol "%s">' % self.token


class ExpressionParam:
    def __init__(self, token):
        self.token = token
        self.key = ""
        self.tail = ""

        self.fmt_spec = self.conversion = None
        if ":" in token:
            token, self.fmt_spec = token.split(":", 1)

        if "!" in token:
            token, self.conversion = token.split("!", 1)

        dotted = token.split(".", 1)
        self.key, self.tail = dotted[0], dotted[1:]

    def eval(self, ast, env):
        # Get value from env
        try:
            as_int = int(self.key)
        except ValueError:
            as_int = None

        if self.key == "":
            value = ast.args.pop(0)
        elif as_int is not None:
            value = ast.args[as_int]
        else:
            value = (
                ast.kwargs[self.key]
                if self.key in ast.kwargs
                else env[self.key]
            )

        # Resolve dotted expression
        for attr in self.tail:
            if isinstance(value, dict):
                value = value[attr]
            else:
                value = getattr(value, attr)

        # Formating
        if self.fmt_spec:
            value = ast.formatter.format_field(value, self.fmt_spec)
        if self.conversion:
            value = ast.formatter.convert_field(value, self.conversion)
        return value


class AST(object):

    formatter = Formatter()

    def __init__(self, atoms, exp):
        self.atoms = atoms
        self.exp = exp
        self.params = []
        self.args = []
        self.kwargs = {}

    def eval(self, args=None, kwargs=None, params=None):
        self.params = params if params is not None else self.params
        self.args = args if args else self.args
        self.kwargs = kwargs or self.kwargs

        # Eval ast wrt to env
        res = self._eval(self.atoms, self.exp.env)
        return res

    def _eval(self, atom, env):
        if isinstance(atom, ExpressionSymbol):
            return atom.eval()

        elif isinstance(atom, ExpressionParam):
            value = atom.eval(self, env)
            return self.emit_literal(value)

        elif isinstance(atom, AST):
            return atom.eval(self.args, self.kwargs, self.params)

        elif not isinstance(atom, list):
            return self.emit_literal(atom)

        else:
            head = atom.pop(0)
            head = self._eval(head, env)
            params = []
            for x in atom:
                val = self._eval(x, env)
                params.append(val)
            if callable(head):
                head = head(*params)
            return head

    def emit_literal(self, x):
        # Collect literal and return placeholder
        if isinstance(x, (tuple, list, set)):
            self.params.extend(x)
            return ", ".join("%s" for _ in x)
        self.params.append(x)
        return "%s"

    def __repr__(self):
        return "<AST [%s]>" % " ".join(map(str, self.atoms))

    def is_aggregate(self):
        for atom in self.atoms:
            if isinstance(atom, AST):
                if atom.is_aggregate():
                    return True
            if getattr(atom, "token", None) in Expression.aggregates:
                return True
        return False
