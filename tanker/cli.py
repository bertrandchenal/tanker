import argparse
import csv
import os
import sys

from .utils import logger, __version__, yaml_load, ctx
from .context import connect, create_tables
from .view import View
from .table import Table


def cli():
    parser = argparse.ArgumentParser(description='Tanker CLI')
    parser.add_argument(
        'action', help='info, read, write, delete or version', nargs=1
    )
    parser.add_argument('table', help='Table to query', nargs='*')
    parser.add_argument(
        '--config',
        help='Config file (defaults to ".tk.yaml")',
        default='.tk.yaml',
    )
    parser.add_argument(
        '-D', '--db-uri',
        help='Database URI (override config file value)',
    )
    parser.add_argument(
        '-l', '--limit', help='Limit number of results', type=int
    )
    parser.add_argument('-o', '--offset', help='Offset results', type=int)
    parser.add_argument(
        '-F', '--filter', action='append', help='Add filter', default=[]
    )
    parser.add_argument(
        '-p', '--purge', help='Purge table after write', action='store_true'
    )
    parser.add_argument(
        '-s', '--sort', action='append', help='Sort results', default=[]
    )
    parser.add_argument(
        '-f', '--file', help='Read/Write to file ' '(instead of stdin/stdout)'
    )
    parser.add_argument(
        '--yaml',
        help='Enable YAML input / ouput ' '(defaults to csv)',
        action='store_true',
    )
    parser.add_argument(
        '--ascii-table',
        '-t',
        help='Enable ascii table output',
        action='store_true',
    )
    parser.add_argument('--vbar', help='Vertical bar plot', action='store_true')
    parser.add_argument('--tic', help='Tic character to use for plot')
    parser.add_argument(
        '-d', '--debug', help='Enable debugging', action='store_true'
    )
    parser.add_argument(
        '-H', '--hide-headers', help='Hide headers', action='store_true'
    )

    args = parser.parse_args()
    if args.debug:
        logger.setLevel('DEBUG')
    if args.action[0] == 'version':
        print(__version__)
        return

    if os.path.exists(args.config):
        cfg = yaml_load(open(args.config))
    else:
        cfg = {}
    if args.db_uri:
        cfg['db_uri'] = args.db_uri
    if cfg.get('schema'):
        cfg['schema'] = yaml_load(open(os.path.expanduser(cfg['schema'])))
    with connect(cfg):
        cli_main(args)


def ascii_table(rows, headers=None, sep=' '):
    # Convert content as strings
    rows = [list(map(str, row)) for row in rows]
    # Compute lengths
    lengths = (len(h) for h in (headers or rows[0]))
    for row in rows:
        lengths = map(max, (len(i) for i in row), lengths)
    lengths = list(lengths)
    # Define row formatter
    fmt = lambda xs: sep.join(x.ljust(l) for x, l in zip(xs, lengths)) + '\n'
    # Output content
    if headers:
        top = fmt(headers)
        yield top
        yield fmt('-' * l for l in lengths)
    for row in rows:
        yield fmt(row)


def vbar(rows, fields, plot_width=80, tic=None):
    tic = tic or '•'
    if not rows:
        return
    if not isinstance(rows[0][-1], (float, int)):
        err = 'Last column must be numeric'
        logger.error(err)
        return

    labels, values = zip(*((r[:-1], r[-1]) for r in rows))
    labels = [str(' / '.join(map(str, l))) for l in labels]
    label_len = max(len(l) for l in labels)
    value_max = max(max(v for v in values), 0)
    value_min = min(min(v for v in values), 0)
    value_width = max(len(f' {value_min:.2f}'), len(f'{value_max:.2f}'))
    delta = (value_max - value_min) or 1
    scale = delta / plot_width

    if value_min < 0:
        left_pane = round(-value_min / scale)
    else:
        left_pane = 0

    for label, value in zip(labels, values):
        yield f'{label:<{label_len}} {value:>{value_width}.2f} '
        if value < 0:
            nb_tics = -round(value / scale)
            line = ' ' * (left_pane - nb_tics) + tic * nb_tics + '|\n'
            yield line
        else:
            pos = round(value / scale)
            yield ' ' * left_pane + '|' + tic * pos + '\n'

    yield ''


def cli_input_data(args):
    fields = args.table[1:] or None
    fh = None
    if args.file:
        fh = open(args.file)
    elif args.action in ('write', 'delete'):
        fh = sys.stdin
    if not fh:
        return fields, None

    if args.yaml:
        data = yaml_load(fh)
    else:
        reader = csv.reader(fh)
        data = list(reader)

    # If not field is given we infer them from the data
    if not fields and data:
        if args.yaml:
            fields = data[0].keys()
        else:
            fields = data[0]
            data = data[1:]

    return fields, data


def cli_main(args):
    action = args.action[0]
    table = args.table[0] if args.table else None
    order = map(lambda x: x.split(':') if ':' in x else x, args.sort)
    fields, data = cli_input_data(args)

    if action == 'read':
        view = View(table, fields)
        res = view.read(
            args.filter,
            order=list(order),
            limit=args.limit,
            offset=args.offset,
        )

        if args.file:
            fh = open(args.file, 'w')
        else:
            fh = sys.stdout
        if args.yaml:
            import yaml

            fh.write(yaml.dump(list(res.dict()), default_flow_style=False))
        elif args.ascii_table:
            headers = (
                None if args.hide_headers else [f.name for f in view.fields]
            )
            for line in ascii_table(res, headers=headers):
                fh.write(line)
        elif args.vbar:
            for line in vbar(list(res), view.fields, tic=args.tic):
                fh.write(line)
        else:
            writer = csv.writer(fh)
            if not args.hide_headers:
                writer.writerow([f.name for f in view.fields])
            writer.writerows(res.all())

    elif action == 'delete':
        View(table, fields).delete(filters=args.filter, data=data)

    elif action == 'write':
        # Extract data
        fields, data = cli_input_data(args)
        View(table, fields).write(data, purge=args.purge)

    elif action == 'info':
        if table:
            columns = sorted(Table.get(table).columns, key=lambda x: x.name)
            for col in columns:
                if col.ctype in ('M2O', 'O2M'):
                    details = '%s -> %s' % (col.ctype, col.fk)
                else:
                    details = col.ctype
                print('%s (%s)' % (col.name, details))
        else:
            for name in sorted(ctx.registry):
                print(name)

    elif action == 'init':
        create_tables()

    else:
        print('Action "%s" not supported' % action)


if __name__ == '__main__':
    cli()
