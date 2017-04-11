from collections import defaultdict
try:
    from Queue import Queue
except ImportError:
    from queue import Queue
from threading import Thread, current_thread
import pytest

from tanker import connect, create_tables, View, TankerThread, ctx
from .base_test import get_config, DB_TYPES

NB_THREADS = 2

@pytest.yield_fixture(scope='function', params=DB_TYPES)
def session(request):
    cfg = get_config(request.param)
    with connect(cfg):
        create_tables()
        yield

def read(in_q, out_q):
    t_id = current_thread().ident
    countries = View('country').read()
    while True:
        in_q.get()
        in_q.task_done()
        c = next(countries, None)
        if c is None:
            break
        out_q.put((t_id, c[0]))

def metronome(in_queues, nb_cty):
    # Loop n + 1 time to let read() finish
    for c in range(nb_cty + 1):
        for in_q in in_queues:
            in_q.put('tic')

def test_read_thread(session):
    countries = View('country').read().all()
    nb_cty = len(countries)
    assert nb_cty > 2
    read_threads = []
    out_q = Queue()
    in_queues = []
    for i in range(NB_THREADS):
        in_q = Queue(maxsize=1)
        in_queues.append(in_q)
        t = TankerThread(target=read, args=(in_q, out_q))
        t.start()
        read_threads.append(t)

    # Launch metronome to feed input lists
    metro_thread = Thread(target=metronome, args=(in_queues, nb_cty))
    metro_thread.start()
    # Loop on results
    is_full = lambda x : len(x) == nb_cty
    per_thread = defaultdict(list)
    while True:
        t_id, c = out_q.get()
        out_q.task_done()
        per_thread[t_id].append(c)
        if all(map(is_full, per_thread.values())):
            break

    # Join everything
    metro_thread.join()
    for t in read_threads:
        t.join()


def test_nested_read(session):
    # Needed because table creation and content is not committed yet
    ctx.connection.commit()

    # Start read from parent ctx
    cursor = View('country').read()
    first = next(cursor)

    # We re-use the current config to create a nested context
    with connect(ctx.cfg):
        nested_res = View('country').read().all()

    res = [first] + list(cursor)
    assert res ==  nested_res


def test_mixed(session):
    view = View('country', ['name'])
    view.write([('Italy',)])
    countries = [c for c, in view.read()]

    in_q = Queue()
    out_q = Queue()

    # Needed because table creation and content is not committed yet
    ctx.connection.commit()

    # We re-use the current config to create a nested context
    with connect(ctx.cfg):
        t = TankerThread(target=read, args=(in_q, out_q))
        t.start()

        res = []
        for _ in countries:
            in_q.put('tic')
            res.append(out_q.get()[1])

        # Release thread loop & wait for it
        in_q.put('tic')
        t.join()

    assert 'Italy' in res
    assert res == countries
