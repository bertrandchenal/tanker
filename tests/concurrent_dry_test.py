from collections import defaultdict
from Queue import Queue
from threading import Thread, current_thread
import pytest

from tanker import connect, create_tables, View, TankerThread, ctx
from base_test import get_config, DB_TYPES

NB_THREADS = 2

@pytest.yield_fixture(scope='function', params=DB_TYPES)
def session(request):
    cfg = get_config(request.param)
    with connect(cfg):
        create_tables()
    yield

def test_read_thread(session):
    cfg = get_config('sqlite')
    with connect(cfg):
        create_tables()
        countries = View('country').read().all()
    nb_cty = len(countries)
    assert nb_cty > 2
    read_threads = []
    out_q = Queue()
    in_queues = []

    for i in range(NB_THREADS):
        in_q = Queue(maxsize=1)
        in_queues.append(in_q)
        t = TankerThread(target=read, args=(in_q, out_q, cfg))
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

def read(in_q, out_q, cfg):
    with connect(cfg):
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
