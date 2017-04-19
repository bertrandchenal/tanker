from datetime import datetime
from tanker import connect, View,  yaml_load, create_tables

schema = '''
- table: test
  columns:
    name: varchar
    ts: timestamp
    code: integer
    value: float
  index:
    - name
'''
cfg = {'schema': yaml_load(schema)}
ts = datetime.now()
data = [(str(i), ts, 1, 1) for i in range(100000)]

with connect(cfg):
    create_tables()
    v = View('test')
    v.write(data)
