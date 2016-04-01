from tanker import connect, create_tables, yaml_load

from basic import yaml_def, populate, delete

cfg = {
    'db_uri': 'postgresql://login:passwd@hostname/dbname',
    'definitions': yaml_load(yaml_def)
}


if __name__ == '__main__':
    with connect(cfg):
        create_tables()
        populate()
        delete()
