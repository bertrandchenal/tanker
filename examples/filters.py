from tanker import connect, create_tables, yaml_load, View, logger

from basic import yaml_def, cfg, populate

if __name__ == '__main__':
    with connect(cfg):
        create_tables()
        populate()

        logger.info('Belgian members')
        filters = '(= team.country.name "Belgium")'
        view = View('member', ['name', 'team.name'])
        for row in view.read(filters=filters):
            logger.info('\t' + str(row))


        logger.info('Belgian members for Blue team and French members')
        filters = (
            '(or '
              '(and (= team.country.name "Belgium")'
                   '(= team.name "Blue"))'
              '(= team.country.name "France")'
            ')'
        )
        view = View('member', ['name', 'team.name'])
        for row in view.read(filters=filters):
            logger.info('\t' + str(row))
