from tanker import connect, create_tables, View, logger
from basic import populate, cfg

if __name__ == '__main__':
    with connect(cfg):
        create_tables()
        populate()

        view = View('team', {
            'Name': 'name',
            'Country': 'country.name',
        })

        logger.info('Sorted by Countries and name descending (limit 2)')
        res = view.read(order=['Country', ('Name', 'DESC')], limit=2)
        for row in res:
            logger.info('\t' + str(row))

        logger.info('Sorted by country.name')
        res = view.read(order='country.name')
        for row in res:
            logger.info('\t' + str(row))
