from tanker import connect, create_tables, logger, fetch, save
from basic import populate, cfg

if __name__ == '__main__':
    with connect(cfg):
        create_tables()
        populate()

        alice = fetch('member', name='Alice')
        logger.info('Fetched data')
        logger.info('\t' + str(alice))

        alice['name'] = alice['name'].upper()
        save('member', alice)

        alice = fetch('member', name='ALICE')
        logger.info('Updated data')
        logger.info('\t' + str(alice))
