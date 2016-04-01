import csv

from tanker import connect, create_tables, View

from basic import yaml_def, cfg


TEAMS = '''Name,Country
Blue,Belgium
Red,Belgium
Blue,France'''


if __name__ == '__main__':
    with connect(cfg):
        create_tables()
        lines = list(csv.reader(TEAMS.split('\n')))
        header = lines[0]
        data = [dict(zip(header, l)) for l in lines[1:]]

        # data is now a list of dicts, like :
        # [{'Country': 'Belgium', 'Name': 'Blue'},
        #  {'Country': 'Belgium', 'Name': 'Red'},
        #  {'Country': 'France', 'Name':'Blue'},
        # ]

        # We give a mapping as second attribute to inform how data
        # should be consumed:
        view = View('team', {
            'Name': 'name',
            'Country': 'country.name',
        })
        view.write(data)

        # If we read the output as a dataframe, the column names
        # matches the above mapping
        res = view.read_df()
        print res

        # Output is:
        # Country  Name
        # 0  Belgium  Blue
        # 1  Belgium  Red
        # 2  France   Blue
