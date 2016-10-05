# Tanker

Tanker goal is to allow easy batch operations without compromising
database modeling. For pandas users, it's like DataFrame.to_sql on
steroids.

Currently Postgresql and Sqlite are supported. There is also optional
support for pandas DataFrames.

See the `examples` folder for a quick overview of the main features.


## Licence

Tanker is available under the ISC Licence, see LICENCE file at the
root of the repository.


## Main features

### Schema definition and database connection

The file `schema.yaml` defines the database structure: table, columns
types and indexes.

    - table: team
      columns:
        name: varchar
        country: m2o country.id
      index:
        - name
        - country
    - table: country
      columns:
        name: varchar
      index:
        - name

The code here-under create the config dictionary and use it to connect
to the database and creates the tables.

    from tanker import connect, create_tables, View, yaml_load

    cfg = {
        'db_uri': 'sqlite:///test.db',
        'schema': yaml_load(open('schema.yaml').read()),
    }
    with connect(cfg):
        create_tables()

Tanker automatically add an `id` column on each table, to allow to
define foreign keys. for example, in the yaml definition, `country:
m2o country.id` will generate the following column definition:

    "country" INTEGER REFERENCES "country" (id) ON DELETE CASCADE

If not specified, `sqlite:///:memory:` will be used as `db_uri` to use
Postgresql, the uri should looks like
`postgresql://login:passwd@hostname/dbname`

Note that every database interaction must happen inside the `with
connect(cfg)` block.


### Read & write

Tanker usage is centered around the `View` object, it is used to
define a mapping between the relational world and Python. For example,
to write and read countries, whe define a view based on the country
table:

    country_view = View(
        'country',  # The base table
        ['name']    # The fields we want to map
    )

So now we can add write to the database:

    countries = [['Belgium'], ['France']]
    country_view.write(countries)

And read it back.

    countries_copy = country_view.read().all()

And `countries_copy` should be identical to `countries`. As `.read()`
returns the database cursor, the `.all()` allows to fetch all the
records.


### Foreign key resolution

To populate the `team` table we have to provide a team name and a
country. We can do it like this:

    team_view = View('team, ['name', 'country'])
    team_view.write([['Red', 1]])

But it's more convenient to use the country name instead of it's id:

    teams = [
        ['Blue', 'Belgium'],
        ['Red', 'Belgium'],
        ['Blue', 'France'],
    ]
    team_view = View('team, ['name', 'country.name'])
    team_view.write(teams)

You can see that we changed `country` into `country.name` in the view,
which means that the use the `name` column to identify the country
(which is conveniently defined as the index in the table definition).


### Foreign keys, advanced usage

Let's say we want to add a member table to our database, we append the
following piece of yaml to our schema file

    - table: member
      columns:
        name: varchar
        registration_code: varchar
        team: m2o team.id
      index:
        - registration_code

And re-run the `create_tables()` function.

To add a member we have to link it to a team, whose index is composed
of both the name and the country column (so we allow two team with the
same name in different countries):

    members = [
        ['Bob', 'Belgium', 'Blue', '001'],
        ['Alice', 'Belgium', 'Red', '002'],
        ['Trudy', 'France', 'Blue', '003'],
    ]
    view = View('member', ['name', 'team.country.name', 'team.name',
                           'registration_code'],
    ])
    view.write(members)
