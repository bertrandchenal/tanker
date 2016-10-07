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
to write and read countries, we define a view based on the country
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
(which is conveniently defined as the index in the table
definition).

We can go further and use more than one dot and let tanker resolve
foreign key for us. Let's say we want to add a member table to our
database, we append the following piece of yaml to our schema file

    - table: member
      columns:
        name: varchar
        registration_code: varchar
        team: m2o team.id
      index:
        - registration_code

And re-run the `create_tables()` as above. Now we can do:

    rows = View('member', ['name', team.country.name]).read()

Here, two join queries will be automatically generated, one between
`member` and `team` and one between `team` and `country`.


To add a member we have to link it to a team, whose index is composed
of both the name and the country column (so we allow two team with the
same name in different countries):

    members = [
        ['Bob', 'Belgium', 'Blue', '001'],
        ['Alice', 'Belgium', 'Red', '002'],
        ['Trudy', 'France', 'Blue', '003'],
    ]
    member_view = View('member', ['name', 'team.country.name', 'team.name',
                                  'registration_code'],
    ])
    member_view.write(members)

Tanker will be able to identify for each member the correct team based
on both country name and team name.


### Filters

The read method accept a `filters` argument it can be a string or a
list of strings. Filter strings use
[s-expression](https://en.wikipedia.org/wiki/S-expression)
notation. So for example to filter a country by name you can do:

    filters = '(= name "Belgium")'
    country_view.read(filters)

or to get `registration_code` above a given value:

    member_view.read('(> registration_code "002")')

You can also combine those filters and use the dot notation:

    filters = '(or ((> registration_code "002") (= team.country.name "Belgium")))'
    member_view.read(filters).read()

The `filters` argument can also be a list, in this case all items are
regrouped in a conjunction, equivalent to `(and item1 item2 ...)`.


### Query arguments

To facilitate the building of queries and more importantly to prevent
sql-injection, you can use arguments. They use the syntax of Python
own
[string format method](https://docs.python.org/2/library/stdtypes.html#str.format),
and will make use of the DB-API's parameter substitution (see for
example
[the sqlite documentation](https://docs.python.org/2/library/sqlite3.html)). Example:

    cond = '(= name {name})'
    rows = team_view.read(cond).args(name='Blue')

You can also pass list values, they will be automatically
expanded. And you can use the dot notation to reach a given parameter
in the object passed as argument:

    cond = '(or (in name {names}) (= registration_code {data.code}))'
    rows = member_view.read(cond).args(names=['Alice', 'Bob'], data=my_object)

The dot notation also supports dictionnaries, so the above example
whould work with `data={'code': '001'}`. The query arguments can also
refer values from the configuration (which can be reach from the `ctx`
object), like:

    ctx.cfg['default_team'] = 'Red'
    cond = '(in name {default_team})'
    rows = view.read(cond)

Finally, arguments can be a list instead of a dict and can be passed to the `read` method, so:

    cond = '(in name {names})'
    rows = team_view.read(cond).args(names=['Blue', 'Red'])

is equivalent to

    cond = '(in name {} {})'
    rows = team_view.read(cond).args('Blue', 'Red')

and is equivalent to

    cond = '(in name {} {})'
    rows = team_view.read(cond, args=['Blue', 'Red'])


### Pandas Dataframes

Instead of passing a list of list we can use a dataframe, and use a
dictionary to map dataframe columns to database columns.

    df = DataFrame({
        'Team': ['Blue', 'Red'],
        'Country': ['France', 'Belgium']
        })
    view = View('team', {
        'Team': 'name',
        'Country': 'country.name',
    })
    view.write(data)

    df_copy = view.read_df()


### Documentation TODO

  - Explain View.write behaviour (update vs insert & indexes)
  - ACL
  - Aliases


## Roadmap

Some ideas, in no particular order:

  - Replace `read_df` by `read().df()`, like that we can do
    `read().args(**kw).df()`
  - Allow to excute complete queries with s-expressions (select,
    update, insert and delete).
  - Add support for
    [PostgreSQL UPSERT](https://www.postgresql.org/docs/9.5/static/sql-insert.html#SQL-ON-CONFLICT)
