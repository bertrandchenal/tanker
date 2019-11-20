
# Tanker

---
### Planning

- Normalisation des tables
- Choix des index
- Tanker
- Lecture & écriture
- Filtres & agrégats
- Ligne de commande et api web


---
## Normalisation des tables

---
### Exemple: jeudis du libre

Exemple de jeux de données: <http://jeudisdulibre.be/>.

Information que l'on peut collecter pour chaque évènement:

``` text
- title
- date
- speaker
- bio
- description
- tags
```

---
### Version naïve

Création de la table:

``` sql
CREATE TABLE "event" (
  "url" VARCHAR NOT  NULL,
  "title" VARCHAR,
  "speaker" VARCHAR,
  "description" VARCHAR,
  "date" DATE,
  "tags" VARCHAR[],
  "bio" VARCHAR
)
```

Insertion en db :

``` sql
INSERT INTO TABLE event ("date", "speaker", "tags", "bio", "title", "url")
VALUES ('2019-05-08', 'Yannick Warnier (Chamilo)' '{e-learning,éducation}', '...');
```

---
### Version naïve

``` text
date       speaker                                    tags                                            bio
---------- ------------------------------------------ ----------------------------------------------- ---
2019-05-08 Yannick Warnier (Chamilo)                  {e-learning,éducation}                          ...
2019-04-10 Joël Lambillotte (IMIO)                    {communauté,développement}                      ...
2019-03-03 Fabrice Flore-Thebault (Stylelabs, Cent... {automation,systèmes}                           ...
2019-02-07 Michel Villers                             {internet,systèmes}
2018-12-21 Mathieu Goeminne (CETIC)                   {Big Data,développement,Traitement des données} ...
2018-11-24 Laurence Baclin (HELHa)                    {éducation,programmation}
2018-11-06 Said Eloudrhiri                            {blockchain,sécurité}
2018-10-05 Robert Viseur                              {automation,photographie,robert viseur}         ...
2018-05-04 Christopher Richard et Quentin Carpenti... {éducation,mons,programmation}                  ...
    2018-02-15 Michaël Hoste (80LIMIT)                    {Javascript,mons,programmation,web}
```
Non affiché: `description` et `url`

---
### Discussion

- Ok pour une utilisation temporaire
- Pose problème pour l'édition des données
- Problématique aussi sur des gros jeux de données


---
### Version normalisée - clés naturelles
Table `speaker`:

``` text
speaker                                    bio
------------------------------------------ ------------------------------------------
Yannick Warnier (Chamilo)                  Diplômé en Sciences In...
Joël Lambillotte (IMIO)                    Joël Lambillotte est d...
Fabrice Flore-Thebault (Stylelabs, Cent... Fabrice Flore-Thebault...
``` 											   

---
### Version normalisée - clés naturelles

Table `event`:

``` text
speaker                                    url  date       description
------------------------------------------ ---- ---------- ------------------------------------------
Yannick Warnier (Chamilo)                  ...  2019-05-08 Le projet de plateforme e-learning Cham...
Joël Lambillotte (IMIO)                    ...  2019-04-10 L’intercommunale Imio conçoit et héberg...
Fabrice Flore-Thebault (Stylelabs, Cent... ...  2019-03-03 Ansible est une plate-forme d’automatis...
Michel Villers                             ...  2019-02-07 Pfsense est un routeur/pare-feu, dérivé...
Mathieu Goeminne (CETIC)                   ...  2018-12-21 Les logiciels open source ont une place...
Laurence Baclin (HELHa)                    ...  2018-11-24 Quoi de neuf dans l’univers de la progr...
Said Eloudrhiri                            ...  2018-11-06 De nos jours, les blockchains restent i...
Robert Viseur                              ...  2018-10-05 Les logiciels et le matériels libres pe...
Christopher Richard et Quentin Carpenti... ...  2018-05-04 Apprendre la logique de programmation e...
Michaël Hoste (80LIMIT)                    ...  2018-02-15 Le développement JavaScript est un véri...
```
Note: La colonne `speaker` pose problème, certaines présentations on plusieurs orateurs.


---
### Version normalisée - clés naturelles

On supprime la colonne `speaker` de `event` et on ajoute la table
`event_speaker`:


``` text
speaker                                    url
------------------------------------------ ---
Yannick Warnier (Chamilo)                  ...
Joël Lambillotte (IMIO)                    ...
Fabrice Flore-Thebault (Stylelabs, Cent... ...
Michel Villers							   ...
Mathieu Goeminne (CETIC)                   ...
```

- chaque orateur va apparaître dans la table speaker et dans la table
  de relation.
- chaque url va apparaître dans cette même table et la table event.

---
###  Discussion

- Beaucoup d'exemples sur le web (entre autre Wikipedia) mettent en
  avant cette approche.
- En pratique on va être vite amené a utiliser des clés naturelles
  composites qui peuvent devenir complexes (quid de modéliser un score
  par spectateur par speaker et par évènement).
- De plus ...


---
###  Discussion

``` sql
jdl=> create table speaker (name varchar primary key);
CREATE TABLE
jdl=> create table event (url varchar primary key);
CREATE TABLE
jdl=> create table speaker_event (speaker varchar references speaker (name), url varchar references event (url));
CREATE TABLE
jdl=> INSERT INTO speaker (name) VALUES ('Bob');
INSERT 0 1
jdl=> INSERT INTO event (url) VALUES ('http://...');
INSERT 0 1
jdl=> INSERT INTO speaker_event (speaker, url) VALUES ('Bob', 'http://...');
INSERT 0 1
jdl=> UPDATE speaker SET name = 'Robert' WHERE name = 'Bob';
ERREUR:  UPDATE ou DELETE sur la table « speaker » viole la contrainte de clé étrangère
« speaker_event_speaker_fkey » de la table « speaker_event »
DÉTAIL : La clé (name)=(Bob) est toujours référencée à partir de la table « speaker_event ».
```

¯\\\_(ツ)\_/¯

---
### Version normalisée - clés artificielle ("surrogate")

Table event:

``` sql
  CREATE TABLE "event" (
  "id" SERIAL PRIMARY KEY,
  "url" VARCHAR NOT NULL,
  "title" VARCHAR, "description" VARCHAR,
  "date" DATE,
  "tags" VARCHAR[]
  )
```

---
### Version normalisée - clés artificielle

Table speaker:

``` sql
  CREATE TABLE "speaker" (
  "id" SERIAL PRIMARY KEY,
  "name" VARCHAR NOT NULL,
  "bio" VARCHAR
  )
```


---
### Version normalisée - clés artificielle

Table event_speaker

``` sql
  CREATE TABLE "event_speaker" (
  "id" SERIAL,
  "speaker" INTEGER REFERENCES "speaker" ("id") ,
  "event" INTEGER REFERENCES "event" ("id"),
  )
```

---
### Insertion: nouvel évènement

``` sql
jdl=> INSERT INTO event (url, title, description) VALUES ('http://...', 'Example Title', 'Example description');
INSERT 0 1
jdl=> select * from event;
 id |    url     |     title     |     description     | date | tags
----+------------+---------------+---------------------+------+------
  1 | http://... | Example Title | Example description |      |
(1 ligne)
```

---
### Insertion: premier orateur

``` sql
jdl=> INSERT INTO speaker (name, bio) VALUES ('John Doe', 'Example Bio');
INSERT 0 1
jdl=> select * from speaker;
 id |   name   |     bio
----+----------+-------------
  1 | John Doe | Example Bio
(1 ligne)
```

---
### Insertion: second orateur

``` sql
jdl=> INSERT INTO speaker (name, bio) VALUES ('Jane Doe', 'Another Bio');
INSERT 0 1
jdl=> INSERT INTO event_speaker (event, speaker) VALUES (1,2);
INSERT 0 1
```

---
### Select: jointure des 3 tables

``` sql
jdl=> SELECT event.url, event.title, speaker.name FROM event
      JOIN event_speaker ON (event_speaker.event = event.id)
	  JOIN speaker ON (event_speaker.speaker = speaker.id);
    url     |     title     |   name
------------+---------------+----------
 http://... | Example Title | John Doe
 http://... | Example Title | Jane Doe
(2 lignes)
```

---
## Indexes et clés uniques

---
### Question

Est-ce que l'on autorise deux évènement avec la même url ? Deux
speaker avec le même nom ?

``` sql
CREATE UNIQUE INDEX "unique_index_event" ON "event" ("url");
CREATE UNIQUE INDEX "unique_index_speaker" ON "speaker" ("name");
CREATE UNIQUE INDEX "unique_index_event_speaker" ON "event_speaker" ("speaker", "event");
```

De manière générale, une table sans une (et une seule) contrainte d'unicité est souvent
signe d'un problème.

---
### Insertion d'un doublon


``` sql
jdl=> INSERT INTO speaker (name, bio) VALUES ('John Doe', 'Example Bio');
ERREUR:  la valeur d'une clé dupliquée rompt la contrainte unique « unique_index_speaker »
DÉTAIL : La clé « (name)=(John Doe) » existe déjà.
```

---
### Second effet KissCool : `ON CONFLICT`

``` sql
jdl=> INSERT INTO speaker ("name", "bio")
      VALUES ('John Doe', 'Updated Bio'), ('Jack Doe', 'yet another Bio')
	  ON CONFLICT("name") DO UPDATE SET bio=EXCLUDED.bio;
INSERT 0 2
```

---
### Résultat

``` sql
jdl=> select * from speaker;
 id |   name   |       bio
----+----------+-----------------
  2 | Jane Doe | Another Bio
  1 | John Doe | Updated Bio
  5 | Jack Doe | yet another Bio
(3 lignes)
```

---
## Et Tanker dans tout ça ?

---
### Recette

Si on assemble tous ces ingrédient:

- Normalisation
- Clés artificielles
- Contrainte d'unicité sur les clé naturelles
- insertion via `ON CONFLICT`

... et que l'on ajoute Python. On obtient Tanker

---
### Définition des tables

``` yaml
- table: event
  columns:
    url: varchar
    title: varchar
    description: varchar
    date: date
    tags: varchar[]
  key:
    - url
```

``` yaml
- table: speaker
  columns:
    name: varchar
    bio: varchar
  key:
    - name

```

---
### Définition des tables

``` yaml
- table: event_speaker
  columns:
    speaker: m2o speaker.id  # <- many to one (aka clé étrangère)
    event: m2o event.id
  key:
    - speaker
    - event
```

---
### Création des tables

``` python
from tanker import connect, create_tables

cfg = {
    'db_uri': 'postgresql:///jdl',
    'schema': schema,
}
with connect(cfg):
    create_tables()
```

---
##### Création des tables: logs

```
DEBUG:2019-11-17 19:53:54: SQL Query:
  SELECT table_name FROM information_schema.tables WHERE table_schema
  = 'public'
DEBUG:2019-11-17 19:53:54: SQL Query:
               SELECT table_name, column_name, data_type
  FROM information_schema.columns ORDER BY table_name
DEBUG:2019-11-17 19:53:54: SQL Query:
  SELECT indexname FROM pg_indexes WHERE schemaname = 'public'
DEBUG:2019-11-17 19:53:54: SQL Query:
  SELECT constraint_name FROM information_schema.table_constraints
DEBUG:2019-11-17 19:53:54: SQL Query:
  CREATE TABLE "all_in_one_event" ("id" SERIAL, "url" VARCHAR NOT
  NULL, "title" VARCHAR, "speaker" VARCHAR, "description" VARCHAR,
  "date" DATE, "tags" VARCHAR[], "bio" VARCHAR)
INFO:2019-11-17 19:53:54: Table "all_in_one_event" created
DEBUG:2019-11-17 19:53:54: SQL Query:
  CREATE TABLE "event" ("id" SERIAL PRIMARY KEY, "url" VARCHAR NOT
  NULL, "title" VARCHAR, "description" VARCHAR, "date" DATE, "tags"
  VARCHAR[])
INFO:2019-11-17 19:53:54: Table "event" created
DEBUG:2019-11-17 19:53:54: SQL Query:
  CREATE TABLE "speaker" ("id" SERIAL PRIMARY KEY, "name" VARCHAR NOT
  NULL, "bio" VARCHAR)
INFO:2019-11-17 19:53:54: Table "speaker" created
DEBUG:2019-11-17 19:53:54: SQL Query:
  CREATE TABLE "event_speaker" ("id" SERIAL)
INFO:2019-11-17 19:53:54: Table "event_speaker" created
DEBUG:2019-11-17 19:53:54: SQL Query:
  ALTER TABLE "event_speaker" ADD COLUMN "speaker" INTEGER REFERENCES
  "speaker" ("id") ON DELETE CASCADE NOT NULL
DEBUG:2019-11-17 19:53:54: SQL Query:
  ALTER TABLE "event_speaker" ADD COLUMN "event" INTEGER REFERENCES
  "event" ("id") ON DELETE CASCADE NOT NULL
DEBUG:2019-11-17 19:53:54: SQL Query:
  CREATE UNIQUE INDEX "unique_index_all_in_one_event" ON
  "all_in_one_event" ("url")
DEBUG:2019-11-17 19:53:54: SQL Query:
  CREATE UNIQUE INDEX "unique_index_event" ON "event" ("url")
DEBUG:2019-11-17 19:53:54: SQL Query:
  CREATE UNIQUE INDEX "unique_index_speaker" ON "speaker" ("name")
DEBUG:2019-11-17 19:53:54: SQL Query:
  CREATE UNIQUE INDEX "unique_index_event_speaker" ON "event_speaker"
  ("speaker", "event")
DEBUG:2019-11-17 19:53:54: SQL Query:
  CREATE TEMPORARY TABLE tmp ("url" VARCHAR NOT NULL, "title" VARCHAR
  , "speaker" VARCHAR , "description" VARCHAR , "date" DATE , "tags"
  VARCHAR[] , "bio" VARCHAR , id SERIAL PRIMARY KEY)
DEBUG:2019-11-17 19:53:54: SQL Query:
  COPY tmp ("url", "title", "speaker", "description", "date", "tags",
  "bio") FROM STDIN WITH (FORMAT csv)
DEBUG:2019-11-17 19:53:54: SQL Query:
  INSERT INTO "all_in_one_event" ("url", "title", "speaker",
  "description", "date", "tags", "bio") SELECT tmp."url", tmp."title",
  tmp."speaker", tmp."description", tmp."date", tmp."tags", tmp."bio"
  FROM tmp LEFT JOIN "all_in_one_event" ON ( tmp."url" =
  "all_in_one_event"."url") ON CONFLICT ("url") DO UPDATE SET "title"
  = EXCLUDED."title", "speaker" = EXCLUDED."speaker", "description" =
  EXCLUDED."description", "date" = EXCLUDED."date", "tags" =
  EXCLUDED."tags", "bio" = EXCLUDED."bio"
SQL Params:
  ()
DEBUG:2019-11-17 19:53:54: SQL Query:
  DROP TABLE tmp
DEBUG:2019-11-17 19:53:54: COMMIT

```

---
### Insertion

``` python
values = [
  ('John Doe', 'Updated Bio'),
  ('Jack Doe', 'yet another Bio')
]
with connect(cfg):
    view = View('speaker', ['name', 'bio'])
	view.write(values)
```

Qui est la traduction de l'exemple précédent de `ON CONFLICT`

---
### Discussion

``` python
with connect(cfg):
    ...
```

Crée un `contextmanager` qui garanti l'atomicité du block de code

---
### Discussion

``` python
view = View('speaker', ['name', 'bio']) # INSERT INTO speaker (name, bio)
view.write(values)                       # VALUES (...) ON CONFLICT ...
```

---
### Lecture

``` python
with connect(cfg):
    data = View('speaker').read().all()
    print(data)
```

```
[('Jane Doe', 'Another Bio'), ('John Doe', 'Updated Bio'), ('Jack Doe', 'yet another Bio')]
```

---
### Lecture

``` python
for row in View('speaker').read().dict():
	print(row)
```

```
{'name': 'Jane Doe', 'bio': 'Another Bio'}
{'name': 'John Doe', 'bio': 'Updated Bio'}
{'name': 'Jack Doe', 'bio': 'yet another Bio'}
```



---
### Lecture

``` python
df = View('speaker').read().df()
print(df)
```

```
       name              bio
0  Jane Doe      Another Bio
1  John Doe      Updated Bio
2  Jack Doe  yet another Bio
```

---
### Lecture

``` python
df = View('event').read().df()
print(df)
```

``` 
          url  date          description          title
0  http://...  None  Example description  Example Title
```


---
### Lecture

``` python
for record in View('event_speaker').read().dict():
    print(record)
```

```
{'speaker.name': 'John Doe', 'event.url': 'http://...'}
{'speaker.name': 'Jane Doe', 'event.url': 'http://...'}
```

---
### Lecture

``` python
df = View('event_speaker', ['event.title', 'speaker.name', 'speaker.bio']).read().df()
print(df)
```

```
     event.title speaker.name  speaker.bio
0  Example Title     John Doe  Updated Bio
1  Example Title     Jane Doe  Another Bio
```


---
### Discussion

``` python
from tanker import logger
logger.setLevel('DEBUG')
with connect(cfg):
    df = View('event_speaker', ['event.title', 'speaker.name', 'speaker.bio']).read().df()
```

``` sql
SELECT "event_0"."title", "speaker_1"."name", "speaker_1"."bio"
FROM "event_speaker"
LEFT JOIN "event" AS "event_0"
  ON ("event_speaker"."event" = "event_0"."id")
LEFT JOIN "speaker" AS "speaker_1"
  ON ("event_speaker"."speaker" = "speaker_1"."id")
```

---
## Relations

---
### One-To-Many


``` yaml
- table: speaker
  columns:
    name: varchar
    bio: varchar
    events: o2m event_speaker.speaker # <- one to many
  key:
    - name
```

---
### One-To-Many

``` python
df = View('speaker', ['name', 'events.event.title', ]).read().df()
print(df)
```

```
       name events.event.title
0  John Doe      Example Title
1  Jane Doe      Example Title
2  Jack Doe               None
```

### SQL

```
SELECT "speaker"."name", "event_1"."title" FROM "speaker"
LEFT JOIN  "event_speaker" AS "event_speaker_0"
 ON ("speaker"."id" = "event_speaker_0"."speaker")
LEFT JOIN "event" AS "event_1"
 ON ("event_speaker_0"."event" = "event_1"."id")
```

---
### Insertion: données réelles

```
>>> len(data)
78
>>> data[0]
{'tags': ['e-learning', 'éducation'],
 'date': '2019-05-08',
 'title': 'Chamilo: Améliorer l’accès à une éducation de qualité partout dans le monde',
 'speaker': 'Yannick Warnier (Chamilo)',
 'description': 'Le projet de plateforme e-learning Chamilo ...',
 'bio': 'Yannick Warnier, diplômé en Sciences Informatiques ...',
 'url': 'http://jeudisdulibre.be/2019/05/08/mons-le-23-mai-chamilo-ameliorer-lacces-a-...'}
```

``` python
View('event').write(data)
speakers = {d['speaker']: d.get('bio') for d in data if d['speaker']}
View('speaker', ['name', 'bio']).write(speakers.items())
View('event_speaker', {
    'speaker': 'speaker.name',
    'url': 'event.url',
}).write(data)
```

---
## Filtres et agrégats

---
###  Filtres

``` python
fltr = {'date': '2019-05-08'}
res = View('event', 'title').read(fltr).all()
print(res)
```

```
[('Chamilo: Améliorer l’accès à une éducation de qualité partout dans le monde',)]
```

---
###  Filtres

``` python
fltr = '(= date "2019-05-08")'
res = View('event', 'title').read(fltr).all()
print(res)
```

```
[('Chamilo: Améliorer l’accès à une éducation de qualité partout dans le monde',)]
```

---
###  Filtres

``` python
fltr = '(and (>= date "2019-01-01") (< date "2020-01-01"))'
res = View('event', 'title').read(fltr).all()
print(res)
```

```
[('Chamilo: Améliorer l’accès à une éducation de qualité partout dans le monde',),
 ('Imio : clés du succès du logiciel libre dans les communes wallonnes',),
 ('Automatiser son infrastructure avec Ansible, tester grâce à Molecule',),
 ('pfSense, un firewall “libre” pour la sécurisation des réseaux domestiques et
 d’entreprises',)]
```

---
### Filtres

``` python
fltr = '(ilike title "%python%")'
res = View('event', ['date', 'title']).read(fltr).all()
print(res)
```

```
[(datetime.date(2015, 12, 20), 'Pour une bonne pédagogie de
 la programmation web avec Python & Django')]
```


---
### Agrégats

``` python
print(View('speaker', '(count *)').read().one())
```

```
(73,)
```

```python
print(View('event', '(count *)').read().one())
```

```
(78,)
```

---
### Agrégats

``` python
res = View('event_speaker', [
    'speaker.name',
    '(count *)',
]).read(order=('(count *)', 'desc'), limit=5).all()
for speaker in res:
    print(speaker)
```

```
('Robert Viseur (UMONS / CETIC / photographe indépendant)', 2)
('Dimitri Durieux (CETIC)', 2)
('Sébastien Noël (UMONS – FPMs et MIND PROJECTOR)', 2)
('Robert Viseur est Docteur ...', 2)
('Robert Viseur', 2)
```

---
### Cast

``` python
df = View('event', {
	 'year': '(extract "year" date )',
	 'month': '(extract "month" date )',
	 'count': '(count *)',
}).read().df() # with auto-groupby
print(df.set_index(['year', 'month']).unstack().fillna(''))
```

---
### Cast

``` text
       count
month   1.0  2.0  3.0  4.0  5.0  6.0  8.0  9.0  10.0 11.0 12.0
year
2011.0                                                  1    1
2012.0     2    2    2    2    2    2         1    1    1    1
2013.0          2         1    1    1         1    1    2
2014.0     1    2    1    1         1         1    1         1
2015.0     2    1    1         1    1    1         1    2    1
2016.0     1         1    1    2              1    2         2
2017.0     1    1    1    1    1         1    1    1    1
2018.0     2    1              1                   1    2    1
2019.0          1    1    1    1
```

---
### Expression

``` python
from tanker import Expression

# On ajoute le support pour une nouvelle expression sql
Expression.builtins['char_length'] = lambda x: 'char_length(%s)' % x
...
     df = View('event', {
          'date': 'date',
          'len': '(char_length title)' }).read(
               order=('len', 'desc'),
               limit=5).df()
     print(df)
```


---
### Expression

``` sql
SELECT "event"."date", char_length("event"."title") FROM "event"
  ORDER BY char_length("event"."title") desc LIMIT 5
```

```
         date  len
0  2018-01-04  176
1  2017-10-27  172
2  2014-09-05  167
3  2012-03-04  152
4  2012-06-26  148
```


---
### ACL

les ACL (access control list) permettent de définir des filtres de
manière globales:

``` python
acl_read = {
     'event': ['(>= event.date "2019-01-01")']
}
cfg = {
     'db_uri': 'postgresql:///jdl',
     'schema': schema,
     'acl-read': acl_read,
}
with connect(cfg):
     view = View('event', '(count *)')
     print(view.read().one()) # -> (4,)

     cfg.pop('acl-read')
     print(view.read().one()) # -> (78,)
```

---
### ACL

``` python
acl_read = {
     'event': ['(>= event.date {first_day})'],
     'speaker': [
       '(exists ( from event_speaker (select 1)'
           '(where (and (>= event.date {first_day})) (= speaker _parent.id))'
       '))'
     ]
}
cfg = {
     'db_uri': 'postgresql:///jdl',
     'schema': schema,
     'acl-read': acl_read,
     'first_day': '2019-01-01',
}
with connect(cfg):
     res = View('event', '(count *)').read().one()
     print(res)  # -> (4,)

     res = View('speaker', '(count *)').read().one()
     print(res) # -> (4,)
```


---
## Ligne de commande et api web

---
### tk: la ligne de commande de tanker


``` shell
$ cat .tk.yaml 
db_uri: postgresql:///jdl
```

---
### tk: la ligne de commande de tanker

``` shell
$ tk info
all_in_one_event
event
event_speaker
speaker
```

``` shell
$ tk info event
date (DATE)
description (VARCHAR)
id (INTEGER)
title (VARCHAR)
url (VARCHAR)
```

---
### tk: la ligne de commande de tanker

``` shell
$ tk read event_speaker -l 5

speaker.name,event.url
Yannick Warnier (Chamilo),http://jeudisdulibre.be/2019/05/08/mons-le-23-mai-chamilo...
Joël Lambillotte (IMIO),http://jeudisdulibre.be/2019/04/10/mons-le-25-avril-imio-cles...
"Fabrice Flore-Thebault (Stylelabs, Cent...",http://jeudisdulibre.be/2019/03/03/mons...
Michel Villers,http://jeudisdulibre.be/2019/02/07/mons-le-21-fevrier-pfsense-un-...
Mathieu Goeminne (CETIC),http://jeudisdulibre.be/2018/12/21/mons-le-17-janvier-...
```

---
### tk: la ligne de commande de tanker

``` shell
$ tk read event_speaker speaker.name event.date -F "(or (ilike title '%python%') (ilike title '%ruby%'))" -t

speaker.name                               event.date
------------------------------------------ ----------
Hugues Bersini (ULB, IRIDIA)               2015-12-20
Etienne Charlier                           2014-02-25
Francois Stephany (Wapict SPRL) et Auré... 2012-11-02
```

``` shell
$ tk read event title -F "(in date '2018-01-04' '2017-10-27' '2014-09-05')" -t
title
--------------------------------------------------------------------------------------------------
Les bases de la connectivité Bluetooth Low Energy sous Linux (Raspberry PI, BeagleBone, etc..) ...
Sauver le Monde ? Vers une économie collaborative par la mise en commun des ressources… Ou quand ...
MOOC – Une façon OUVERTE d’apprendre LIBREMENT ? Exemples avec ITyPa (sujet généré par les ...
```

---
### Cadeau Bonux: une api en 60 lignes de code

``` python
@route('/read/<table>')                             |     for k, v in params.items():
def read(table, ext='json'):						|         if k not in names or not v.strip():
    table, *fields = table.split('+')				|             continue
    view = View(table, fields or None)				|         for op in Expression.builtins:
    fltr = []										|             if v.startswith(op):
    args = []										|                 fltr.append('(%s %s {})' % (op, k))
    params = dict(request.params)					|                 v = shlex.split(v[len(op):])
    names = [f.name for f in view.fields] + ['id']	|                 args.append(v[0] if len(v) == 0 else v)
    ...												|                 break
													|         else:
													|             fltr.append('(ilike %s {})' % k)
													|             args.append(v + '%')
													|
													|     res = list(view.read(fltr, args=args).dict())
													|     return {'data': res}

```


---
# Merci
