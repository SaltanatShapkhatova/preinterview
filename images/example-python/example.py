#!/usr/bin/env python
import csv
import json
import sqlalchemy
import collections
from datetime import date, datetime

engine = sqlalchemy.create_engine("mysql://codetest:swordfish@database:3306/codetest")
connection = engine.connect()

metadata = sqlalchemy.schema.MetaData(engine)


def execsql(query):
  try:
    connection.execute(query)
    print("table created...")
  except RuntimeError:
    print(RuntimeError)
  
execsql("""create table if not exists `places` (
      `id` int not null auto_increment,
      `city` varchar(255) DEFAULT NULL,
      `county` varchar(255) DEFAULT NULL,
      `country` varchar(255) DEFAULT NULL,
      PRIMARY KEY (`id`)
    );""")

execsql("""create table if not exists `people` (
    `id` int not null auto_increment,
    `given_name` varchar(255) default null,
    `family_name` varchar(255) default null,
    `date_of_birth` date default null,
    `place_fk` int null,
    FOREIGN KEY (`place_fk`)
    REFERENCES `places`(`id`)
    on DELETE CASCADE,
    primary key (`id`)
  );""")


places = sqlalchemy.schema.Table('places', metadata, autoload=True, autoload_with=engine)
with open('/data/places.csv') as csv_file:
  reader = csv.reader(csv_file)
  next(reader)
  for r in reader:
    connection.execute(places.insert().values(city = r[0],county=r[1],country=r[2]))
print("loaded places data...")


people = sqlalchemy.schema.Table('people', metadata, autoload=True, autoload_with=engine)
with open('/data/people.csv') as csv_file:
  reader = csv.reader(csv_file)
  next(reader)
  cities = connection.execute(f"select id, city from places")
  dict = cities.mappings().all()
  
  for r in reader:
    s = [d for d in dict if d['city'] == r[3]]
    print(s)
    connection.execute(people.insert().values(given_name = r[0], family_name=r[1], date_of_birth=r[2], place_fk=s[0]['id']))
print("loaded people data...")


stmt = "select plc.country, count(plc.country) as count_country  from people as ppl join places as plc on plc.id = ppl.place_fk group by plc.country;"
output = connection.execute(stmt).fetchall()

arr = {}

for r in output:
    arr[r[0]] = r[1]

print(arr)
j = json.dumps(arr)

with open('/data/summary_output.json', 'w') as json_file:
  json_file.write(j)
print("prepared summary_output.json...")
