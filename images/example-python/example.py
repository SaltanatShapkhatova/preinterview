#!/usr/bin/env python
import csv
import json
import sqlalchemy
import collections
from datetime import date, datetime

def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

engine = sqlalchemy.create_engine("mysql://codetest:swordfish@172.18.0.2:3306/codetest")
connection = engine.connect()

metadata = sqlalchemy.schema.MetaData(engine)

try:
  connection.execute("""create table if not exists `places` (
    `id` int not null auto_increment,
    `city` varchar(255) DEFAULT NULL,
    `county` varchar(255) DEFAULT NULL,
    `country` varchar(255) DEFAULT NULL,
    PRIMARY KEY (`id`)
  );""")
except RuntimeError:
  print(RuntimeError)

print("places created...")

try:
  connection.execute("""create table if not exists `people` (
    `id` int not null auto_increment,
    `given_name` varchar(255) default null,
    `family_name` varchar(255) default null,
    `date_of_birth` date default null,
    `place_fk` int null,
    INDEX `idx_place_id` (`place_fk`),
    FOREIGN KEY (`place_fk`)
    REFERENCES `places`(`id`)
    on DELETE CASCADE,
    primary key (`id`)
  );""")
except RuntimeError:
  print(RuntimeError)


print("people created...")


try:
  connection.execute('SET FOREIGN_KEY_CHECKS = 0; TRUNCATE TABLE people; TRUNCATE TABLE places; SET FOREIGN_KEY_CHECKS = 1;')
except RuntimeError:
  print(RuntimeError)

print("truncated tables...")

ex_places = sqlalchemy.schema.Table('places', metadata, autoload=True, autoload_with=engine)
with open('/data/places.csv') as csv_file:
  reader = csv.reader(csv_file)
  next(reader)
  for r in reader:
    connection.execute(ex_places.insert().values(city = r[0],county=r[1],country=r[2]))
print("loaded places data...")


ex_people = sqlalchemy.schema.Table('people', metadata, autoload=True, autoload_with=engine)
with open('/data/people.csv') as csv_file:
  reader = csv.reader(csv_file)
  next(reader)
  for r in reader:
    a = connection.execute(f"select id from places where city = '{r[3]}'")
    results_as_dict = a.mappings().all()
    connection.execute(ex_people.insert().values(given_name = r[0], family_name=r[1], date_of_birth=r[2], place_fk=results_as_dict[0]['id']))
print("loaded people data...")


stmt = "select plc.country, count(plc.country) as count_country  from people as ppl join places as plc on plc.id = ppl.place_fk group by plc.country;"
compare_rows = connection.execute(stmt).fetchall()

arr = {}

for r in compare_rows:
    arr[r[0]] = r[1]

print(arr)
j = json.dumps(arr, default=json_serial)

with open('/data/summary_output.json', 'w') as json_file:
  json_file.write(j)
print("prepared summary_output.json...")
