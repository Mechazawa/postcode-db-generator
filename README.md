Postcode DB Generator
---------------------

You can use this to generate a lookup table for postal codes to use in your application. 
It accepts OSM XML exports as input and has support for a wide range of databases.

## Generating the database

```sh
# Download the latest OSM 
wget https://download.geofabrik.de/europe/netherlands-latest.osm.bz2

# Generate SQLite databse for looking up postal codes for the Netherlands
# I use PV to montior the progress since it'll take about 10 minutes to import the Netherlands on an M1 Macbook
touch postcode.db
pv netherlands-latest.osm.bz2 | bunzip2 | cargo run --release -- --fresh --db 'sqlite://postcode.db'

# You can have multiple countries in the same database by just pointing it at the same database
wget https://download.geofabrik.de/europe/belgium-latest.osm.bz2
wget https://download.geofabrik.de/europe/germany-latest.osm.bz2

pv belgium-latest.osm.bz2 | bunzip2 | cargo run --release -- --db 'sqlite://postcode.db'
pv germany-latest.osm.bz2 | bunzip2 | cargo run --release -- --db 'sqlite://postcode.db'
```

## Querying the dataset
Postal codes that are linked to only a single street won't have more then one record and the `house_number` will be set to `null`.

```SQL
SELECT
    lat, lon, city, country, postcode, province, street
FROM node
WHERE 
    postcode = '5038LX'
AND (house_number = '13' OR house_number is null)
LIMIT 1;


+------------------+------------------+---------+---------+----------+---------------+---------------+
|       lat        |       lon        |  city   | country | postcode | province      |    street     |
+------------------+------------------+---------+---------+----------+---------------+---------------+
| 51.5608428009063 | 5.07643564622356 | Tilburg | NL      | 5038LX   | Noord-Brabant | Talent Square |
+------------------+------------------+---------+---------+----------+---------------+---------------+
```

## Limitations
Due to how the file is structured there are currently some errors when setting the province for a postal code.
This will be resolved in a future revision
