create keyspace beers with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
use beers;
create table beernames (beerid int primary key, name varchar);
