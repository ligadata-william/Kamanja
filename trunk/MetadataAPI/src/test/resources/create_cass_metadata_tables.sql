CREATE KEYSPACE metadata WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
 
USE metadata;
 
CREATE TABLE metadata_objects
(
	  key blob 
	, value blob
	, primary key (key)
);

CREATE TABLE transaction_id
(
	  key blob 
	, value blob
	, primary key (key)
);

CREATE TABLE jar_store
(
	  key blob 
	, value blob
	, primary key (key)
);

CREATE TABLE config_objects
(
	  key blob 
	, value blob
	, primary key (key)
);

exit
