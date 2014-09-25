CREATE KEYSPACE metadata WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
 
USE metadata;
 
CREATE TABLE models
(
	  key blob 
	, value blob
	, primary key (key)
);

CREATE TABLE messages
(
	  key blob 
	, value blob
	, primary key (key)
);

CREATE TABLE containers
(
	  key blob 
	, value blob
	, primary key (key)
);

CREATE TABLE concepts
(
	  key blob 
	, value blob
	, primary key (key)
);

CREATE TABLE functions
(
	  key blob 
	, value blob
	, primary key (key)
);

CREATE TABLE types
(
	  key blob 
	, value blob
	, primary key (key)
);

CREATE TABLE others
(
	  key blob 
	, value blob
	, primary key (key)
);

exit
