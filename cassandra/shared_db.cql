CREATE KEYSPACE "RestCommSMSC" WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1};
         
USE "RestCommSMSC";

CREATE TABLE "SHARED_ALERTS_${CLUSTERING_SMSC_NAME}" (
	"TARGET_ID" ascii,
	"CREATED" timestamp,
	PRIMARY KEY ("TARGET_ID")
);

CREATE TABLE "SHARED_EXPECTED_ALERTS" (
  "TARGET_ID" ascii,
  "SMSC" ascii,
  "CREATED" timestamp,
	PRIMARY KEY ("TARGET_ID", "SMSC")
);

CREATE TABLE "SHARED_CORRELATION_ID" (
	"ID" ascii,
  "VALUE" blob,
	PRIMARY KEY ("ID")
);
