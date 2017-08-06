-- Script to create an external table in Azure SQL DW on top of generated data (PARQUET format expected) 
-- You can use ADF to convert to PARQUET
-- !!! Replace 'REPLACEME' with your values

-- A: Create a master key.
-- Only necessary if one does not already exist.
-- Required to encrypt the credential secret in the next step.
CREATE MASTER KEY;

-- B: Create a database scoped credential
-- IDENTITY: Provide any string, it is not used for authentication to Azure storage.
-- SECRET: Provide your Azure storage account key.


CREATE DATABASE SCOPED CREDENTIAL AzureStorageCredential
WITH
    IDENTITY = 'user',
    SECRET = 'REPLACEME'
;


-- C: Create an external data source
-- TYPE: HADOOP - PolyBase uses Hadoop APIs to access data in Azure blob storage.
-- LOCATION: Provide Azure storage account name and blob container name.
-- CREDENTIAL: Provide the credential created in the previous step.

CREATE EXTERNAL DATA SOURCE DataStorage
WITH (
    TYPE = HADOOP,
    LOCATION = 'wasbs://REPLACEME@REPLACEME.blob.core.windows.net',
    CREDENTIAL = AzureStorageCredential
);

-- D: Create an external file format
-- FORMAT_TYPE: Type of file format in Azure storage (supported: DELIMITEDTEXT, RCFILE, ORC, PARQUET).
-- FORMAT_OPTIONS: Specify field terminator, string delimiter, date format etc. for delimited text files.
-- Specify DATA_COMPRESSION method if data is compressed.

CREATE EXTERNAL FILE FORMAT DataFormat
WITH (
    FORMAT_TYPE = PARQUET
	--,DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'  
);

-- E: Create the external table
-- Specify column names and data types. This needs to match the data in the sample file.
-- LOCATION: Specify path to file or directory that contains the data (relative to the blob container).
-- To point to all files under the blob container, use LOCATION='.'

CREATE EXTERNAL TABLE dbo.data_cleansed (
    deviceid VARCHAR(50) NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
	pressure DOUBLE PRECISION NOT NULL,
	ts VARCHAR(50) NOT NULL,
	source VARCHAR(50) NOT NULL
)
WITH (
    LOCATION='/polybase_output/',
    DATA_SOURCE=DataStorage,
    FILE_FORMAT=DataFormat,
	REJECT_TYPE = VALUE,
	REJECT_VALUE = 5.0
);

-- Run a query on the external table

SELECT AVG(temperature), deviceId, source FROM dbo.data_cleansed
GROUP BY deviceId, source
ORDER BY deviceId, source;

select TOP 100 * from dbo.data_cleansed;

select count(*) from dbo.data_cleansed;

-- materialise the table using CTAS (Create Table As Select)
create table dbo.data_cleansed_materialised 
WITH (DISTRIBUTION = HASH (ts))
AS select * from dbo.data_cleansed

select count(*) FROM dbo.data_cleansed_materialised
