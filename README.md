# MyDynamo

This handy little utility will read a MySQL database and migrate the data to DynamoDB. It does the "lift" for you so that you can focus on the "shift".

## Good to know

The cli uses SharedConfigEnable from the AWS SDK to read your config values. This includes credentials, region, and support for assume role. It will load its configuration from both the shared config file (\~/.aws/config) and shared credentials file (\~/.aws/credentials).

When putting items in DynanoDB this utility leverages the BatchWriteItem operation. The batch size maybe up to 25 items per batch, or 16MB of data in total. If your items are very large you should consider lowering batch-size to have fewer but larger items in each batch. More info on this https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html

## Some notable features

`-force-pk-as-string` will convert you PK to a string in DynamoDB. Since there is no sush thing as auto_increment in DynamoDB and you probably want to use UUID for PK this is the flag to use.

`-prefix-with-database` is useful when you intend to migrate several multi-tenant databases to a DynamoDB silo model. It will prefix all DynamoDB tables with the database name and using `-prefix-separator` as separator. E.g. Tenant1_Customer, Tenant2_Customer and so on.

`-tinyint-as-bool` will treat any tinyint as a boolean in DynamoDB.

## Usage

```
Usage:
  -batch-size int
    	Batch size (default 25)
  -create-table
    	Create DynamoDB table is missing
  -database string
    	MySQL database to be read from
  -force-pk-as-string
    	Convert PK to string
  -help
    	Shows this help
  -mysql-host string
    	MySQL hostname (default "localhost")
  -mysql-password string
    	MySQL password (default "root")
  -mysql-port string
    	MySQL port number (default "3306")
  -mysql-username string
    	MySQL username (default "root")
  -prefix-separator string
    	Separator if prefix is used (default "_")
  -prefix-with-database
    	Use database as prefix for DynamoDB table name
  -table string
    	Table to be migrate, if omitted all tables will be migrated
  -tinyint-as-bool
    	Convert tinyint to bool
```

## Build and run

```
# go build mydynamo.go
./mydynamo --help
```
