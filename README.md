# MySQL2DynamoDB

This handy little utility will ready a MySQL database and migrate the data to DynamoDB

## Good to know

The cli uses SharedConfigEnable from the AWS SDK to read your config values. This includes credentials, region, and support for assume role. It will load its configuration from both the shared config file (\~/.aws/config) and shared credentials file (\~/.aws/credentials).

When putting items in DynanoDB this utility leverages the BatchWriteItem operation. The batch size maybe up to 25 items per batch, or 16MB of data in total. If your items are very large you should consider lowering batch-size to have fewer but larger items in each batch. More info on this https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html

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
