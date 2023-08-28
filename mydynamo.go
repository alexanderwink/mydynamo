package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stoewer/go-strcase"
)

type DynDBColumnDefinition struct {
	Name     string
	Datatype string
	PK       bool
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage:\n")
		flag.PrintDefaults()
	}

	mysqlHostPtr := flag.String("mysql-host", "localhost", "MySQL hostname")
	mysqlPortPtr := flag.String("mysql-port", "3306", "MySQL port number")
	mysqlUsernamePtr := flag.String("mysql-username", "root", "MySQL username")
	mysqlPasswordPtr := flag.String("mysql-password", "root", "MySQL password")
	databasePtr := flag.String("database", "", "MySQL database to be read from")
	tablePtr := flag.String("table", "", "Table to be migrate, if omitted all tables will be migrated")
	prefixTableNamePtr := flag.Bool("prefix-with-database", false, "Use database as prefix for DynamoDB table name")
	prefixSeparatorPtr := flag.String("prefix-separator", "_", "Separator if prefix is used")
	tinyintToBoolPtr := flag.Bool("tinyint-as-bool", false, "Convert tinyint to bool")
	forcePKPtr := flag.Bool("force-pk-as-string", false, "Convert PK to string")
	createTablePtr := flag.Bool("create-table", false, "Create DynamoDB table is missing")
	batchSizePtr := flag.Int("batch-size", 25, "Batch size")
	helpPtr := flag.Bool("help", false, "Shows this help")
	flag.Parse()
	if flag.NFlag() == 0 {
		flag.Usage()
		os.Exit(1)
	}
	if *helpPtr {
		flag.Usage()
		os.Exit(0)
	}

	// Create a DynamoDB client
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	svc := dynamodb.New(sess)

	// Connect to MySQL
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=True", *mysqlUsernamePtr, *mysqlPasswordPtr, *mysqlHostPtr, *mysqlPortPtr, *databasePtr))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Establish a list of tables to migrate
	var tables []string
	if len(*tablePtr) > 0 {
		tables = append(tables, *tablePtr)
	} else {
		tables = tableMetadata(db, *databasePtr)
	}

	// Migrate and optionally create tables in DynamoDB
	for _, table := range tables {
		if *createTablePtr {
			createTable(db, svc, *databasePtr, table, *prefixTableNamePtr, *prefixSeparatorPtr, *tinyintToBoolPtr, *forcePKPtr)
		}
		migrate(db, svc, *databasePtr, table, *prefixTableNamePtr, *prefixSeparatorPtr, *tinyintToBoolPtr, *forcePKPtr, *batchSizePtr)
	}

}

// Create a table in DynamoDB
// Will throw ResourceInUseException if table already exists
func createTable(db *sql.DB, dyndb *dynamodb.DynamoDB, database string, table string, prefixTableName bool, prefixSeparator string, tinyintToBool bool, forcePK bool) {
	dyndbcols := columnMetadata(db, database, table, tinyintToBool, forcePK)

	// Find the PK column from all available columns
	var pk string
	var dt string
	for i := range dyndbcols {
		if dyndbcols[i].PK {
			pk = dyndbcols[i].Name
			dt = dyndbcols[i].Datatype
		}
	}

	// Make the request object
	dynTableName := dynamoDbTableName(database, table, prefixTableName, prefixSeparator)
	input := &dynamodb.CreateTableInput{
		BillingMode: aws.String("PAY_PER_REQUEST"),
		TableName:   aws.String(dynTableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(pk),
				AttributeType: aws.String(dt),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(pk),
				KeyType:       aws.String("HASH"),
			},
		},
	}

	// Call CreateTable API
	_, err := dyndb.CreateTable(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() != dynamodb.ErrCodeResourceInUseException {
				fmt.Printf("Failed attempting to create table %s\n", dynTableName)
				fmt.Println(input)
				panic(err)
			}
		}
	}
	// Wait for table to be created so that we can put items to it
	dyndb.WaitUntilTableExists(&dynamodb.DescribeTableInput{
		TableName: aws.String(dynTableName)})
}

// Get the table name for DynamoDB
func dynamoDbTableName(database string, table string, prefixTableName bool, prefixSeparator string) string {
	if prefixTableName {
		return database + prefixSeparator + table
	}
	return table
}

// Get information about all MySQL tables
func tableMetadata(db *sql.DB, database string) []string {
	tabs, err := db.Query("select table_name from information_schema.tables where table_schema=?", database)
	if err != nil {
		panic(err)
	}
	defer tabs.Close()
	var tables []string
	for tabs.Next() {
		var tablename string
		tabs.Scan(&tablename)
		tables = append(tables, tablename)
	}
	return tables
}

// Get information about all MySQL columns
func columnMetadata(db *sql.DB, database string, table string, tinyintToBool bool, forcePK bool) []DynDBColumnDefinition {
	var dyndbcols []DynDBColumnDefinition

	metas, err := db.Query("select col.column_name, col.data_type, sta.column_name as pk from information_schema.columns as col left join information_schema.statistics as sta on col.table_schema=sta.table_schema and col.table_name=sta.table_name and col.column_name=sta.column_name and sta.index_name = 'primary' where col.table_schema=? and col.table_name=? order by ordinal_position", database, table)
	if err != nil {
		panic(err)
	}
	defer metas.Close()

	for metas.Next() {
		var colname string
		var datatype string
		var pk string
		metas.Scan(&colname, &datatype, &pk)
		if forcePK && len(pk) > 0 {
			datatype = "S" // If column is pk and force-pk-as-string flag is set, force datatype to string
		} else if tinyintToBool && datatype == "tinyint" {
			datatype = "BOOL"
		} else if strings.HasSuffix(datatype, "int") {
			datatype = "N"
		} else if strings.HasSuffix(datatype, "blob") {
			datatype = "B"
		} else {
			datatype = "S"
		}
		colname = strcase.UpperCamelCase(colname) // Yes, this is the way. Always use PascalCase aka UpperCamelCase
		dyndbcols = append(dyndbcols, DynDBColumnDefinition{Name: colname, Datatype: datatype, PK: len(pk) > 0})
	}
	return dyndbcols
}

// Read from MySQL and write to DynamoDB
func migrate(db *sql.DB, dyndb *dynamodb.DynamoDB, database string, table string, prefixTableName bool, prefixSeparator string, tinyintToBool bool, forcePK bool, batchSize int) {
	rows, err := db.Query("select * from " + table)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	dyndbcols := columnMetadata(db, database, table, tinyintToBool, forcePK)
	var cols []string
	cols, err = rows.Columns()
	if len(cols) != len(dyndbcols) {
		panic("Number of columns from resultset and metadata mismatch")
	}

	dynTableName := dynamoDbTableName(database, table, prefixTableName, prefixSeparator)
	items := []*dynamodb.WriteRequest{}
	itemCount := 0

	for rows.Next() {
		columns := make([]sql.NullString, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i, _ := range columns {
			columnPointers[i] = &columns[i]
		}
		rows.Scan(columnPointers...)

		av := make(map[string]*dynamodb.AttributeValue)
		for i := range cols {
			if dyndbcols[i].Datatype == "N" {
				if columns[i].Valid {
					av[dyndbcols[i].Name] = &dynamodb.AttributeValue{N: aws.String(columns[i].String)}
				} else {
					av[dyndbcols[i].Name] = &dynamodb.AttributeValue{NULL: aws.Bool(true)}
				}
			} else if dyndbcols[i].Datatype == "BOOL" {
				b, _ := strconv.ParseBool(columns[i].String)
				av[dyndbcols[i].Name] = &dynamodb.AttributeValue{BOOL: aws.Bool(b)}
			} else if dyndbcols[i].Datatype == "B" {
				av[dyndbcols[i].Name] = &dynamodb.AttributeValue{B: []byte(columns[i].String)}
			} else {
				av[dyndbcols[i].Name] = &dynamodb.AttributeValue{S: aws.String(columns[i].String)}
			}
		}

		items = append(items, &dynamodb.WriteRequest{PutRequest: &dynamodb.PutRequest{
			Item: av,
		}})

		itemCount++

		// Check if batch size is reached and write
		if itemCount%batchSize == 0 {
			bwii := &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]*dynamodb.WriteRequest{
					dynTableName: items,
				},
			}
			dyndb.BatchWriteItem(bwii)
			items = nil
		}
	}

	// Check for remaining items and write
	if len(items) > 0 {
		bwii := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				dynTableName: items,
			},
		}
		dyndb.BatchWriteItem(bwii)
	}

}
