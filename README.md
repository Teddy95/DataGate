# DataGate

Synchronize Tables from Oracle to Microsoft SQL Server :page_facing_up::door:

![CMD performing DataGate](/assets/datagate_screen.png)

### mapping.json data structure

If you are synchronizing more than one sources, then each source must have the same table structure!

```
{
	source {			// Source can be an object or an array of objects (to aggregate multiple data sources into one)
		user			// Oracle Database Username
		password		// Password
		descriptionString	// Connection string
	}
	dest {
		host		// Microsoft SQL Server Hostname / IP-Address
		user		// Username
		password	// Password
		database	// Database
		scheme		// Database scheme
	}
	tables [
		{
			source {
				table		// Table which should be synchronized (Source table)
				fields		// Table columns which should be synchronized (Array)
				where [		// Where statements (Array)
					{
						attr		// Column
						operator	// Comparison operator
						value		// Value
					}
					{
						attr
						operator
						value
						method		// AND or OR
					}
				]
			},
			dest {
				table		// Target table
				fields		// Synchronized table columns (Array)
				primaryKeys	// Array with index numbers from field list to determine the primary keys
			}
		}
	]
}
```

### Execute DataGate

You can execute the DataGate script in two different ways:

1. Execute DataGate.bat (uses mapping.json)
2. Open the command line and navigate to the DataGate project directory, then enter `php synchronize.php`

If you are using the batch file, open DataGate.bat first in a text editor and replace the path to php and DataGate.

If you are using the command line interface, you can use another mapping file for example:
```bash
$ php synchronize.php newMapping.json
```

### Requirements

- PHP 7.2.x
- OCI8 extension for PHP
- SQL Server extension for PHP

### Environment

Testet on Windows 10 Pro Build 1809 with PHP 7.2.19 cli.
Testet with Oracle 9i database, Oracle InstantClient 10.2 and Microsoft SQL Azure (RTM) - 12.0.2000.8 :+1:
