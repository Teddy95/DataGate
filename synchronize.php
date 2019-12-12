#!/usr/bin/env php
<?php
/**
 * Author: Andre Sieverding
 * Copyright (c) 2019
 */

// This file is using the mapping.json configuration file!
// Check SAPI for command line interface
if (PHP_SAPI !== 'cli') {
    echo 'This script must be run as a CLI application';
    die();
}

// Determine mapping file
if (isset($argv[1]) && !empty($argv[1]) && strpos($argv[1], '.json')) {
    $mappingFile = $argv[1];
} else {
    $mappingFile = 'mapping.json';
}

// Set timelimit for script execution
set_time_limit(0);

// Set memory limit for script execution
ini_set('memory_limit', '256M');

// Check mapping.json
echo "Check " . $mappingFile . " ...\r\n";
if (!file_exists($mappingFile)) {
	echo 'File ' . $mappingFile . ' don\'t exists!';
	die();
}

// Get data from mapping.json and check all required fields
$mapping = json_decode(file_get_contents($mappingFile));

if (!(isset($mapping->source)
	&& !empty($mapping->source)
	&& isset($mapping->source->user)
	&& !empty($mapping->source->user)
	&& isset($mapping->source->password)
	&& !empty($mapping->source->password)
	&& isset($mapping->source->descriptionString)
	&& !empty($mapping->source->descriptionString)
	&& isset($mapping->dest)
	&& !empty($mapping->dest)
	&& isset($mapping->dest->host)
	&& !empty($mapping->dest->host)
	&& isset($mapping->dest->user)
	&& !empty($mapping->dest->user)
	&& isset($mapping->dest->password)
	&& !empty($mapping->dest->password)
	&& isset($mapping->dest->database)
	&& !empty($mapping->dest->database)
	&& isset($mapping->dest->scheme)
	&& !empty($mapping->dest->scheme)
	&& isset($mapping->tables)
	&& !empty($mapping->tables))) {
	echo 'File ' . $mappingFile . ' don\'t have all required fields!';
	die();
}

// Open logfile handlers
$errorLogFileHandle = fopen('log/' . date('Y-m-d_H-i-s') . '_error-log.txt', 'w');
$queryLogFileHandle = fopen('log/' . date('Y-m-d_H-i-s') . '_query-log.txt', 'w');

// Tableindex
$tableIndex = 0;

foreach ($mapping->tables as $table) {
    // Connect to Oracle Database
    echo "Connect to Oracle database ...\r\n";
    $oraDB = oci_pconnect($mapping->source->user, $mapping->source->password, $mapping->source->descriptionString, 'AL32UTF8');

    // Export data from Oracle Database and create a sql query array for Microsoft SQL Server (for each table)
	// Check all required fields from mapping.json file for current table
	echo "Check " . $mappingFile . " for current table ...\r\n";
	if (!(isset($table->source)
		&& !empty($table->source)
		&& isset($table->source->table)
		&& !empty($table->source->table)
		&& isset($table->source->fields)
		&& !empty($table->source->fields)
		&& isset($table->dest)
		&& !empty($table->dest)
		&& isset($table->dest->table)
		&& !empty($table->dest->table)
		&& isset($table->dest->fields)
		&& !empty($table->dest->fields)
		&& isset($table->dest->primaryKeys)
		&& !empty($table->dest->primaryKeys))) {
		echo 'File ' . $mappingFile . ' don\'t have all required fields for table synchronization!';
		die();
	}

	echo "Tables: " . $table->source->table . " (Oracle) -> " . $mapping->dest->scheme . "." . $table->dest->table . " (SQL Server) ...\r\n";

	// Build sql query
	echo "Build SELECT query for Oracle ...\r\n";
	$query = "SELECT " . implode(', ', $table->source->fields) . " FROM " . $table->source->table;

	// Build WHERE statement for sql query if where is in mapping.json included
	if (isset($table->source->where) && !empty($table->source->where) && is_array($table->source->where)) {
		$index = 0;

		foreach ($table->source->where as $whereStmt) {
			// At first iteration use keyword WHERE, all other iterations use AND or OR
			if ($index == 0) {
				$query .= " WHERE " . $whereStmt->attr . " " . $whereStmt->operator . " " . $whereStmt->value;
			} else {
				if (isset($whereStmt->method) && !empty($whereStmt->method) && $whereStmt->method == 'OR') {
					$query .= " OR ";
				} else {
					$query .= " AND ";
				}

				$query .= $whereStmt->attr . " " . $whereStmt->operator . " " . $whereStmt->value;
			}

			$index++;
		}
	}

    // Define query stack
    $queryStack = Array();

	// Add query to query log file
	fwrite($queryLogFileHandle, date('Y-m-d H:i:s') . " [query][Oracle]:\t\t" . $query . "\r\n");

	// Execute query
	echo "Execute query ...\r\n";
	$statement = oci_parse($oraDB, $query);
	oci_execute($statement);

	// Fetch the result into a new sql import file for SQL Server
	if ($statement) {
		echo "Build sql import queries for table: " . $mapping->dest->scheme . "." . $table->dest->table . " ...\r\n";

        // Define query counter
        $queryCounter = 0;

		while (($row = oci_fetch_array($statement, OCI_NUM)) != false) {
            // Increase query counter
            $queryCounter++;

			// Build INSERT INTO sql queries
            $fileContent = "INSERT INTO [" . $mapping->dest->scheme . "].[" . $table->dest->table . "_temp_importtable]";
            $values = Array();

            // Escape special characters
            for ($i = 0, $j = count($mapping->tables[$tableIndex]->source->fields); $i < $j; $i++) {
                if (isset($row[$i])) {
                    $values[] = "'" . str_replace("'", "''", $row[$i]) . "'";
                } else {
                    $values[] = 'null';
                }
            }

			if (count($table->dest->fields) == 1 && $table->dest->fields[0] == '*') {
				$fileContent .= " VALUES ('" . implode("', '", $values) . "')";
			} else {
				$fileContent .= " ([" . implode('], [', $table->dest->fields) . "]) VALUES (" . implode(", ", $values) . ");";
			}

            $queryStack[] = $fileContent;
		}
	} else {
		fwrite($errorLogFileHandle, date('Y-m-d H:i:s') . " [error]:\tTable " . $table->source->table . " couldn't be synced!\r\n");
	}

    // Output number of queries
    echo "Queries for " . number_format($queryCounter, 0, ',', '.') . " Records have been created ...\r\n";

    // Close Oracle Database connection
    echo "Close Oracle connection ...\r\n";
    oci_close($oraDB);

    // Connect to SQL Server
    echo "Connect to Microsoft SQL Server " . $mapping->dest->user . "@" . $mapping->dest->host . " ...\r\n";
    $sqlsrvDB = sqlsrv_connect($mapping->dest->host, [
    	"Database" => $mapping->dest->database,
    	"Uid" => $mapping->dest->user,
    	"PWD" => $mapping->dest->password,
        "CharacterSet" => 'UTF-8'
    ]);

    // Import all sql files to SQL Server database
    echo "Import all sql queries of table " . $mapping->dest->scheme . "." . $table->dest->table . " to SQL Server ...\r\n";

    // Create import table & truncate it in case of a magic filled table
    echo "Create temporary import table from table " . $mapping->dest->scheme . "." . $table->dest->table . " ...\r\n";
    $query = "SELECT * INTO [" . $mapping->dest->scheme . "].[" . $table->dest->table . "_temp_importtable] FROM [" . $mapping->dest->scheme . "].[" . $table->dest->table . "] WHERE 1 <> 1;";
    sqlsrv_query($sqlsrvDB, $query);
    $query = "TRUNCATE TABLE [" . $mapping->dest->scheme . "].[" . $table->dest->table . "_temp_importtable];";
    sqlsrv_query($sqlsrvDB, $query);

    // Iterate trough every sql file for current table, execute & unlink them
    echo "Import sql queries for table " . $mapping->dest->scheme . "." . $table->dest->table . " ...\r\n";
    foreach ($queryStack as $query) {
        // Execute query
        $result = sqlsrv_query($sqlsrvDB, $query);

        if (!$result) {
            fwrite($errorLogFileHandle, date('Y-m-d H:i:s') . " [error]:\tQuery couldn't be performed: " . $query . "\r\n");
            fwrite($errorLogFileHandle, date('Y-m-d H:i:s') . " [error]:\tQuery error server result: " . print_r(sqlsrv_errors(), true) . "\r\n");
        }
    }

    // Empty query stack
    $queryStack = Array();

    // Merge import tables with target tables
    echo "Merge import table with target table ...\r\n";

    // Build merge query
	$query = "MERGE [" . $mapping->dest->scheme . "].[" . $table->dest->table . "] AS TARGET ";
	$query .= "USING [" . $mapping->dest->scheme . "].[" . $table->dest->table . "_temp_importtable] AS SOURCE ";
    $query .= "ON (";

	$index = 0;

	foreach ($table->dest->primaryKeys as $key) {
		if ($index > 0) {
			$query .= " AND ";
		}

		$query .= "TARGET.[" . $table->dest->fields[--$key] . "] = SOURCE.[" . $table->dest->fields[$key] . "]";
		$index++;
	}

	$query .= ") WHEN MATCHED THEN UPDATE SET ";
	$index = 0;

	foreach ($table->dest->fields as $attr) {
		if ($index > 0) {
			$query .= ", ";
		}

		$query .= "TARGET.[" . $attr . "] = SOURCE.[" . $attr . "]";
		$index++;
	}

	$query .= " WHEN NOT MATCHED BY TARGET THEN INSERT ([" . implode('], [', $table->dest->fields) . "]) VALUES (SOURCE.[" . implode('], SOURCE.[', $table->dest->fields) . "]) ";
	$query .= "WHEN NOT MATCHED BY SOURCE THEN DELETE;";

	// Execute merge query
	echo "Merging " . $mapping->dest->scheme . "." . $table->dest->table . "_temp_importtable into " . $mapping->dest->scheme . "." . $table->dest->table . " ...\r\n";
	$result = sqlsrv_query($sqlsrvDB, $query);

    if (!$result) {
		fwrite($errorLogFileHandle, date('Y-m-d H:i:s') . " [error]:\tQuery couldn't be performed: " . $query . "\r\n");
        fwrite($errorLogFileHandle, date('Y-m-d H:i:s') . " [error]:\tQuery error server result: " . print_r(sqlsrv_errors(), true) . "\r\n");
	}

    // Delete temporary import table
    echo "Drop temporary import table for table " . $mapping->dest->scheme . "." . $table->dest->table . " ...\r\n";
    $query = "DROP TABLE [" . $mapping->dest->scheme . "].[" . $table->dest->table . "_temp_importtable];";
    sqlsrv_query($sqlsrvDB, $query);

    // Close SQl Server database connection
    echo "Close SQL Server connection ...\r\n";
    sqlsrv_close($sqlsrvDB);

    // Increase table index
    $tableIndex++;
}

// Done!
fwrite($errorLogFileHandle, date('Y-m-d H:i:s') . " [success]:\tDone!\r\n");
echo "Done!\r\n";

// At least, close log file handler
fclose($errorLogFileHandle);
fclose($queryLogFileHandle);
