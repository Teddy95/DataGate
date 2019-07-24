<?php
/**
 * Author: Andre Sieverding
 * Copyright (c) 2019
 */

// This file is using the mapping.json configuration file!

// Set timelimit for script execution
set_time_limit(0);

// Check mapping.json
if (!file_exists('mapping.json')) {
	echo 'File mapping.json don\'t exists!';
	die();
}

// Get data from mapping.json and check all required fields
$mapping = json_decode(file_get_contents('mapping.json'));

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
	echo 'File mapping.json don\'t have all required fields!';
	die();
}

// Open logfile handlers
$errorLogFileHandle = fopen('log/' . date('Y-m-d') . '_error-log.txt', 'w');
$queryLogFileHandle = fopen('log/' . date('Y-m-d') . '_query-log.txt', 'w');

// Connect to Oracle Database
$oraDB = oci_pconnect($mapping->source->user, $mapping->source->password, $mapping->source->descriptionString, 'AL32UTF8');

// Export data from Oracle Database and create a sql import file for Microsoft SQL Server (for each table)
foreach ($mapping->tables as $table) {
	// Check all required fields from mapping.json file for current table
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
		&& isset($table->dest->table->import)
		&& !empty($table->dest->table->import)
		&& isset($table->dest->table->merge)
		&& !empty($table->dest->table->merge)
		&& isset($table->dest->fields)
		&& !empty($table->dest->fields))) {
		echo 'File mapping.json don\'t have all required fields for table synchronization!';
		die();
	}

	// Build sql query
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

	// Add query to query log file
	fwrite($queryLogFileHandle, date('Y-m-d H:i:s') . " [query]: " . $query . "\r\n");

	// Execute query
	$statement = oci_parse($oraDB, $query);
	oci_execute($statement);

	// Fetch the result into a new sql import file for SQL Server
	if ($statement) {
		// Create file handler
		$fileHandle = fopen('cache/' . $table->dest->table->merge . '.sql', 'w');

		while (($row = oci_fetch_array($statement, OCI_NUM)) != false) {
			// Build INSERT INTO sql queries
			fwrite($fileHandle, "INSERT INTO " . $mapping->dest->scheme . "." . $table->dest->table->import);

			if (count($table->dest->fields) == 1 && $table->dest->fields[0] == '*') {
				fwrite($fileHandle, " VALUES ('" . implode("', '", $row) . "')");
			} else {
				fwrite($fileHandle, " (" . implode(', ', $table->dest->fields) . ") VALUES ('" . implode("', '", $row) . "');\r\n");
			}
		}

		// Close file handler
		fclose($fileHandle);
	} else {
		fwrite($errorLogFileHandle, date('Y-m-d H:i:s') . " [error]: Table " . $table->source->table . " konnte nicht synchronisiert werden!\r\n");
	}
}

// Close Oracle Database connection
oci_close($oraDB);

// Connect to SQL Server
$sqlsrvDB = \sqlsrv_connect($mapping->dest->host, [
	"Database" => $mapping->dest->database,
	"Uid" => $mapping->dest->user,
	"PWD" => $mapping->dest->password
]);

// Import all sql files to SQL Server database
foreach ($mapping->tables as $table) {
	// Check whether sql file for current table exists
	if (file_exists('cache/' . $table->dest->table->merge . '.sql')) {
		// Create file handler
		$fileHandle = fopen('cache/' . $table->dest->table->merge . '.sql', 'r');

		while (!feof($fileHandle)) {
			// Read query from file
			$query = fgets($fileHandle);

			if (!empty($query)) {
				// Execute query
				$result = sqlsrv_query($sqlsrvDB, $query);

				if (!$result) {
					fwrite($errorLogFileHandle, date('Y-m-d H:i:s') . " [error]: Query couldn\'t be performed: " . $query . "\r\n");
				}
			}
		}

		// Close file handle
		fclose($fileHandle);

		// Delete sql file from cache
		unlink('cache/' . $table->dest->table->merge . '.sql');
	} else {
		fwrite($errorLogFileHandle, date('Y-m-d H:i:s') . " [error]: Table " . $table->source->table . " konnte nicht synchronisiert werden!\r\n");
	}
}

// Close SQl Server database connection
sqlsrv_close($sqlsrvDB);

// Done!
fwrite($errorLogFileHandle, date('Y-m-d H:i:s') . " [success]: Done!\r\n");
echo 'Done!';

// At least, close log file handler
fclose($errorLogFileHandle);
fclose($queryLogFileHandle);
