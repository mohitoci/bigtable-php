<?php

ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);
ini_set('memory_limit', '1024M');// or you could use 1G

require 'vendor/autoload.php';
putenv('GOOGLE_APPLICATION_CREDENTIALS=Grass Clump 479-b5c624400920.json');

require 'instance.php';
require 'table.php';

$options['projectId']  = "grass-clump-479";
$options['instanceId'] = "php-perf";
//'dotnet-perf';
echo "<pre>";
/*
 * Instance
 */
$BigtableInstance = new BigtableInstance($options);

// Project Name
$projectName = $BigtableInstance->projectName();

// List Instance
// $instances = $BigtableInstance->listInstace($projectName);
// print_r($instances);
// die;
// Instance Name
// $instance = $BigtableInstance->instanceName();

// Create Instance
// $instance = $BigtableInstance->createInstace($projectName, 'php-perf', 'clusters1');
// print_r($instance);

// Delete Instance
// $deleteInst = $BigtableInstance->deleteInstance($BigtableInstance->instanceName('grass-clump-479', 'instance1'));

/*
 * Table
 */
$BigtableTable = new BigtableTable($options);
// Instance Name
// $instance = $BigtableTable->instanceName();

// Table Name
// $tableName = $BigtableTable->tableName('table123');

// Create Table
// $tableName = 'pref0001';
// $table = $BigtableTable->createTable($instance, $tableName);

// Create table with column family
// $tableId = 'pref0002';
// $cfName = 'cf';
// $table = $BigtableTable->createTableWithColumnFamily($instance, $tableId, $cfName);
// print_r($table);
// die;
// Get Table
// $table = $BigtableTable->getTable($BigtableTable->tableName('table123'));

/*For list of key families*/
/*$cfs = $table->getColumnFamilies();
$iterator = $cfs->getIterator();
foreach ($iterator as $key => $value){
echo $key;
}
 */

// List of Tables
// $tableList = $BigtableTable->listTables($BigtableTable->instanceName('', 'dotnet-perf'));
// print_r($tableList);
// die;

// Delete table
// $table = $BigtableTable->deleteTable($BigtableTable->tableName('pref0001'));

// Add column family to table
// $cfName = 'cf';
// $table = $BigtableTable->addColumnFamilies($BigtableTable->tableName('pref0001'), $cfName);

// Delete column family from table
// $cfName = 'cf';
// $table = $BigtableTable->deleteColumnFamilies($BigtableTable->tableName('pref0001'), $cfName);

//Insert record
// $record = $BigtableTable->insertRecord($BigtableTable->tableName('pref0002'), 'user');
// die;

//Read row with rowkeys
$rowkeys = ['user0000000', 'user0000001'];
// $rows = $BigtableTable->readRows($BigtableTable->tableName('pref0001'), $rowkeys);
$rows = $BigtableTable->readRows($BigtableTable->tableName('pref0002'), $rowkeys);

// Read all rows
// $rows = $BigtableTable->readRows($BigtableTable->tableName('table123'), [], [], 1000000000000000000000000000000000000); //array('rowsLimit' => 2)
print_r($rows);
// die;
?>