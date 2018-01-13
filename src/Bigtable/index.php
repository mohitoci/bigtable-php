<?php
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);
ini_set('memory_limit', '2028M');// or you could use 1G
ini_set('max_execution_time', 30000); //300 seconds = 5 minutes

require 'vendor/autoload.php';
// putenv('GOOGLE_APPLICATION_CREDENTIALS=Grass_Clump_479-b5c624400920.json');

use Google\Cloud\Bigtable\src\BigtableInstance;
use Google\Cloud\Bigtable\src\BigtableTable;
use Google\Cloud\Bigtable\src\Bigtable;

// require 'src/BigtableInstance.php';
// require 'src/Bigtabletable.php';

$projectId  = "grass-clump-479";
$instanceId = "php-perf";
//'dotnet-perf';
echo "<pre>";

$Bigtable = new Bigtable();

$options['version'] = 'V2';
$options['credentials'] = 'Grass_Clump_479-b5c624400920.json';
$options['scopes'] = '';
$options['timeout'] = '';
// $Bigtable ->connection($projectId, $options);
// die;

/*
 * Instance
 */
$BigtableInstance = new BigtableInstance();

// Project Name
$projectName = $BigtableInstance->projectName($projectId);
// $projectName = BigtableInstance::projectName($projectId);

// List Instance
// $instances = $BigtableInstance->listInstances($projectName);
// print_r($instances);
// die;
// Instance Name
// $instance = $BigtableInstance->instanceName($projectId, $instanceId);

// Create Instance
$instance = $BigtableInstance->createInstace($projectName, 'php-perf2', 'clusters1');
print_r($instance);

// Delete Instance
// $deleteInst = $BigtableInstance->deleteInstance($BigtableInstance->instanceName('grass-clump-479', 'instance1'));

/*
 * Table
 */
$BigtableTable = new BigtableTable();
// Instance Name
$instanceName = $BigtableTable->instanceName($projectId, $instanceId);
//$instance = BigtableTable::instanceName($projectId, $instanceId);

// Table Name
$tableName = $BigtableTable->tableName($projectId, $instanceId, 'test123');

// Create Table
// $tableName = 'pref0001';
// $table = $BigtableTable->createTable($instanceName, $tableName);

// Create table with column family
// $tableId = 'pref0003';
// $cfName = 'cf';
// $table = $BigtableTable->createTableWithColumnFamily($instanceName, $tableId, $cfName);
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
// $tableList = $BigtableTable->listTables($instanceName);
// print_r($tableList);
// die;

// Delete table
// $table = $BigtableTable->deleteTable($BigtableTable->tableName($projectId, $instanceId, 'table123'));

// Add column family to table
// $cfName = 'cf';
// $table = $BigtableTable->addColumnFamilies($BigtableTable->tableName('pref0001'), $cfName);

// Delete column family from table
// $cfName = 'cf';
// $table = $BigtableTable->deleteColumnFamilies($BigtableTable->tableName('pref0001'), $cfName);

//Insert record
$options = ['total_row' => 1000, 'batch_size' => 1000];
// $record = $BigtableTable->insertRecord($tableName, 'test', $options);

$options = ['total_row' => 1000, 'interations' => 100];
$rowKey_pref = 'test';
$columnFamily = 'cf';
$record = $BigtableTable->randomReadWrite($tableName, $rowKey_pref, $columnFamily, $options);

print_r($record);
die;

//Read row with rowkeys

$rowkeys = ['user0000000', 'user0000001'];
// $rows = $BigtableTable->readRows($BigtableTable->tableName('pref0001'), $rowkeys);
$tableName = $BigtableTable->tableName($projectId, $instanceId, 'test123');//pref0002
$rows = $BigtableTable->readRows($tableName);

// Read all rows
// $rows = $BigtableTable->readRows($BigtableTable->tableName('table123'), [], [], 1000000000000000000000000000000000000); //array('rowsLimit' => 2)
print_r($rows);
// die;
?>