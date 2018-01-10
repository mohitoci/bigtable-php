<?php
require 'vendor/autoload.php';
putenv('GOOGLE_APPLICATION_CREDENTIALS=Grass Clump 479-b5c624400920.json');

use Google\Bigtable\Admin\V2\ColumnFamily;
use Google\Bigtable\Admin\V2\GcRule;
use Google\Bigtable\Admin\V2\Table;

use Google\Bigtable\V2\Mutation;

use Google\Bigtable\V2\Mutation_SetCell;

use Google\Cloud\Bigtable\Admin\V2\BigtableTableAdminClient;

use Google\Cloud\Bigtable\V2\BigtableClient;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\MapField;

/**
 * A minimal application that connects to Cloud Bigtable using the native HBase API
 * and performs some basic operations.
 */

class HelloWorld {

	private $tableName    = 'Hello-Bigtable';
	private $columnFamily = 'cf';
	private $rowKey       = 'key';

	/**
	 * Connects to Cloud Bigtable, runs some basic operations and prints the results.
	 */
	public function doHelloWorld($projectId, $instanceId) {

		$bigtableTableAdminClient = new BigtableTableAdminClient();

		$parent = $bigtableTableAdminClient->instanceName($projectId, $instanceId);

		$table = new Table();
		$table->setGranularity(3);

		$gc = new GcRule();
		$gc->setMaxNumVersions(3);

		$cf = new ColumnFamily();
		$cf->setGcRule($gc);

		$arr                      = new MapField(GPBType::STRING, GPBType::MESSAGE, ColumnFamily::class );
		$arr[$this->columnFamily] = $cf;

		$table->setColumnFamilies($arr);
		$this->print("Create table ".$this->tableName);
		try {
			$bigtableTableAdminClient->createTable($parent, $this->tableName, $table);
		}
		 catch (Exception $e) {
			$this->print("Create table error ".$e->getMessage());
		}

		//Get created table name
		$formatedTable = $this->tableName($projectId, $instanceId, $this->tableName);
		$this->print("Formatted table name ".$formatedTable);

		//Inserting Record into table
		$MutationArray = [];

		$Mutation_SetCell = new Mutation_SetCell();
		$Mutation_SetCell->setFamilyName($this->columnFamily);
		$Mutation_SetCell->setColumnQualifier('qualifier');
		$Mutation_SetCell->setValue('VALUE');
		$utc_str = gmdate("M d Y H:i:s", time());
		$utc     = strtotime($utc_str);
		$Mutation_SetCell->setTimestampMicros($utc*1000);

		$Mutation = new Mutation();
		$Mutation->setSetCell($Mutation_SetCell);
		$MutationArray[] = $Mutation;

		$this->print("Inserting record into table ".$this->tableName);
		$BigtableClient = new BigtableClient();
		try {
			$BigtableClient->mutateRow($formatedTable, $this->rowKey, $MutationArray);
		}
		 catch (Exception $e) {
			$this->print("Inserting record error ".$e->getMessage());
		}

		//Get row from table
		$this->print("Get row from table ".$this->tableName);
		$response = $BigtableClient->readRows($formatedTable);
		$readAll  = $response->readAll();
		$current  = $readAll->current();
		$chunks   = $current->getChunks();
		for ($i = 0; $i < count($chunks); $i++) {
			$row = $chunks[$i];
			// print_r($row);

			$row_key          = $chunks[$i]->getRowKey();
			$family_name      = ($chunks[$i]->getFamilyName())?$chunks[$i]->getFamilyName()->getValue():'';
			$qualifier        = ($chunks[$i]->getQualifier())?$chunks[$i]->getQualifier()->getValue():'';
			$timestamp_micros = ($chunks[$i]->getTimestampMicros())?$chunks[$i]->getTimestampMicros():0;
			$value            = $chunks[$i]->getValue();

			echo '<br>'.' Row key : '.$row_key;
			echo '<br>'.' Family Name : '.$family_name;
			echo '<br>'.'Qualifier Name : '.$qualifier;
			echo '<br>'.' Timestamp : '.$timestamp_micros;
			echo '<br>'.' value : '.$value;
			echo '<br>';
		}

		//delete table
		$this->print("Delete table ".$this->tableName);
		try {
			$bigtableTableAdminClient->deleteTable($formatedTable);
		}
		 catch (Exception $e) {
			$this->print("Deleting table error ".$e->getMessage());
		}

	}

	/**
	 * Formats a string containing the fully-qualified path to represent
	 *
	 *
	 * @param string $table
	 * @param string $projectId
	 * @param string $instanceId
	 *
	 * @return string The formatted table resource.
	 * @experimental
	 */
	public function tableName($projectId, $instanceId, $table) {
		if (!$projectId) {
			$projectId = $this->projectId;
		}
		if (!$instanceId) {
			$instanceId = $this->instanceId;
		}
		return BigtableTableAdminClient::tableName($projectId, $instanceId, $table);
	}

	private function print($msg) {
		echo "HelloWorld: ".$msg."<br>";
	}
}

$HelloWorld = new HelloWorld();
$HelloWorld->doHelloWorld('grass-clump-479', 'hello-bigtable');
?>