<?php
namespace Google\Cloud\Bigtable\src;

use Google\Bigtable\Admin\V2\ColumnFamily;
use Google\Bigtable\Admin\V2\GcRule;
use Google\Bigtable\Admin\V2\ModifyColumnFamiliesRequest_Modification as Modification;
use Google\Bigtable\Admin\V2\Table;
use Google\Cloud\Bigtable\Admin\V2\BigtableTableAdminClient;

use Google\Bigtable\V2\Mutation;
use Google\Bigtable\V2\Mutation_SetCell;
use Google\Bigtable\V2\RowFilter;
use Google\Bigtable\V2\RowSet;
use Google\Bigtable\V2\MutateRowsRequest_Entry;
use Google\Bigtable\V2\ReadModifyWriteRule;

use Google\Cloud\Bigtable\V2\BigtableClient;
//use Google\Cloud\Bigtable\V2\Gapic\BigtableGapicClient;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\MapField;


use Google\GAX\ValidationException;

use Google\Bigtable\V2\RowFilter_Condition;

/**
 *
 */

class BigtableTable
{
	/**
	 * Formats a string containing the fully-qualified path to represent
	 * a instance resource.
	 *
	 * @param string $projectId projectId
	 * @param string $instanceId instanceId
	 *
	 * @return string The formatted instance resource.
	 * @experimental
	 */
	public static function instanceName($projectId, $instanceId) {
		$formattedParent = BigtableTableAdminClient::instanceName($projectId, $instanceId);
		return $formattedParent;
	}

	/**
	 * Creates a new table in the specified instance.
	 * @param string $parent       The unique name of the instance in which to create the table.
	 *                             Values are of the form `projects/<project>/instances/<instance>`.
	 * @param string $tableId      The name by which the new table should be referred to within the parent
	 *                             instance, e.g., `foobar` rather than `<parent>/tables/foobar`.
	 * @param array  $optionalArgs {
	 *                             Optional.
	 * @return \Google\Bigtable\Admin\V2\Table
	 * @experimental
	 */
	public function createTable($parent, $tableId, $optionalArgs = []) {
		try {
			$bigtableTableAdminClient = new BigtableTableAdminClient();
			$table                    = new Table();
			$response                 = $bigtableTableAdminClient->createTable($parent, $tableId, $table, $optionalArgs);
			return $response;
		}
		finally{
			$bigtableTableAdminClient->close();
		}
	}

	/**
	 * Formats a string containing the fully-qualified path to represent
	 *
	 * @param string $projectId
	 * @param string $instanceId
	 * @param string $table
	 *
	 * @return string The formatted table resource.
	 * @experimental
	 */
	public function tableName($projectId, $instanceId, $table) {
		return BigtableTableAdminClient::tableName($projectId, $instanceId, $table);
	}

	/**
	 * Creates a new table in the specified instance with column family.
	 * @param string $parent       The unique name of the instance in which to create the table.
	 *                             Values are of the form `projects/<project>/instances/<instance>`.
	 * @param string $tableId      The name by which the new table should be referred to within the parent
	 *                             instance, e.g., `foobar` rather than `<parent>/tables/foobar`.
	 * @param string $columnFamily e.g., `cf`
	 * @param array  $optionalArgs {
	 *                             Optional.
	 * @return \Google\Bigtable\Admin\V2\Table
	 * @experimental
	 */
	public function createTableWithColumnFamily($parent, $tableId, $columnFamily, $optionalArgs = []) {
		try {
			$bigtableTableAdminClient = new BigtableTableAdminClient();
			$table                    = new Table();
			$table->setGranularity(3);

			$gc = new GcRule();
			$gc->setMaxNumVersions(3);

			$cf = new ColumnFamily();
			$cf->setGcRule($gc);

			$arr                = new MapField(GPBType::STRING, GPBType::MESSAGE, ColumnFamily::class );
			$arr[$columnFamily] = $cf;

			$table->setColumnFamilies($arr);
			$response = $bigtableTableAdminClient->createTable($parent, $tableId, $table, $optionalArgs);
			return $response;
		}
		finally{
			$bigtableTableAdminClient->close();
		}
	}

	/**
	 * Permanently deletes a specified table and all of its data.
	 *
	 *
	 * @param string $table 		The unique name of the table to be deleted.
	 *                          	Values are of the form
	 *                          	`projects/<project>/instances/<instance>/tables/<table>`.
	 * @param array  $optionalArgs {
	 *                             Optional.
	 *
	 * @throws \Google\GAX\ApiException if the remote call fails
	 * @experimental
	 */
	public function deleteTable($table, $optionalArgs = []) {
		try {
			$bigtableTableAdminClient = new BigtableTableAdminClient();
			return $bigtableTableAdminClient->deleteTable($table);
		}finally{
			$bigtableTableAdminClient->close();
		}
	}

	/**
	 * Lists all tables served from a specified instance.
	 *
	 * @param string $parent       The unique name of the instance for which tables should be listed.
	 *                             Values are of the form `projects/<project>/instances/<instance>`.
	 * @param array  $optionalArgs {
	 *                             Optional.
	 *
	 * @return array The formatted table resource.
	 * @experimental
	 */
	public function listTables($parent, $optionalArgs = []) {
		try {
			$bigtableTableAdminClient = new BigtableTableAdminClient();
			$pagedResponse            = $bigtableTableAdminClient->listTables($parent, $optionalArgs);
			$result                   = $pagedResponse->getPage();
			return $result;
		}
		finally{
			$bigtableTableAdminClient->close();
		}
	}

	/**
	 * Gets metadata information about the specified table.
	 *
	 * @param string $table 	The unique name of the requested table.
	 *                          Values are of the form
	 *                          `projects/<project>/instances/<instance>/tables/<table>`.
	 *
	 * @return \Google\Bigtable\Admin\V2\Table
	 *
	 * @throws \Google\GAX\ApiException if the remote call fails
	 * @experimental
	 */
	public function getTable($table) {
		$bigtableTableAdminClient = new BigtableTableAdminClient();
		return $bigtableTableAdminClient->getTable($table);
	}

	/**
	 * Modify column family to perticular table.
	 *
	 * @param string $table         The unique name of the table whose families should be modified.
	 *                              Values are of the form
	 *                              `projects/<project>/instances/<instance>/tables/<table>`.
	 * @param string $cfName        Column family name.
	 *
	 * @param array  $optionalArgs {
	 *                             Optional.
	 *
	 * @return \Google\Bigtable\Admin\V2\Table
	 *
	 * @throws \Google\GAX\ApiException if the remote call fails
	 * @experimental
	 */
	public function addColumnFamilies($table, $cfName, $optionalArgs = []) {
		$bigtableTableAdminClient = new BigtableTableAdminClient();
		$gc                       = new GcRule();
		$gc->setMaxNumVersions(3);

		$cf = new ColumnFamily();
		$cf->setGcRule($gc);

		$Modification = new Modification();
		$Modification->setId($cfName);
		$Modification->setCreate($cf);

		$Modifications    = [];
		$Modifications[0] = $Modification;

		$response = $bigtableTableAdminClient->modifyColumnFamilies($table, $Modifications, []);
		return $response;
	}

	/**
	 * delete column family from perticular table.
	 *
	 * @param string $table         The unique name of the table whose families should be modified.
	 *                              Values are of the form
	 *                              `projects/<project>/instances/<instance>/tables/<table>`.
	 * @param string $cfName        Column family name.
	 *
	 * @param array  $optionalArgs {
	 *                             Optional.
	 *
	 * @return \Google\Bigtable\Admin\V2\Table
	 *
	 * @throws \Google\GAX\ApiException if the remote call fails
	 * @experimental
	 */
	public function deleteColumnFamilies($table, $cfName, $optionalArgs = []) {
		$bigtableTableAdminClient = new BigtableTableAdminClient();

		$Modification = new Modification();

		$Modification->setId($cfName);
		$Modification->setDrop(true);

		$Modifications    = [];
		$Modifications[0] = $Modification;

		$response = $bigtableTableAdminClient->modifyColumnFamilies($table, $Modifications, []);
		return $response;
	}

	/**
	 * insert record in to table.
	 *
	 * @param string $table         The unique name of the table whose families should be modified.
	 *                              Values are of the form
	 *                              `projects/<project>/instances/<instance>/tables/<table>`.
	 * @param string $rowKey       The key of the row to which the mutation should be applied.
	 *
	 * @param array  $optionalArgs {
	 *                             Optional.
	 *
	 * @return \Google\Bigtable\V2\MutateRowResponse
	 *
	 * @throws \Google\GAX\ApiException if the remote call fails
	 */
	public function insertRecord($table, $rowKey, $optionalArgs = [])
	{
		$total_row = (isset($optionalArgs['total_row'])) ?  $optionalArgs['total_row'] : 10000000;
		$batch_size = (isset($optionalArgs['batch_size'])) ?  $optionalArgs['batch_size'] : 1000;

		if($total_row < $batch_size){
			throw new ValidationException('Please set total row (total_row) >= '.$batch_size);
		}
		$interations = $total_row / $batch_size;
		
		$allEntries = [];
		$MutateRowsRequest = [];
		$index = 0;
		for ($k = 0; $k < $interations; $k++){ //iterations
			$entries = [];
			for ($j = 0; $j < $batch_size; $j++) { //batch_size
				$rowKey = sprintf($rowKey.'%07d', $index);
				$MutationArray = [];
				for ($i = 0; $i < 10; $i++) {
					$utc_str = gmdate("M d Y H:i:s", time());
					$utc     = strtotime($utc_str);
					$value = 
					$cell['cf'] = 'cf';
					$cell['qualifier'] = 'field'.$i;
					$cell['value'] = $this->generateRandomString(100);
					$cell['timestamp'] = $utc*1000;
					$MutationArray[$i] = $this->mutationCell($cell);
				}
				// setMutations
				$MutateRowsRequest_Entry = new MutateRowsRequest_Entry();
				$MutateRowsRequest_Entry->setRowKey($rowKey);
				$MutateRowsRequest_Entry->setMutations($MutationArray);
				$entries[$index] = $MutateRowsRequest_Entry;
				$index++;
			}
			$MutateRowsRequest = array_merge($MutateRowsRequest, $entries);
			$BigtableClient = new BigtableClient();
			$ServerStream = $BigtableClient->mutateRows($table, $entries, $optionalArgs);
			$allEntries[] = $ServerStream;
		}
		$success = 0;
		$failure = 0;
		$MutateRowsIndex = 0;
		foreach($allEntries as $chunkFormatter){
			$current = $chunkFormatter->readAll()->current();
			$Entries = $current->getEntries();
			foreach($Entries->getIterator() as $Iterator){
				// echo "<br> Index = ".$MutateRowsIndex;// $Iterator->getIndex();
				$status = $Iterator->getStatus();
				$code = $status->getCode();
				// echo "    code = ". $code;
				if($code == 0) $success++;
				else if($code == 1) $failure++;

				$MutateRowsIndex++;
			}
		}
		$response = ['Success' => $success, 'failure' => $failure];
		return $response;
	}

	/**
	 * Random Read Write from table.
	 *
	 * @param string $table         The unique name of the table whose families should be modified.
	 *                              Values are of the form
	 *                              `projects/<project>/instances/<instance>/tables/<table>`.
	 * @param string $rowKey       The key of the row to which the mutation should be applied.
	 *
	 * @param array  $optionalArgs {
	 *                             Optional.
	 *
	 * @return \Google\Bigtable\V2\MutateRowResponse
	 *
	 * @throws \Google\GAX\ApiException if the remote call fails
	 */
	public function randomReadWrite($table, $rowKey_pref, $cf, $optionalArgs = [])
	{
		$total_row = (isset($optionalArgs['total_row'])) ?  $optionalArgs['total_row'] : 10000000;
		$interations = (isset($optionalArgs['interations'])) ?  $optionalArgs['interations'] : 100;
		$readRowsTotal = ['success' => [], 'failure' => []];
		$writeRowsTotal = ['success' => [], 'failure' => []];
		for($i=0; $i < $interations; $i++){
			$random = mt_rand(0, $total_row);
			$randomRowKey = sprintf($rowKey_pref.'%07d', $random);
			if($i % 2 == 0){
				$start = microtime(true);
				$res = $this->readRows($table, [$randomRowKey]);
				$time_elapsed_secs = microtime(true) - $start;
				
				if(count($res) ){
					$readRowsTotal['success'][] = ['rowKey' => $randomRowKey, 'microseconds' => $time_elapsed_secs];
				}
				else{
					$readRowsTotal['failure'][] = ['rowKey' => $randomRowKey, 'microseconds' => $time_elapsed_secs];
				}
			}
			else{
				$value = $this->generateRandomString(100);
				// $randomRowKey = 'test0000098';
				$cell['cf'] = $cf; //Specify column name, without column familly not updating row
				$cell['value'] = $value;
				$cell['qualifier'] = 'field0'; //Specify qualifier (optional)

				$start = microtime(true);
				$res = $this->mutateRow($table, $randomRowKey, [$this->mutationCell($cell)]);
				$time_elapsed_secs = microtime(true) - $start;
				$writeRowsTotal['success'][] = ['rowKey' => $randomRowKey, 'microseconds' => $time_elapsed_secs];				
			}
		}
		return(['readRowsTotal' => $readRowsTotal, 'writeRowsTotal' => $writeRowsTotal]);
	}

	public function mutateRow($table, $rowKey, $cell)
	{
		$BigtableClient = new BigtableClient();		
		return $BigtableClient->mutateRow($table, $rowKey, $cell);
		
	}

	public function checkAndMutateRow($tableName, $rowKey, $optionalArgs = [])
	{
		
		$cell['cf'] = 'cf';
		$cell['value'] = '678';//$value;
		$cell['qualifier'] = 'field0';
		//$optionalArgs['trueMutations'] = [$this->mutationCell($cell)];
		$cell = [];
		$cell['cf'] = 'cf';
		$cell['value'] = '852';
		// $cell['qualifier'] = 'field1';
		// $cell['timestamp'] = 1515662884000;
		$optionalArgs['falseMutations'] = $this->mutationCell($cell);

		// $optionalArgs['predicateFilter'] = ;
		$BigtableClient = new BigtableClient();
		return $BigtableClient->checkAndMutateRow($table, $rowKey, $optionalArgs);
	}

	/**
	 * Set Mutation SetCell.
	 * 
	 * @param string $cf			Column Family name
	 * @param string $qualifier		Qualifier name
	 * @param string $qualifier		Qualifier name
	 * @param string $timestamp		Timestamp in micros
	 * 
	 * @param \Google\Bigtable\V2\Mutation_SetCell 
	 */
	public function mutationCell($cell)
	{
		$Mutation_SetCell = new Mutation_SetCell();
		if(isset($cell['cf']))
			$Mutation_SetCell->setFamilyName($cell['cf']);
		if(isset($cell['qualifier']))
			$Mutation_SetCell->setColumnQualifier($cell['qualifier']);
		if(isset($cell['value']))
			$Mutation_SetCell->setValue($cell['value']);
		if(isset($cell['timestamp']))
			$Mutation_SetCell->setTimestampMicros($cell['timestamp']);

		$Mutation = new Mutation();
		$Mutation->setSetCell($Mutation_SetCell);
		return $Mutation;
	}

	/**
	 * Read row from table.
	 *
	 * @param string $table         The unique name of the table whose families should be modified.
	 *                              Values are of the form
	 *                              `projects/<project>/instances/<instance>/tables/<table>`.
	 *
	 * @param array  {
	 *     @array $rowKeys
	 *          The row keys and/or ranges to read. If not specified, reads from all rows.
	 *     @array $filter
	 *          The filter to apply to the contents of the specified row(s). If unset,
	 *          reads the entirety of each row.
	 *     @int $rowsLimit
	 *          The read will terminate after committing to N rows' worth of results. The
	 *          default (zero) is to return all results.
	 *     @int $timeoutMillis
	 *          Timeout to use for this call.
	 * }
	 *
	 * @return array
	 * @experimental
	 */
	public function readRows($table, $rowKeys = [], $filter = [], $rowsLimit = '', $timeoutMillis = '') {
		$optionalArgs = [];
		if (count($rowKeys) > 0) {
			$rowSet = new RowSet();
			$rowSet->setRowKeys($rowKeys);
			$optionalArgs['rows'] = $rowSet;
		}

		if (count($filter) > 0) {
			$rowFilter = new RowFilter();
			// $optionalArgs['filter'] = $rowFilter;
		}

		if ($rowsLimit) {
			$optionalArgs['rowsLimit'] = $rowsLimit;
		}

		if ($timeoutMillis) {
			$optionalArgs['timeoutMillis'] = $timeoutMillis;
		}

		$BigtableClient = new BigtableClient();//BigtableGapicClient();
		$chunkFormatter = $BigtableClient->readRows($table, $optionalArgs);
		$rows           = [];
		foreach ($chunkFormatter->readAll() as $flatRow) {
			$rows[] = $flatRow;
		}
		return $rows;
	}

	/**
	 * Generate random string
	 *
	 * @param integer $length
	 *
	 * @return string
	 * @experimental
	 */
	private function generateRandomString($length = 10) {
		$characters       = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
		$charactersLength = strlen($characters);
		$randomString     = '';
		for ($i = 0; $i < $length; $i++) {
			$randomString .= $characters[rand(0, $charactersLength-1)];
		}
		return $randomString;
	}
}
?>