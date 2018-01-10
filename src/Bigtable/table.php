<?php

use Google\Bigtable\Admin\V2\ColumnFamily;
use Google\Bigtable\Admin\V2\GcRule;
use Google\Bigtable\Admin\V2\ModifyColumnFamiliesRequest_Modification as Modification;

use Google\Bigtable\Admin\V2\Table;
use Google\Bigtable\V2\Mutation;
use Google\Bigtable\V2\Mutation_SetCell;
use Google\Bigtable\V2\RowFilter;
use Google\Bigtable\V2\RowSet;
use Google\Cloud\Bigtable\Admin\V2\BigtableTableAdminClient;
use Google\Cloud\Bigtable\V2\BigtableClient;
use Google\Protobuf\Internal\GPBType;

use Google\Protobuf\Internal\MapField;

/**
 *
 */

class BigtableTable {
	/**
	 * Constructor
	 * @param array $options {
	 *                       Options for configuring the service API wrapper.
	 * 		@type string $projectId		The unique name of the project
	 * 		@type string $instanceId 	The unique name of the instance
	 */
	function __construct($options) {
		$this->projectId = $options['projectId'];
		//"grass-clump-479";
		//'grape-spaceship-123';
		$this->instanceId = $options['instanceId'];
		//"node-perf";
		//'grapebigtable123';
		$this->previousCell = array();
	}

	/**
	 * Formats a string containing the fully-qualified path to represent
	 * a instance resource.
	 *
	 * @param string $project Optional
	 * @param string $instance Optional
	 *
	 * @return string The formatted instance resource.
	 * @experimental
	 */
	public function instanceName($projectId = '', $instanceId = '') {
		if (!$projectId) {
			$projectId = $this->projectId;
		}
		if (!$instanceId) {
			$instanceId = $this->instanceId;
		}

		// $bigtableTableAdminClient = new BigtableTableAdminClient();
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
	 *
	 * @param string $table
	 * @param string $projectId
	 * @param string $instanceId
	 *
	 * @return string The formatted table resource.
	 * @experimental
	 */
	public function tableName($table, $projectId = '', $instanceId = '') {
		if (!$projectId) {
			$projectId = $this->projectId;
		}
		if (!$instanceId) {
			$instanceId = $this->instanceId;
		}
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
	 * @experimental
	 */
	public function insertRecord($table, $rowKey, $optionalArgs = []) {
		$BigtableClient = new BigtableClient();

		for ($j = 0; $j < 10000; $j++) {
			$MutationArray = [];
			for ($i = 0; $i < 10; $i++) {
				$Mutation_SetCell = new Mutation_SetCell();
				$Mutation_SetCell->setFamilyName('cf');
				$Mutation_SetCell->setColumnQualifier('field'.$i);
				$Mutation_SetCell->setValue('VAL_'.$i);
				$utc_str = gmdate("M d Y H:i:s", time());
				$utc     = strtotime($utc_str);
				$Mutation_SetCell->setTimestampMicros($utc*1000);

				$Mutation = new Mutation();
				$Mutation->setSetCell($Mutation_SetCell);

				$MutationArray[$i] = $Mutation;
			}

			if ($j >= 0 && $j < 10) {
				$key = '000000'.$j;
			} else if ($j >= 10 && $j < 100) {
				$key = '00000'.$j;
			} else if ($j >= 100 && $j < 1000) {
				$key = '0000'.$j;
			} else if ($j >= 1000 && $j < 10000) {
				$key = '000'.$j;
			} else if ($j >= 10000 && $j < 100000) {
				$key = '00'.$j;
			} else if ($j >= 100000 && $j < 1000000) {
				$key = '0'.$j;
			} else if ($j >= 1000000 && $j < 10000000) {
				$key = $j;
			}
			$response = $BigtableClient->mutateRow($table, $rowKey.$key, $MutationArray, $optionalArgs);
		}
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

		$BigtableClient = new BigtableClient();
		$chunkFormatter = $BigtableClient->readRows($table, $optionalArgs);
		$rows = [];
		foreach($chunkFormatter->readAll() as $flatRow){
			$rows[] = $flatRow;
		}
		return $rows;
	}
}
?>