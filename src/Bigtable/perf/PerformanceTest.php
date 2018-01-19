<?php

use Google\Cloud\Bigtable\src\BigtableTable;

class PerformanceTest {
	private $BigtableTable;

	function __construct() {
		$this->BigtableTable = new BigtableTable();
	}

	public function insertRecord($table, $rowKey_pref, $columnFamily, $optionalArgs = []) {
		$total_row  = (isset($optionalArgs['total_row']))?$optionalArgs['total_row']:10000000;
		$batch_size = (isset($optionalArgs['batch_size']))?$optionalArgs['batch_size']:1000;

		if ($total_row < $batch_size) {
			throw new ValidationException('Please set total row (total_row) >= '.$batch_size);
		}
		$interations = $total_row/$batch_size;

		$hdr = hdr_init(1, 3600000, 3);

		$allEntries        = [];
		$MutateRowsRequest = [];
		$index             = 0;
		$processStartTime  = round(microtime(true)*1000);
		for ($k = 0; $k < $interations; $k++) {//iterations
			$entries = [];
			for ($j = 0; $j < $batch_size; $j++) {//batch_size
				$rowKey        = sprintf($rowKey_pref.'%07d', $index);
				$MutationArray = [];
				for ($i = 0; $i < 10; $i++) {
					$utc_str           = gmdate("M d Y H:i:s", time());
					$utc               = strtotime($utc_str);
					$cell['cf']        = $columnFamily;
					$cell['qualifier'] = 'field'.$i;
					$cell['value']     = $this->generateRandomString(100);
					$cell['timestamp'] = $utc*1000;
					$MutationArray[$i] = $this->BigtableTable->mutationCell($cell);
				}
				// setMutations
				$entries[$index] = $this->BigtableTable->mutateRowsRequest($rowKey, $MutationArray);
				$index++;
			}
			$MutateRowsRequest = array_merge($MutateRowsRequest, $entries);

			$startTime    = round(microtime(true)*1000);
			$ServerStream = $this->BigtableTable->mutateRows($table, $entries);
			$endTime      = round(microtime(true)*1000)-$startTime;
			hdr_record_value($hdr, $endTime);
			$allEntries[] = $ServerStream;
		}
		$time_elapsed_secs = round(microtime(true)*1000)-$processStartTime;

		$success         = 0;
		$failure         = 0;
		$MutateRowsIndex = 0;
		foreach ($allEntries as $chunkFormatter) {
			$current = $chunkFormatter->readAll()->current();
			$Entries = $current->getEntries();
			foreach ($Entries->getIterator() as $Iterator) {
				// echo "<br> Index = ".$MutateRowsIndex;
				// $Iterator->getIndex();
				$status = $Iterator->getStatus();
				$code   = $status->getCode();
				if ($code == 0) {$success++;
				} else if ($code == 1) {$failure++;
				}

				$MutateRowsIndex++;
			}
		}
		// $response = ['Success' => $success, 'failure' => $failure];
		$min           = hdr_min($hdr);
		$max           = hdr_max($hdr);
		$total         = $success+$failure;
		$throughput    = round($total/$time_elapsed_secs, 4);
		$statesticData = [
			'operation_name'     => 'Data Load',
			'run_time'           => $time_elapsed_secs,
			'mix_latency'        => $max/100,
			'min_latency'        => $min/100,
			'oprations'          => $total,
			'throughput'         => $throughput,
			'p50_latency'        => hdr_value_at_percentile($hdr, 50),
			'p75_latency'        => hdr_value_at_percentile($hdr, 75),
			'p90_latency'        => hdr_value_at_percentile($hdr, 90),
			'p95_latency'        => hdr_value_at_percentile($hdr, 95),
			'p99_latency'        => hdr_value_at_percentile($hdr, 99),
			'p99.99_latency'     => hdr_value_at_percentile($hdr, 99.99),
			'success_operations' => $success,
			'failed_operations'  => $failure
		];
		return $statesticData;
	}

	public function randomReadWrite($table, $rowKey_pref, $cf, $optionalArgs = []) {
		$total_row        = (isset($optionalArgs['total_row']))?$optionalArgs['total_row']:10000000;
		$total_operations = (isset($optionalArgs['interations']))?$optionalArgs['total_operations']:100;

		$readRowsTotal  = ['success' => [], 'failure' => []];
		$writeRowsTotal = ['success' => [], 'failure' => []];
		$hdr_read       = hdr_init(1, 3600000, 3);
		$hdr_write      = hdr_init(1, 3600000, 3);

		$operation_start            = round(microtime(true)*1000);
		$read_oprations_total_time  = 0;
		$write_oprations_total_time = 0;

		$time1 = date("h:i:s");
		echo 'Satrt Time '.$time1;
		$currentTimestemp = new DateTime($time1);

		$time2      = date(" h:i:s", time()+60*30);//sec
		$after30Sec = new DateTime($time2);
		$i          = 0;
		while ($currentTimestemp < $after30Sec) {
			// for($i=0; $i < $total_operations; $i++){
			$random       = mt_rand(0, $total_row);
			$randomRowKey = sprintf($rowKey_pref.'%07d', $random);
			$start        = round(microtime(true)*1000);
			if ($i%2 == 0) {
				$res               = $this->BigtableTable->readRows($table, [$randomRowKey]);
				$time_elapsed_secs = round(microtime(true)*1000)-$start;
				if (count($res)) {
					$readRowsTotal['success'][] = ['rowKey' => $randomRowKey, 'microseconds' => $time_elapsed_secs];
				} else {
					$readRowsTotal['failure'][] = ['rowKey' => $randomRowKey, 'microseconds' => $time_elapsed_secs];
				}
				$read_oprations_total_time += $time_elapsed_secs;
				hdr_record_value($hdr_read, $time_elapsed_secs);
			} else {
				$value = $this->generateRandomString(100);
				// $randomRowKey = 'test0000098';
				$cell['cf']        = $cf;//Specify column name, without column familly not updating row
				$cell['value']     = $value;
				$cell['qualifier'] = 'field0';//Specify qualifier (optional)

				$mutationCell = $this->BigtableTable->mutationCell($cell);
				$res          = $this->BigtableTable->mutateRow($table, $randomRowKey, [$mutationCell]);

				$time_elapsed_secs           = round(microtime(true)*1000)-$start;
				$writeRowsTotal['success'][] = ['rowKey' => $randomRowKey, 'microseconds' => $time_elapsed_secs];
				$write_oprations_total_time += $time_elapsed_secs;
				hdr_record_value($hdr_write, $time_elapsed_secs);
			}
			$i++;
			$currentTimestemp = new DateTime(date("h:i:s"));
		}
		echo '\n end Time'.date("h:i:s");
		$total_runtime = round(microtime(true)*1000)-$operation_start;
		//$throughput = $total_operations/$total_runtime;

		//Read operations
		$min_read       = hdr_min($hdr_read);
		$max_read       = hdr_max($hdr_read);
		$total_read     = count($readRowsTotal['success'])+count($readRowsTotal['failure']);
		$readThroughput = round($total_read/$read_oprations_total_time, 4);
		$readOperations = [
			'operation_name'     => 'Random Read',
			'run_time'           => $read_oprations_total_time,
			'mix_latency'        => $max_read/100,
			'min_latency'        => $min_read/100,
			'oprations'          => $total_read,
			'throughput'         => $readThroughput,
			'p50_latency'        => hdr_value_at_percentile($hdr_read, 50),
			'p75_latency'        => hdr_value_at_percentile($hdr_read, 75),
			'p90_latency'        => hdr_value_at_percentile($hdr_read, 90),
			'p95_latency'        => hdr_value_at_percentile($hdr_read, 95),
			'p99_latency'        => hdr_value_at_percentile($hdr_read, 99),
			'p99.99_latency'     => hdr_value_at_percentile($hdr_read, 99.99),
			'success_operations' => count($readRowsTotal['success']),
			'failed_operations'  => count($readRowsTotal['failure'])
		];

		//Write Operations
		$min_write       = hdr_min($hdr_write);
		$max_write       = hdr_max($hdr_write);
		$total_write     = count($writeRowsTotal['success'])+count($writeRowsTotal['failure']);
		$writeThroughput = round($total_write/$write_oprations_total_time, 4);
		$writeOperations = [
			'operation_name'     => 'Random Write',
			'run_time'           => $write_oprations_total_time,
			'mix_latency'        => $max_write/100,
			'min_latency'        => $min_write/100,
			'oprations'          => $total_write,
			'throughput'         => $writeThroughput,
			'p50_latency'        => hdr_value_at_percentile($hdr_write, 50),
			'p75_latency'        => hdr_value_at_percentile($hdr_write, 75),
			'p90_latency'        => hdr_value_at_percentile($hdr_write, 90),
			'p95_latency'        => hdr_value_at_percentile($hdr_write, 95),
			'p99_latency'        => hdr_value_at_percentile($hdr_write, 99),
			'p99.99_latency'     => hdr_value_at_percentile($hdr_write, 99.99),
			'success_operations' => count($writeRowsTotal['success']),
			'failed_operations'  => count($writeRowsTotal['failure'])
		];
		return (['readOperations' => $readOperations, 'writeOperations' => $writeOperations]);
	}

	/**
	 * Generate random string
	 *
	 * @param integer $length
	 *
	 * @return string
	 * @experimental
	 */
	public function generateRandomString($length = 10) {
		$characters       = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
		$charactersLength = strlen($characters);
		$randomString     = '';
		for ($i = 0; $i < $length; $i++) {
			$randomString .= $characters[rand(0, $charactersLength-1)];
		}
		return $randomString;
	}
}

$projectId  = "grass-clump-479";
$instanceId = "php-perf";
$tableName  = BigtableTable::tableName($projectId, $instanceId, 'test123');

//Insert record
$options         = ['total_row' => 10000, 'batch_size' => 100];
$rowKey_pref     = 'test';
$columnFamily    = 'cf';
$PerformanceTest = new PerformanceTest();
$inserted        = $PerformanceTest->insertRecord($tableName, $rowKey_pref, $columnFamily, $options);

//Random read row
$options         = ['total_row' => 1000, 'total_operations' => 20];
$rowKey_pref     = 'test';
$columnFamily    = 'cf';
$randomReadWrite = $PerformanceTest->randomReadWrite($tableName, $rowKey_pref, $columnFamily, $options);

$info = array(
	'Platform,Linux',
	'PHP,v7.0',
	'Bigtable,v2.0',
	'Start Time,'.gmdate("D M d Y H:i:s e"),
	'',
	'NOTE: All values are in milliseconds',
	'',
);

$filepath = 'reports_latency_test.csv';
header('Content-Type: application/excel');
header('Content-Disposition: attachment; filename="'.$filepath.'"');
$fp = fopen($filepath, "w");
// $fp = fopen('php://output', 'w');
foreach ($info as $line) {
	$val = explode(",", $line);
	fputcsv($fp, $val);
}
$header = ['Operation Name', 'Run Time', 'Max Latency', 'Min Latency', 'Operations', 'Throughput', 'p50 Latency', 'p75 Latency', 'p90 Latency', 'p95 Latency', 'p99 Latency', 'p99.99 Latency', 'Success Operations', 'Failed Operations'];
fputcsv($fp, $header);
fputcsv($fp, array_values($inserted));
fputcsv($fp, array_values($randomReadWrite['readOperations']));
fputcsv($fp, array_values($randomReadWrite['writeOperations']));
fclose($fp);

echo "\n File generated ".$filepath;
echo "\n";