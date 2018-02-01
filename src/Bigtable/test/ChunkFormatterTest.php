<?php
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);

require '../vendor/autoload.php';
require '../src/chunkformatter.php';

//use function Eloquent\Phony\spy;
//use function Eloquent\Phony\Pho\mock;

use Google\Bigtable\V2\ReadRowsResponse_CellChunk;
use Google\Protobuf\BytesValue;
use Google\Protobuf\StringValue;
//use Google\Protobuf\Internal\RepeatedField;

use PHPUnit\Framework\TestCase;

class ChunkFormatterTest extends TestCase {

	/** @test */
	public function instantiation() {
		$chunkFormatter = new ChunkFormatter();
		// echo "<pre>";
		// print_r($chunkFormatter->row);
		// die;
		//print_r(get_class_methods($this));
		// die;
		// $this->assertDeepEqual($chunkFormatter->row, [], 'invalid initial state');
		$this->assertEquals($chunkFormatter->row, [], 'invalid initial state');
		$this->assertEquals($chunkFormatter->prevRowKey, '', 'invalid initial state');
		$this->assertEquals($chunkFormatter->family, [], 'invalid initial state');
		$this->assertEquals($chunkFormatter->qualifiers, [], 'invalid initial state');
		$this->assertEquals($chunkFormatter->qualifier, [], 'invalid initial state');
		$this->assertEquals($chunkFormatter->state, 1, 'invalid initial state');
	}

	/** @test */
	public function newRow() {
		$chunkFormatter = new ChunkFormatter();
		$cellChunk      = new ReadRowsResponse_CellChunk();
		$StringValue    = new StringValue();
		$BytesValue     = new BytesValue();

		// echo "<pre>";
		// print_r(get_class_methods( $RepeatedField) );
		// die;
		$callback = function ($err, $res) {return array('err' => $err, 'res' => $res);/*echo "callback<pre>"; print_r($res);*/};
		//it('should throw exception when row key is undefined ', function () {
		try {
			//$chunkFormatter->row = array('key' => 'abc');
			$cellChunk->setRowKey('abc');
			$chunkFormatter->newRow($cellChunk, [], $callback);
		} catch (Exception $err) {
			//pass
			//print_r($err->getMessage());
			// $this->assertEquals($err->getMessage(), 'A row key must be set');//if row key is not set
			$this->assertEquals($err->getMessage(), 'A family must be set');//if row key set
		}
		// $this->assert($newRowSpy->threw());
		// });

		//it('should throw exception when resetRow is true ', function () {
		try {
			$cellChunk->setRowKey('key');
			$cellChunk->setResetRow(true);
			$chunkFormatter->newRow($cellChunk, [], $callback);
		} catch (Exception $err) {
			//pass
			//echo "<br>";
			//print_r($err->getMessage());
			$this->assertEquals($err->getMessage(), 'A new row cannot be reset');
		}
		// 	$this->$this->assert($newRowSpy->threw());
		// });

		//it('should throw exception when resetRow', function () {
		try {
			$cellChunk->setResetRow(true);
			$chunkFormatter->newRow($cellChunk, [], $callback);
		} catch (Exception $err) {
			//pass
			//echo "<br>";
			//print_r($err->getMessage());
			$this->assertEquals($err->getMessage(), 'A new row cannot be reset');
		}
		// 	$this->$this->assert($newRowSpy->threw());
		// });

		// it('should throw exception when row key is equal to previous row key ', function () {
		try {
			$chunkFormatter->prevRowKey = 'key';
			$cellChunk->setRowKey('key');
			$cellChunk->setResetRow(false);
			$chunkFormatter->newRow($cellChunk, [], $callback);
		} catch (Exception $err) {
			// echo "<br>";
			// print_r($err->getMessage());
			$this->assertEquals($err->getMessage(), 'A commit happened but the same key followed');
		}
		//	$this->$this->assert($newRowSpy->threw());
		//});

		//it('should throw exception when family name is undefined ', function () {
		$chunkFormatter->prevRowKey = '';
		try {
			$cellChunk->setRowKey('key');
			$chunkFormatter->newRow($cellChunk, [], $callback);
		} catch (Exception $err) {
			// echo "<br>";
			//print_r($err->getMessage());
			$this->assertEquals($err->getMessage(), 'A family must be set');
		}
		// 	$this->$this->assert($newRowSpy->threw());
		// });

		//it('should throw exception when qualifier is undefined ', function () {
		try {
			$cellChunk->setRowKey('key');
			$StringValue->setValue('family');
			$cellChunk->setFamilyName($StringValue);

			$chunkFormatter->newRow($cellChunk, [], $callback);
		} catch (Exception $err) {
			// echo "<br>";
			// print_r($err->getMessage());
			$this->assertEquals($err->getMessage(), 'A column qualifier must be set');
		}
		//$this->$this->assert($newRowSpy->threw());
		//});

		// it('should throw exception when valueSize>0 and commitRow=true ', function () {
		try {
			$cellChunk->setRowKey('key');
			$StringValue->setValue('family');
			$cellChunk->setFamilyName($StringValue);

			$BytesValue->setValue('qualifier');
			$cellChunk->setQualifier($BytesValue);

			$cellChunk->setValueSize(10);
			$cellChunk->setCommitRow(true);
			$chunkFormatter->newRow($cellChunk, [], $callback);
		} catch (Exception $err) {
			// print_r($err->getMessage());
			$this->assertEquals($err->getMessage(), 'A row cannot be have a value size and be a commit row');
		}
		// 	$this->$this->assert($newRowSpy->threw());
		// });

		//it('should commit 1 row ', function () {
		$cellChunk->setRowKey('key');
		$StringValue->setValue('family');
		$cellChunk->setFamilyName($StringValue);

		$BytesValue->setValue('qualifier');
		$cellChunk->setQualifier($BytesValue);

		$cellChunk->setValueSize(0);
		$cellChunk->setTimestampMicros(0);
		$cellChunk->setLabels(['L_1']);
		$cellChunk->setCommitRow(true);
		$cellChunk->setValue('value');
		$row = [];
		$chunkFormatter->newRow($cellChunk, [], function ($err, $res) use (&$row) {
				$row = $res;
			});
		//echo "<pre>";
		//$this->assertEquals($returnValue, true, 'reset state failed');
		//$this->assert($callback->called);
		//$chunkFormatter->prevRowKey = 'key';
		//$this->assertEquals($chunkFormatter->prevRowKey, $cellChunk->getRowKey(), 'wrong state prevrowkey');
		//let row = callback.getCall(0).args[1];

		//$row         = $callback->getCall(0)[args[1]];
		$expectedRow = [
			'key'  => $cellChunk->getRowKey(),
			'data' => []
		];

		$qualifier = [
			'value'     => $cellChunk->getValue(),
			'timestamp' => $cellChunk->getTimestampMicros(),
			'labels'    => ($cellChunk->getLabels()->getIterator()->valid())?$cellChunk->getLabels()->getIterator()->current():'',
			'size'      => $cellChunk->getValueSize()
		];
		$family                       = $cellChunk->getFamilyName()->getValue();
		$expectedRow['data'][$family] = [];

		$qualifierName                                = $cellChunk->getQualifier()->getValue();
		$expectedRow['data'][$family][$qualifierName] = ['0' => $qualifier];

		// echo "<pre>";
		// print_r($expectedRow);
		// print_r($row);
		//$this->assertEquals($row, $expectedRow);
		// });
		//die;
		//it('partial row', function () {
		$cellChunk->setRowKey('key');
		$StringValue->setValue('family');
		$cellChunk->setFamilyName($StringValue);

		$BytesValue->setValue('qualifier');
		$cellChunk->setQualifier($BytesValue);

		$cellChunk->setValueSize(0);
		$cellChunk->setTimestampMicros(0);
		$cellChunk->setLabels(['L_1']);
		$cellChunk->setCommitRow(false);
		$cellChunk->setValue('value');
		$row            = [];
		$chunkFormatter = new $chunkFormatter();
		$chunkFormatter->newRow($cellChunk, [], function ($err, $res) use (&$row) {
			$row = $res;
		});

		$partialRow = [
			'key'  => $cellChunk->getRowKey(),
			'data' => []
		];

		$qualifier = [
			'value'     => $cellChunk->getValue(),
			'timestamp' => $cellChunk->getTimestampMicros(),
			'labels'    => ($cellChunk->getLabels()->getIterator()->valid())?$cellChunk->getLabels()->getIterator()->current():'',
			'size'      => $cellChunk->getValueSize()
		];
		$family                      = $cellChunk->getFamilyName()->getValue();
		$partialRow['data'][$family] = [];

		$qualifierName                               = $cellChunk->getQualifier()->getValue();
		$partialRow['data'][$family][$qualifierName] = ['0' => $qualifier];
		//print_r($chunkFormatter->row);

		$this->assertEquals($chunkFormatter->row, $partialRow);
		//$this->assertEquals($chunkFormatter->state, $chunkFormatter->RowStateEnum['ROW_IN_PROGRESS'], 'wrong state');
		// });

		//it('partial cell', function () {
		$cellChunk->setRowKey('key');
		$StringValue->setValue('family');
		$cellChunk->setFamilyName($StringValue);

		$BytesValue->setValue('qualifier');
		$cellChunk->setQualifier($BytesValue);

		$cellChunk->setValueSize(10);
		$cellChunk->setTimestampMicros(0);
		$cellChunk->setLabels(['L_1']);
		$cellChunk->setCommitRow(false);
		$cellChunk->setValue('value');
		$row            = [];
		$chunkFormatter = new $chunkFormatter();
		$chunkFormatter->newRow($cellChunk, [], function ($err, $res) use (&$row) {
				$row = $res;
			});
		//$this->assertEqual($returnValue, false, 'reset state failed');
		//$this->assertEqual($callback->called, false, 'wrong callback');
		$partialRow = [
			'key'  => $cellChunk->getRowKey(),
			'data' => []
		];

		$qualifier = [
			'value'     => $cellChunk->getValue(),
			'timestamp' => $cellChunk->getTimestampMicros(),
			'labels'    => ($cellChunk->getLabels()->getIterator()->valid())?$cellChunk->getLabels()->getIterator()->current():'',
			'size'      => $cellChunk->getValueSize()
		];
		$family                      = $cellChunk->getFamilyName()->getValue();
		$partialRow['data'][$family] = [];

		$qualifierName                               = $cellChunk->getQualifier()->getValue();
		$partialRow['data'][$family][$qualifierName] = ['0' => $qualifier];

		$this->assertEquals($chunkFormatter->row, $partialRow);
		$this->assertEquals($chunkFormatter->state, $chunkFormatter->RowStateEnum['CELL_IN_PROGRESS'], 'wrong state');
		//});
	}
	
	/** @test */
	public function rowInProgress() {
		$chunkFormatter = new ChunkFormatter();
		$cellChunk      = new ReadRowsResponse_CellChunk();
		$StringValue    = new StringValue();
		$BytesValue     = new BytesValue();

		$callback = function ($err, $res) {return array('err' => $err, 'res' => $res);/*echo "callback<pre>"; print_r($res);*/};
		//it('should throw exception when resetRow and rowkey', function () {
		try {
			$cellChunk->setRowKey('key');
			$cellChunk->setResetRow(true);
			$chunkFormatter->rowInProgress($cellChunk, [], $callback);
		} catch (Exception $err) {
			//pass
			//print_r($err->getMessage());
			$this->assertEquals($err->getMessage(), 'A reset should have no data');
		}
		
		// 	$this->$this->assert($rowInProgress->threw());
		// });

		//it('should throw exception when resetRow and familyName', function () {
		try {
			$StringValue->setValue('family');
			$cellChunk->setFamilyName($StringValue);
			$cellChunk->setResetRow(true);
			$chunkFormatter->rowInProgress($cellChunk, [], $callback);
		} catch (Exception $err) {
			//pass
			//print_r($err->getMessage());
			$this->assertEquals($err->getMessage(), 'A reset should have no data');
		}
		// 	$this->$this->assert($rowInProgress->threw());
		// });

		//it('should throw exception when resetRow and qualifier', function () {
		try {
			$BytesValue->setValue('qualifier');
			$cellChunk->setQualifier($BytesValue);
			$cellChunk->setResetRow(true);
			$chunkFormatter->rowInProgress($cellChunk, [], $callback);
		} catch (Exception $err) {
			//print_r($err->getMessage());
			$this->assertEquals($err->getMessage(), 'A reset should have no data');
		}
		// 	$this->$this->assert($rowInProgress->threw());
		// });

		//it('should throw exception when resetRow and value', function () {
		try {
			$cellChunk->setValue('value');
			$cellChunk->setResetRow(true);
			$chunkFormatter->rowInProgress($cellChunk, [], $callback);
		} catch (Exception $err) {
			//print_r($err->getMessage());
			$this->assertEquals($err->getMessage(), 'A reset should have no data');
		}
		// 	$this->$this->assert($rowInProgress->threw());
		// });

		// it('should throw exception when resetRow and timestampMicros', function () {
		try {
			$cellChunk->setTimestampMicros(10);
			$cellChunk->setResetRow(true);
			$chunkFormatter->rowInProgress($cellChunk, [], $callback);
		} catch (Exception $err) {
			//pass
			//print_r($err->getMessage());
			$this->assertEquals($err->getMessage(), 'A reset should have no data');
		}
		// 	$this->$this->assert($rowInProgress->threw());
		// });

		// it('should throw exception when rowKey not equal to prevRowKey', function () {
		try {
			$chunkFormatter->row = ['key' => 'key1'];
			$cellChunk->setRowKey('key');
			$chunkFormatter->rowInProgress($cellChunk, [], $callback);
		} catch (Exception $err) {
			//pass
			//print_r($err->getMessage());
			$this->assertEquals($err->getMessage(), 'A reset should have no data');
		}
		// 	$this->$this->assert($rowInProgress->threw());
		// });

		//it('should throw exception when valueSize>0 and commitRow=true', function () {
		try {
			$cellChunk->setValueSize(10);
			$cellChunk->setCommitRow(true);
			$chunkFormatter->rowInProgress($cellChunk, [], $callback);
		} catch (Exception $err) {
			//pass
			// print_r($err->getMessage());
			$this->assertEquals($err->getMessage(), 'A commit is required between row keys');
		}
		// 	$this->assert($rowInProgress->threw());
		// });

		//it('should throw exception when familyName without qualifier', function () {
		try {
			$StringValue->setValue('family');
			$cellChunk->setFamilyName($StringValue);
			$chunkFormatter->rowInProgress($cellChunk, [], $callback);
		} catch (Exception $err) {
			//pass
			//print_r($err->getMessage());
			$this->assertEquals($err->getMessage(), 'A commit is required between row keys');
		}
		// 	$this->assert($rowInProgress->threw());
		// });

		// it('should return true on resetRow ', function () {
		// $cellChunk->setResetRow(true);
		// $returnValue = $chunkFormatter->rowInProgress($cellChunk, [], function($err, $res) use(&$row){
		// 	$row = $res;
		// 	print_r($res);
		// });
		// $this->assert($returnValue);
		// $this->assert(!$callback.called);
		// });

		//it('bare commitRow should produce qualifer ', function () {
		//$chunkFormatter = new ChunkFormatter();
		$chunkFormatter->qualifiers = [];
		$chunkFormatter->row        = [
			'key'         => 'key',
			'data'        => [
				'family'     => [
					'qualifier' => $chunkFormatter->qualifiers,
				]
			]
		];
		$cellChunk->setCommitRow(true);
		$row = [];
		$chunkFormatter->rowInProgress($cellChunk, [], function ($err, $res) use (&$row) {
			$row = $res;
		});
		// $this->assert($returnValue);
		// $this->assert($callback->called);
		$expectedRow = [
			'key'         => 'key',
			'data'        => [
				'family'     => [
					'qualifier' => [
						[
							'value'     => 'value',
							'labels'    => '',
							'size'      => '10',
							'timestamp' => '10'
						]
					]
				]
			]
		];
		$this->assertEquals($row, $expectedRow, 'row mismatch');
		$this->assertEquals($chunkFormatter->state, $chunkFormatter->RowStateEnum['NEW_ROW'], 'state mismatch');
		// });

		//it('chunk with qualifier and commit should produce row ', function () {
			$chunkFormatter->qualifiers = [];
			$chunkFormatter->family = [
				'qualifier' => $chunkFormatter->qualifiers,
			];
			$chunkFormatter->row = [
				'key'     => 'key',
				'data'    => [
					'family' => $chunkFormatter->family,
				]
			];

			$BytesValue->setValue('qualifier2');

			$cellChunk->setCommitRow(true);
			$cellChunk->setQualifier($BytesValue);
			$cellChunk->setValue('value');
			$cellChunk->setTimestampMicros(0);
			$cellChunk->setLabels([]);
			$cellChunk->setValueSize(0);
			
			$row = [];
			$returnValue = $chunkFormatter->rowInProgress($cellChunk, [], function ($err, $res) use (&$row) {
				$row = $res;
			});
			// $this->assert($returnValue);
			// $this->assert($callback->called);
			$expectedRow = [
				'key'          => 'key',
				'data'         => [
					'family'      => [
						'qualifier'  => [],
						'qualifier2' => [
							[
								'value'     => 'value',
								'size'      => 0,
								'timestamp' => 0,
								'labels'    => '',
							]
						]
					]
				]
			];
			
			$this->assertEquals($row, $expectedRow, 'row mismatch');
			$this->assertEquals($chunkFormatter->state, $chunkFormatter->RowStateEnum['NEW_ROW'], 'state mismatch');
		// });

		//it('chunk with new family and commitRow should produce row', function () {
			$chunkFormatter->qualifiers = [];
			$chunkFormatter->family = [
				'qualifier' => $chunkFormatter->qualifiers,
			];
			$chunkFormatter->row = [
				'key'     => 'key',
				'data'    => [
					'family' => $chunkFormatter->family,
				]
			];

			$cellChunk->setCommitRow(true);
			$StringValue->setValue('family2');
			$cellChunk->setFamilyName($StringValue);

			$BytesValue->setValue('qualifier2');
			$cellChunk->setQualifier($BytesValue);
			
			$cellChunk->setValue('value');
			$cellChunk->setTimestampMicros(0);
			$cellChunk->setLabels(['']);
			$cellChunk->setValueSize(0);

			$row = [];
			$chunkFormatter->rowInProgress($cellChunk, [], function ($err, $res) use (&$row) {
				$row = $res;
			});
			// $this->assert($returnValue);
			// $this->assert($callback->called);
			$expectedRow = [
				'key'         => 'key',
				'data'        => [
					'family'     => [
						'qualifier' => [],
					],
					'family2'     => [
						'qualifier2' => [
							[
								'value'     => 'value',
								'size'      => 0,
								'timestamp' => 0,
								'labels'    => ''
							]
						]
					]
				]
			];
			$this->assertEquals($row, $expectedRow, 'row mismatch');
			$this->assertEquals($chunkFormatter->state, $chunkFormatter->RowStateEnum['NEW_ROW'], 'state mismatch');
		// });

		//it('partial cell ', function () {
			$chunkFormatter->qualifiers = [];
			$chunkFormatter->row = [
				'key'         => 'key',
				'data'        => [
					'family'     => [
						'qualifier' => $chunkFormatter->qualifiers,
					]
				]
			];
			$cellChunk->setCommitRow(false);
			$cellChunk->setValue('value2');
			$cellChunk->setTimestampMicros(0);
			$cellChunk->setLabels([]);
			$cellChunk->setValueSize(10);
			$row = [];
			$chunkFormatter->rowInProgress($cellChunk, [], function ($err, $res) use (&$row) {
				$row = $res;
			});
			$expectedState = [
				'key'         => 'key',
				'data'        => [
					'family'     => [
						'qualifier' => [
							[
								'value'     => 'value2',
								'size'      => 10,
								'timestamp' => 0,
								'labels'    => [],
							]
						]
					]
				]
			];
			// print_r($chunkFormatter->row);
			// die;
			// $this->assertEquals($chunkFormatter->row, $expectedState, 'row state mismatch');
			$this->assertEquals($chunkFormatter->state, $chunkFormatter->RowStateEnum['CELL_IN_PROGRESS'], 'state mismatch');
		// });
	}

	/** @test */
	public function cellInProgress() {
		$chunkFormatter = new ChunkFormatter();
		$cellChunk      = new ReadRowsResponse_CellChunk();
		$StringValue    = new StringValue();
		$BytesValue     = new BytesValue();

		$callback = function ($err, $res) {return array('err' => $err, 'res' => $res);/*echo "callback<pre>"; print_r($res);*/};
		
		//it('should throw exception when resetRow and rowkey', function () {
		try {
			$cellChunk->setRowKey('key');
			$cellChunk->setResetRow(true);
			$chunkFormatter->rowInProgress($cellChunk, [], $callback);
		} catch (Exception $err) {
			//pass
			//print_r($err->getMessage());
			$this->assertEquals($err->getMessage(), 'A reset should have no data');
		}

		//it('should throw exception when resetRow and rowkey', function () {
			try {
				$cellChunk->setRowKey('key');
				$cellChunk->setResetRow(true);
				$chunkFormatter->rowInProgress($cellChunk, [], $callback);
			} catch (Exception $err) {
				//pass
				//print_r($err->getMessage());
				$this->assertEquals($err->getMessage(), 'A reset should have no data');
			}
			// $this->assert($cellInProgressSpy->threw());
		//});

		// it('should throw exception when resetRow and familyName', function () {
			try {
				$cellChunk->setResetRow(true);
				$StringValue->setValue('family');
				$cellChunk->setFamilyName($StringValue);
				
				$chunkFormatter->rowInProgress($cellChunk, [], $callback);
			} catch (Exception $err) {
				//pass
				//print_r($err->getMessage());
				$this->assertEquals($err->getMessage(), 'A reset should have no data');
			}
		// 	$this->assert($cellInProgressSpy->threw());
		// });

		// it('should throw exception when resetRow and qualifier', function () {
			try {
				$cellChunk->setResetRow(true);
				$BytesValue->setValue('qualifier');
				$cellChunk->setQualifier($BytesValue);
				$chunkFormatter->rowInProgress($cellChunk, [], $callback);
			} catch (Exception $err) {
				//pass
				//print_r($err->getMessage());
				$this->assertEquals($err->getMessage(), 'A reset should have no data');
			}
		// 	$this->assert($cellInProgressSpy->threw());
		// });

		// it('should throw exception when resetRow and value', function () {
			try {
				$cellChunk->setResetRow(true);
				$cellChunk->setValue('value');
				$chunkFormatter->rowInProgress($cellChunk, [], $callback);
			} catch (Exception $err) {
				//pass
				//print_r($err->getMessage());
				$this->assertEquals($err->getMessage(), 'A reset should have no data');
			}
		// 	$this->assert($cellInProgressSpy->threw());
		// });

		// it('should throw exception when resetRow and timestampMicros', function () {
			try {
				$cellChunk->setResetRow(true);
				$cellChunk->setTimestampMicros(10);
				$chunkFormatter->rowInProgress($cellChunk, [], $callback);
			} catch (Exception $err) {
				//pass
				//print_r($err->getMessage());
				$this->assertEquals($err->getMessage(), 'A reset should have no data');
			}
		// 	$this->assert($cellInProgressSpy->threw());
		// });

		//it('should throw exception when rowKey not equal to prevRowKey', function () {
			try {
				$chunkFormatter->row = ['key' => 'key'];
				$cellChunk->setRowKey('rowKey');
				$chunkFormatter->rowInProgress($cellChunk, [], $callback);
			} catch (Exception $err) {
				//pass
				//print_r($err->getMessage());
				$this->assertEquals($err->getMessage(), 'A reset should have no data');
			}
		// 	$this->assert($cellInProgressSpy->threw());
		// });

		// it('should throw exception when valueSize>0 and commitRow=true', function () {
			try {
				$cellChunk->setValueSize(10);
				$cellChunk->setCommitRow(true);
				$chunkFormatter->rowInProgress($cellChunk, [], $callback);
			} catch (Exception $err) {
				//pass
				//print_r($err->getMessage());
				$this->assertEquals($err->getMessage(), 'A commit is required between row keys');
			}
		// 	$this->assert($cellInProgressSpy->threw());
		// });

		// it('should return true on resetRow', function () {
			// $cellChunk->setResetRow(true);
			// $chunkFormatter->cellInProgress($chunk, [], $callback);
			// $this->assert($returnValue);
			// $this->assert(!$callback->called);
		// });

		// it('should produce row on commitRow', function () {
			$chunkFormatter->qualifier = [
				'value'     => 'value',
				'size'      => 0,
				'timestamp' => 0,
				'labels'    => [],
			];
			$chunkFormatter->qualifiers = [$chunkFormatter->qualifier];
			$chunkFormatter->family = [
				'qualifier' => $chunkFormatter->qualifiers
			];
			$chunkFormatter->row = [
				'key'     => 'key',
				'data'    => [
					'family' => $chunkFormatter->family,
				]
			];
			
			$cellChunk->setCommitRow(true);
			$cellChunk->setValue('2');
			$cellChunk->setValueSize(0);
			$row = [];
			$chunkFormatter->cellInProgress($cellChunk, [], function ($err, $res) use (&$row) {
				$row = $res;
			});
			// $this->assert($returnValue);
			// $this->assert($callback->called);
			$expectedRow = [
				'key'         => 'key',
				'data'        => [
					'family'     => [
						'qualifier' => [
							[
								'value'     => 'value2',
								'size'      => 0,
								'timestamp' => 0,
								'labels'    => [],
							]
						]
					]
				]
			];
			$this->assertEquals($row, $expectedRow, 'row mismatch');
			$this->assertEquals($chunkFormatter->state, $chunkFormatter->RowStateEnum['NEW_ROW'], 'state mismatch');
		// });

		// it('without commitRow should change state to rowInProgress', function () {
			$chunkFormatter->qualifier = [
				'value'     => 'value',
				'size'      => 0,
				'timestamp' => 0,
				'labels'    => [],
			];
			$chunkFormatter->qualifiers = [$chunkFormatter->qualifier];
			$chunkFormatter->family = [
				'qualifier' => $chunkFormatter->qualifiers
			];
			$chunkFormatter->row = [
				'key'     => 'key',
				'data'    => [
					'family' => $chunkFormatter->family
				],
			];
			$chunk = [
				'commitRow' => false,
				'value'     => '2',
				'valueSize' => 0
			];

			$cellChunk->setCommitRow(false);
			$cellChunk->setValue('2');
			$cellChunk->setValueSize(0);
			$row = [];
			$chunkFormatter->cellInProgress($cellChunk, [], function ($err, $res) use (&$row) {
				$row = $res;
			});
			// $this->assert(!$returnValue);
			// $this->assert(!$callback->called);
			$expectedState = [
				'key'         => 'key',
				'data'        => [
					'family'     => [
						'qualifier' => [
							[
								'value'     => 'value2',
								'size'      => 0,
								'timestamp' => 0,
								'labels'    => []
							]
						]
					]
				]
			];
			$this->assertEquals($chunkFormatter->row, $expectedState, 'row mismatch');
			$this->assertEquals($chunkFormatter->state, $chunkFormatter->RowStateEnum['ROW_IN_PROGRESS'], 'state mismatch');
		// });
	}

	public function formatChunks()
	{
		$chunkFormatter = new ChunkFormatter();
		$cellChunk      = new ReadRowsResponse_CellChunk();
		$callback = function ($err, $res) {return array('err' => $err, 'res' => $res);/*echo "callback<pre>"; print_r($res);*/};
		
		// it('when current state returns true it should reset state', function () {
			/*$chunkFormatter->state = sinon.spy(function() {
			return true;
			});*/
			$chunkFormatter->state = 1;
			$chunkFormatter->row = ['key' => 'key'];
			// $chunks = [['key'             => 'key']];
			$cellChunk->setRowKey('key');
			
			$chunkFormatter->formatChunks([$cellChunk], [], $callback);
			$this->assertEquals($chunkFormatter->row, [], ' state mismatch');
		// });
	}

}

//$ChunkFormatterTest = new ChunkFormatterTest();
// $ChunkFormatterTest->newRow();
//$ChunkFormatterTest->rowInProgress();

?>