<?php

describe('ChunkFormatter', function () {
		$ChunkFormatter;
		$chunkFormatter;

		beforeEach(function () {
				$chunkFormatter = new ChunkFormatter();
			});

		describe('instantiation', function () {
				it('should have initial state', function () {
						$this->assert($chunkFormatter instanceof $ChunkFormatter);
						$this->prevRowKey = '';
						$this->family = [];
						$this->qualifiers = [];
						$this->qualifier = [];
						$this->row = [];
						$this->state = $this->newRow;
						$this->assertDeepEqual($chunkFormatter->row, [], 'invalid initial state');
						$this->assertDeepEqual($chunkFormatter->prevRowKey, '', 'invalid initial state');
						$this->assertDeepEqual($chunkFormatter->family, [], 'invalid initial state');
						$this->assertDeepEqual($chunkFormatter->qualifiers, [], 'invalid initial state');
						$this->assertDeepEqual($chunkFormatter->qualifier, [], 'invalid initial state');
						$this->assertDeepEqual(
							$chunkFormatter->state,
							$chunkFormatter->newRow,
							'invalid initial state'
						);
					});
				it('calling as function should return chunkformatter instance', function () {
						$instance = $ChunkFormatter();
						$this->assert($instance instanceof $ChunkFormatter);
					});
			});

		describe('newRow', function () {
				$newRowSpy;
				$callback;
				/*beforeEach(function() {
				newRowSpy = sinon.spy(chunkFormatter, 'newRow');
				callback = sinon.spy();
				});*/

				it('should throw exception when row key is undefined ', function () {
					try {
						$chunkFormatter->row = array('key' => 'abc');
						$newRowSpy->call($chunkFormatter, [], [], callback);
					} catch (Exception $err) {
						//pass
					}
					$this->$this->assert($newRowSpy->threw());
				});
				it('should throw exception when chunk key is undefined ', function () {
						try {
							$newRowSpy->call($chunkFormatter, [], [], callback);
						} catch (Exception $err) {
							//pass
						}
						$this->$this->assert($newRowSpy->threw());
					});
				it('should throw exception when resetRow is true ', function () {
						try {
							$newRowSpy->call(
								$chunkFormatter,
								['rowKey' => 'key', 'resetRow' => true],
								[],
								$callback
							);
						} catch (Exception $err) {
							//pass
						}
						$this->$this->assert($newRowSpy->threw());
					});
				it('should throw exception when resetRow', function () {
						try {
							$newRowSpy->call($chunkFormatter, ['resetRow' => true], [], $callback);
						} catch (Exception $err) {
							//pass
						}
						$this->$this->assert($newRowSpy->threw());
					});
				it('should throw exception when row key is equal to previous row key ', function () {
						$chunkFormatter->prevRowKey = 'key';
						try {
							$newRowSpy->call(
								$chunkFormatter,
								['rowKey' => 'key', 'resetRow' => false],
								[],
								$callback
							);
						} catch (Exception $err) {
							//pass
						}
						$this->$this->assert($newRowSpy->threw());
					});
				it('should throw exception when family name is undefined ', function () {
						try {
							$newRowSpy->call($chunkFormatter, ['rowKey' => 'key'], [], $callback);
						} catch (Exception $err) {
							//pass
						}
						$this->$this->assert($newRowSpy->threw());
					});
				it('should throw exception when qualifier is undefined ', function () {
						try {
							$newRowSpy->call(
								$chunkFormatter,
								['rowKey' => 'key', 'familyName' => 'family'],
								[],
								$callback
							);
						} catch (Exception $err) {
							//pass
						}
						$this->$this->assert($newRowSpy->threw());
					});
				it('should throw exception when valueSize>0 and commitRow=true ', function () {
						try {
							$newRowSpy->call(
								$chunkFormatter,
								[
									'rowKey'     => 'key',
									'familyName' => 'family',
									'qualifier'  => 'qualifier',
									'valueSize'  => 10,
									'commitRow'  => true
								],
								[],
								$callback
							);
						} catch (Exception $err) {
							//pass
						}
						$this->$this->assert($newRowSpy->threw());
					});
				it('should commit 1 row ', function () {
						$chunk = array(
							'rowKey'          => 'key',
							'familyName'      => ['value'      => 'family'],
							'qualifier'       => ['value'       => 'qualifier'],
							'valueSize'       => 0,
							'timestampMicros' => 0,
							'labels'          => [],
							'commitRow'       => true,
							'value'           => 'value',
						);
						$returnValue = $chunkFormatter->newRow($chunk, [], $callback);
						$this->$this->assert($returnValue, true, 'reset state failed');
						$this->$this->assert($callback->called);
						$this->assertEqual(
							$chunkFormatter->prevRowKey,
							$chunk['rowKey'],
							'wrong state prevrowkey'
						);
						//let row = callback.getCall(0).args[1];
						$row = $callback->getCall(0)[args[1]];
						$expectedRow = [
							'key'           => $chunk['rowKey'],
							'data'          => [
								'family'       => [
									'qualifier'   => [
										'0'          => [
											'value'     => $chunk['value'],
											'timestamp' => $chunk['timestampMicros'],
											'labels'    => $chunk['labels'],
											'size'      => $chunk['valueSize'],
										]
									]
								]
							]
						];
						$this->assertDeepEqual($row, $expectedRow);
					});
				it('partial row', function () {
						$chunk = array(
							'rowKey'          => 'key',
							'familyName'      => ['value'      => 'family'],
							'qualifier'       => ['value'       => 'qualifier'],
							'valueSize'       => 0,
							'timestampMicros' => 0,
							'labels'          => [],
							'commitRow'       => false,
							'value'           => 'value',
						);
						$returnValue = $chunkFormatter->newRow($chunk, [], $callback);
						$this->assertEqual($returnValue, false, 'reset state failed');
						$this->assertEqual($callback->called, false, 'wrong callback');
						$partialRow = [
							'key'           => $chunk['rowKey'],
							'data'          => [
								'family'       => [
									'qualifier'   => [
										'0'          => [
											'value'     => $chunk['value'],
											'timestamp' => $chunk['timestampMicros'],
											'labels'    => $chunk['labels'],
											'size'      => $chunk['valueSize'],
										]
									]
								]
							]
						];
						$this->assertEqual($chunkFormatter->row, $partialRow);
						$this->assertEqual(
							$chunkFormatter->state,
							$chunkFormatter->rowInProgress,
							'wrong state'
						);
					});
				it('partial cell', function () {
						$chunk = [
							'rowKey'          => 'key',
							'familyName'      => ['value'      => 'family'],
							'qualifier'       => ['value'       => 'qualifier'],
							'valueSize'       => 10,
							'timestampMicros' => 0,
							'labels'          => [],
							'commitRow'       => false,
							'value'           => 'value',
						];
						$returnValue = $chunkFormatter->newRow($chunk, [], $callback);
						$this->assertEqual($returnValue, false, 'reset state failed');
						$this->assertEqual($callback->called, false, 'wrong callback');
						$partialRow = [
							'key'           => $chunk['rowKey'],
							'data'          => [
								'family'       => [
									'qualifier'   => [
										'0'          => [
											'value'     => $chunk['value'],
											'timestamp' => $chunk['timestampMicros'],
											'labels'    => $chunk['labels'],
											'size'      => $chunk['valueSize'],
										]
									]
								]
							]
						];
						$this->assertEqual($chunkFormatter->row, $partialRow);
						$this->assertEqual(
							$chunkFormatter->state,
							$chunkFormatter->cellInProgress,
							'wrong state'
						);
					});
			});

		describe('rowInProgress', function () {
				$rowInProgress;
				$callback;
				beforeEach(function () {
						$rowInProgress = sinon.spy($chunkFormatter, 'rowInProgress');
						$callback = sinon.spy();
					});
				it('should throw exception when resetRow and rowkey', function () {
						try {
							$rowInProgress->call(
								$chunkFormatter,
								['resetRow' => true, 'rowKey' => 'key'],
								[],
								$callback
							);
						} catch (Exception $err) {
							//pass
						}
						$this->$this->assert($rowInProgress->threw());
					});
				it('should throw exception when resetRow and familyName', function () {
						try {
							$rowInProgress->call(
								$chunkFormatter,
								['resetRow' => true, 'familyName' => 'family'],
								[],
								$callback
							);
						} catch (Exception $err) {
							//pass
						}
						$this->$this->assert($rowInProgress->threw());
					});
				it('should throw exception when resetRow and qualifier', function () {
						try {
							$rowInProgress->call(
								$chunkFormatter,
								['resetRow' => true, 'qualifier' => 'qualifier'],
								[],
								$callback
							);
						} catch (Exception $err) {
							//pass
						}
						$this->$this->assert($rowInProgress->threw());
					});
				it('should throw exception when resetRow and value', function () {
						try {
							$rowInProgress->call(
								$chunkFormatter,
								['resetRow' => true, 'value' => 'value'],
								[],
								$callback
							);
						} catch (Exception $err) {
							//pass
						}
						$this->$this->assert($rowInProgress->threw());
					});
				it('should throw exception when resetRow and timestampMicros', function () {
						try {
							$rowInProgress->call(
								$chunkFormatter,
								['resetRow' => true, 'timestampMicros' => 10],
								[],
								$callback
							);
						} catch (Exception $err) {
							//pass
						}
						$this->$this->assert($rowInProgress->threw());
					});
				it('should throw exception when rowKey not equal to prevRowKey', function () {
						try {
							$chunkFormatter->row = ['key'                   => 'key1'];
							$rowInProgress->call($chunkFormatter, ['rowKey' => 'key'], [], $callback);
						} catch (Exception $err) {
							//pass
						}
						$this->$this->assert($rowInProgress->threw());
					});
				it('should throw exception when valueSize>0 and commitRow=true', function () {
						try {
							$rowInProgress->call(
								$chunkFormatter,
								['valueSize' => 10, 'commitRow' => true],
								[],
								$callback
							);
						} catch (Exception $err) {
							//pass
						}
						$this->assert($rowInProgress->threw());
					});
				it('should throw exception when familyName without qualifier', function () {
						try {
							$rowInProgress->call(
								$chunkFormatter,
								['familyName' => 'family'],
								[],
								$callback
							);
						} catch (Exception $err) {
							//pass
						}
						$this->assert($rowInProgress->threw());
					});
				it('should return true on resetRow ', function () {
						$chunk = ['resetRow' => true];
						$returnValue = $chunkFormatter->rowInProgress($chunk, [], $callback);
						$this->assert($returnValue);
						$this->assert(!$callback.called);
					});
				it('bare commitRow should produce qualifer ', function () {
						$chunkFormatter->qualifiers = [];
						$chunkFormatter->row = [
							'key'         => 'key',
							'data'        => [
								'family'     => [
									'qualifier' => $chunkFormatter->qualifiers,
								]
							]
						];
						$chunk = ['commitRow' => true];
						$returnValue = $chunkFormatter->rowInProgress($chunk, [], $callback);
						$this->assert($returnValue);
						$this->assert($callback->called);
						$expectedRow = [
							'key'         => 'key',
							'data'        => [
								'family'     => [
									'qualifier' => [
										[
											'value'     => '',
											'size'      => '',
											'timestamp' => '',
											'labels'    => '',
										]
									]
								]
							]
						];
						$row = $callback->getCall(0)[$args[1]];
						$this->assertDeepEqual($row, $expectedRow, 'row mismatch');
						assert.equal(
							$chunkFormatter->state,
							$chunkFormatter->newRow,
							'state mismatch'
						);
					});
				it('chunk with qualifier and commit should produce row ', function () {
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
						$chunk = [
							'commitRow'       => true,
							'qualifier'       => ['value'       => 'qualifier2'],
							'value'           => 'value',
							'timestampMicros' => 0,
							'labels'          => [],
							'valueSize'       => 0,
						];
						$returnValue = $chunkFormatter->rowInProgress($chunk, [], $callback);
						$this->assert($returnValue);
						$this->assert($callback->called);
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
											'labels'    => [],
										]
									]
								]
							]
						];
						$row = $callback->getCall(0)['args'][1];
						$this->assertDeepEqual($row, $expectedRow, 'row mismatch');
						assert.equal(
							$chunkFormatter->state,
							$chunkFormatter->newRow,
							'state mismatch'
						);
					});
				it('chunk with new family and commitRow should produce row', function () {
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
						$chunk = [
							'commitRow'       => true,
							'familyName'      => ['value'      => 'family2'],
							'qualifier'       => ['value'       => 'qualifier2'],
							'value'           => 'value',
							'timestampMicros' => 0,
							'labels'          => [],
							'valueSize'       => 0,
						];
						$returnValue = $chunkFormatter->rowInProgress($chunk, [], $callback);
						$this->assert($returnValue);
						$this->assert($callback->called);
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
											'labels'    => []
										]
									]
								]
							]
						];
						$row = $callback->getCall(0)['args'][1];
						$this->assertDeepEqual($row, $expectedRow, 'row mismatch');
						assert.equal(
							$chunkFormatter->state,
							$chunkFormatter->newRow,
							'state mismatch'
						);
					});
				it('partial cell ', function () {
						$chunkFormatter->qualifiers = [];
						$chunkFormatter->row = [
							'key'         => 'key',
							'data'        => [
								'family'     => [
									'qualifier' => $chunkFormatter->qualifiers,
								]
							]
						];
						$chunk = [
							'commitRow'       => false,
							'value'           => 'value2',
							'valueSize'       => 10,
							'timestampMicros' => 0,
							'labels'          => [],
						];
						$returnValue = $chunkFormatter->rowInProgress($chunk, [], $callback);
						$this->assert(!$returnValue);
						$this->assert(!$callback->called);
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
									],
								]
							]
						];
						$this->assertDeepEqual($chunkFormatter->row, $expectedState, 'row state mismatch');
						assert.equal(
							$chunkFormatter->state,
							$chunkFormatter->cellInProgress,
							'state mismatch'
						);
					});
			});

		describe('cellInProgress', function () {
				$cellInProgressSpy;
				$callback;
				beforeEach(function () {
						// $cellInProgressSpy = sinon.spy(chunkFormatter, 'cellInProgress');
						//$callback = sinon.spy();
					});
				it('should throw exception when resetRow and rowkey', function () {
						try {
							$cellInProgressSpy->call(
								$this,
								['resetRow' => true, 'rowKey' => 'key'],
								[],
								$callback
							);
						} catch (Exception $err) {
							//pass
						}
						$this->assert($cellInProgressSpy->threw());
					});
				it('should throw exception when resetRow and familyName', function () {
						try {
							$cellInProgressSpy->call(
								$this,
								['resetRow' => true, 'familyName' => 'family'],
								[],
								$callback
							);
						} catch (Exception $err) {
							//pass
						}
						$this->assert($cellInProgressSpy->threw());
					});
				it('should throw exception when resetRow and qualifier', function () {
						try {
							$cellInProgressSpy->call(
								$this,
								['resetRow' => true, 'qualifier' => 'qualifier'],
								[],
								$callback
							);
						} catch (Exception $err) {
							//pass
						}
						$this->assert($cellInProgressSpy->threw());
					});
				it('should throw exception when resetRow and value', function () {
						try {
							$cellInProgressSpy->call(
								$this,
								['resetRow' => true, 'value' => 'value'],
								[],
								$callback
							);
						} catch (Exception $err) {
							//pass
						}
						$this->assert($cellInProgressSpy->threw());
					});
				it('should throw exception when resetRow and timestampMicros', function () {
						try {
							$cellInProgressSpy->call(
								$this,
								['resetRow' => true, 'timestampMicros' => 10],
								[],
								$callback
							);
						} catch (Exception $err) {
							//pass
						}
						$this->assert($cellInProgressSpy->threw());
					});
				it('should throw exception when rowKey not equal to prevRowKey', function () {
						try {
							$chunkFormatter->row = ['key'             => 'key'];
							$cellInProgressSpy->call($this, ['rowKey' => 'rowKey'], [], $callback);
						} catch (Exception $err) {
							//pass
						}
						$this->assert($cellInProgressSpy->threw());
					});
				it('should throw exception when valueSize>0 and commitRow=true', function () {
						try {
							$cellInProgressSpy->call(
								$this,
								['valueSize' => 10, 'commitRow' => true],
								[],
								$callback
							);
						} catch (Exception $err) {
							//pass
						}
						$this->assert($cellInProgressSpy->threw());
					});
				it('should return true on resetRow', function () {
						$chunk = ['resetRow' => true];
						$returnValue = $chunkFormatter->cellInProgress($chunk, [], $callback);
						$this->assert($returnValue);
						$this->assert(!$callback->called);
					});
				it('should produce row on commitRow', function () {
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
						$chunk = [
							'commitRow' => true,
							'value'     => '2'
						];
						$returnValue = $chunkFormatter->cellInProgress($chunk, [], $callback);
						$this->assert($returnValue);
						$this->assert($callback->called);
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
						$row = $callback->getCall(0)['args'][1];
						$this->assertDeepEqual($row, $expectedRow, 'row mismatch');
						assert.equal(
							$chunkFormatter->state,
							$chunkFormatter->newRow,
							'state mismatch'
						);
					});
				it('without commitRow should change state to rowInProgress', function () {
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
						$returnValue = $chunkFormatter->cellInProgress($chunk, [], $callback);
						$this->assert(!$returnValue);
						$this->assert(!$callback->called);
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
						$this->assertDeepEqual($chunkFormatter->row, $expectedState, 'row mismatch');
						assert.equal(
							$chunkFormatter->state,
							$chunkFormatter->rowInProgress,
							'state mismatch'
						);
					});
			});

		describe('onStreamEnd', function () {
				$onStreamEndSpy;
				beforeEach(function () {
						// $onStreamEndSpy = sinon.spy(chunkFormatter, 'onStreamEnd');
					});
				it('pending row should throw exception', function () {
						$chunkFormatter->row = ['key' => 'key'];
						try {
							$onStreamEndSpy->call($chunkFormatter);
						} catch (Exception $err) {
							//pass
						}
						$this->assert($onStreamEndSpy->threw());
					});
				it('completed row should complete successfully', function () {
						$chunkFormatter->row = [];
						try {
							$onStreamEndSpy->call($chunkFormatter);
						} catch (Exception $err) {
							//pass
						}
						$this->assert(!$onStreamEndSpy->threw());
					});
			});

		describe('formatChunks', function () {
				$callback;
				beforeEach(function () {
						// $callback = sinon.spy();
					});
				it('when current state returns true it should reset state', function () {
						/*$chunkFormatter->state = sinon.spy(function() {
						return true;
						});*/
						$chunkFormatter->row = ['key' => 'key'];
						$chunks = [['key'             => 'key']];
						$chunkFormatter->formatChunks($chunks, [], $callback);
						$this->assertDeepEqual($chunkFormatter->row, [], ' state mismatch');
					});
				it('when current state returns false it should keep state', function () {
						/*$chunkFormatter->state = sinon.spy(function() {
						return false;
						});*/
						$chunkFormatter->row = ['key' => 'key'];
						$chunks = [[key               => 'key']];
						$chunkFormatter->formatChunks($chunks, [], $callback);
						$this->assertDeepEqual($chunkFormatter->row, ['key' => 'key'], ' state mismatch');
					});
			});
	});
?>