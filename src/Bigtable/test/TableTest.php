<?php
require '../vendor/autoload.php';

use Google\Bigtable\Admin\V2\ColumnFamily;
use Google\Bigtable\Admin\V2\GcRule;

use Google\Bigtable\Admin\V2\Table;
use Google\Bigtable\V2\Mutation;
use Google\Bigtable\V2\Mutation_SetCell;
use Google\Cloud\Bigtable\src\BigtableTable;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\MapField;
use PHPUnit\Framework\TestCase;
use Prophecy\Argument;

/**
 *
 */

class TableTest extends TestCase {
	const PROJECT_ID  = 'grass-clump-479';
	const INSTANCE_ID = 'php-perf';
	const TABLE_ID    = 'myTableId';

	/** @test */
	public function testInstanceName() {
		$expected = 'projects/'.self::PROJECT_ID.'/instances/'.self::INSTANCE_ID;

		$mock = $this->createMock(BigtableTable::class );
		$mock->method('instanceName')
		     ->willReturn($expected);

		$formatedName = $mock->instanceName(Argument::type('string'), Argument::type('string'));
		$this->assertEquals($formatedName, $expected);
	}

	/** @test */
	public function testCreateTable() {
		$parent    = 'projects/'.self::PROJECT_ID.'/instances/'.self::INSTANCE_ID;
		$fakeTable = new Table();
		$fakeTable->setName($parent);

		$mock = $this->createMock(BigtableTable::class );
		$mock->method('createTable')
		     ->willReturn($fakeTable);

		$table = $mock->createTable(Argument::type('string'), Argument::type('string'));
		$this->assertEquals($table->getName(), $parent);
		$this->assertInstanceOf(Table::class , $table);
	}

	/** @test */
	public function testTableName() {
		$expected = 'projects/'.self::PROJECT_ID.'/instances/'.self::INSTANCE_ID.'/tables/'.self::TABLE_ID;

		$mock = $this->createMock(BigtableTable::class );
		$mock->method('tableName')
		     ->willReturn($expected);

		$formatedName = $mock->tableName(Argument::type('string'), Argument::type('string'), Argument::type('string'));
		$this->assertEquals($formatedName, $expected);
	}

	/** @test */
	public function testCreateTableWithColumnFamily() {
		$columnFamily = 'cf';
		$parent       = 'projects/'.self::PROJECT_ID.'/instances/'.self::INSTANCE_ID;

		$fakeTable = new Table();
		$fakeTable->setName($parent);

		$BigtableTable = new BigtableTable();
		$MapField      = $BigtableTable->columnFamily(3, $columnFamily);
		$fakeTable->setColumnFamilies($MapField);
		$fakeTable->setGranularity(2);

		$mock = $this->createMock(BigtableTable::class );
		$mock->method('createTableWithColumnFamily')
		     ->willReturn($fakeTable);
		$table = $mock->createTableWithColumnFamily(Argument::type('integer'), Argument::type('string'), Argument::type('string'));

		$this->assertInstanceOf(Table::class , $table);
		$this->assertEquals($table->getName(), $parent);
	}

	/** @test */
	public function testColumnFamily() {
		$columnFamily = 'cf';
		$gcRule       = new GcRule();
		$gcRule->setMaxNumVersions(2);

		$cf = new ColumnFamily();
		$cf->setGcRule($gcRule);

		$MapField                = new MapField(GPBType::STRING, GPBType::MESSAGE, ColumnFamily::class );
		$MapField[$columnFamily] = $cf;

		// print_r(get_class_methods($MapField));

		$mock = $this->createMock(BigtableTable::class );
		$mock->method('columnFamily')
		     ->willReturn($MapField);
		$MapField = $mock->columnFamily(Argument::type('integer'), Argument::type('string'), Argument::type('string'));

		$this->assertInstanceOf(MapField::class , $MapField);
	}

	/** @test */
	public function testGetTable() {
		$expected  = 'projects/'.self::PROJECT_ID.'/instances/'.self::INSTANCE_ID.'/tables/'.self::TABLE_ID;
		$fakeTable = new Table();
		$fakeTable->setName($expected);

		$mock = $this->createMock(BigtableTable::class );
		$mock->method('getTable')
		     ->willReturn($fakeTable);
		$table = $mock->getTable(Argument::type('string'));

		$this->assertInstanceOf(Table::class , $table);
		$this->assertEquals($table->getName(), $expected);
	}

	/** @test */
	public function mutationCell() {
		$cell['cf']        = 'cf';
		$cell['qualifier'] = 'qualifier';
		$cell['value']     = 'value';

		$utc_str           = gmdate("M d Y H:i:s", time());
		$utc               = strtotime($utc_str);
		$cell['timestamp'] = $utc;

		$Mutation_SetCell = new Mutation_SetCell();
		$Mutation_SetCell->setFamilyName($cell['cf']);
		$Mutation_SetCell->setColumnQualifier($cell['qualifier']);
		$Mutation_SetCell->setValue($cell['value']);
		$Mutation_SetCell->setTimestampMicros($cell['timestamp']);

		$Mutation = new Mutation();
		$Mutation->setSetCell($Mutation_SetCell);

		$mock = $this->createMock(BigtableTable::class );
		$mock->method('mutationCell')
		     ->willReturn($Mutation);

		$mutationCell = $mock->mutationCell(Argument::type('array'));
		$this->assertInstanceOf(Mutation::class , $mutationCell);

	}
}
?>
