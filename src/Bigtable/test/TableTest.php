<?php
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);

require '../vendor/autoload.php';
putenv('GOOGLE_APPLICATION_CREDENTIALS=../Grass_Clump_479-b5c624400920.json');

use Google\Bigtable\Admin\V2\Table;
use Google\Protobuf\GPBEmpty;
use Google\Cloud\Bigtable\src\BigtableTable;
use PHPUnit\Framework\TestCase;

echo "<pre>";

/**
 *
 */
class TableTest extends TestCase
{
    const PROJECT_ID = 'grass-clump-479';
    const INSTANCE_ID = 'php-perf';
    const TABLE_ID = 'myTableId';

    private $bigTable;

    /** @test */
    public function setUp()
    {
        $this->bigTable = new BigtableTable();
        $this->assertInstanceOf(BigtableTable::class, $this->bigTable);
    }

    
    public function instanceName(){
        $name = $this->bigTable->instanceName(self::PROJECT_ID, self::INSTANCE_ID);
        $this->assertEquals($name, 'projects/'.self::PROJECT_ID.'/instances/'.self::INSTANCE_ID);
    }

    
    public function createTable()
    {
        $parent = $this->bigTable->instanceName(self::PROJECT_ID, self::INSTANCE_ID);
        try{
            $table = $this->bigTable->createTable($parent, self::TABLE_ID);
            $tableName = $this->bigTable->tableName(self::PROJECT_ID, self::INSTANCE_ID, self::TABLE_ID);

            $this->assertEquals($table->getName(), $tableName);
            $this->assertInstanceOf(Table::class, $table);
        }
        catch(Exception $err){
            $err = json_decode($err->getMessage());
            $this->assertEquals($err->status, 'ALREADY_EXISTS');
        }
    }

    
    public function tableName()
    {
        $tableName = $this->bigTable->tableName(self::PROJECT_ID, self::INSTANCE_ID, self::TABLE_ID);
        $this->assertEquals($tableName, 'projects/'.self::PROJECT_ID.'/instances/'.self::INSTANCE_ID.'/tables/'.self::TABLE_ID);
    }

    /** @test */
    public function deleteTable()
    {
        try{
            $tableName = $this->bigTable->tableName(self::PROJECT_ID, self::INSTANCE_ID, self::TABLE_ID);
            $table = $this->bigTable->deleteTable($tableName);
            // print_r($table);
            $this->assertInstanceOf(GPBEmpty::class, $table);
        }
        catch(Exception $err){
            $err = json_decode($err->getMessage());
            $this->assertEquals($err->status, 'NOT_FOUND');
        }
    }
    
    /** @test */
    public function createTableWithColumnFamily()
    {
        $parent = $this->bigTable->instanceName(self::PROJECT_ID, self::INSTANCE_ID);
        $columnFamily = 'cf';
        $table = $this->bigTable->createTableWithColumnFamily($parent, self::TABLE_ID, $columnFamily);
        // $test = $table->getColumnFamilies()->getIterator()->current();
        // print_r(get_class_methods( $test) );
        // print_r($test);
        // print_r(get_class_methods( $table) );
        
        $tableName = $this->bigTable->tableName(self::PROJECT_ID, self::INSTANCE_ID, self::TABLE_ID);

        $this->assertEquals($table->getName(), $tableName);
    }
}

// $BigtableTableTest = new TableTest();
// $BigtableTableTest->setUp();

?>
