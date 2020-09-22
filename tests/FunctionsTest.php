<?php

namespace Tinderbox\ClickhouseBuilder;

use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Client;
use Tinderbox\Clickhouse\Common\File;
use Tinderbox\Clickhouse\Common\FileFromString;
use Tinderbox\Clickhouse\Common\Format;
use Tinderbox\Clickhouse\Common\TempTable;
use Tinderbox\Clickhouse\Interfaces\FileInterface;
use Tinderbox\Clickhouse\Server;
use Tinderbox\Clickhouse\ServerProvider;
use Tinderbox\ClickhouseBuilder\Exceptions\BuilderException;
use Tinderbox\ClickhouseBuilder\Query\Builder;
use Tinderbox\ClickhouseBuilder\Query\Expression;
use Tinderbox\ClickhouseBuilder\Query\Identifier;

class FunctionsTest extends TestCase
{
    public function testTap()
    {
        $value = 1;
        $result = 0;
        $callback = function () use (&$result) {
            $result = 2;
        };

        $returnedValue = tap($value, $callback);

        $this->assertEquals($value, $returnedValue);
        $this->assertEquals(2, $result);
    }

    public function testArrayFlatten()
    {
        $array = [
            'first' => [
                'second' => [
                    'third' => 'value',
                ],
            ],
        ];

        $flatten = array_flatten($array);

        $this->assertEquals(['value'], $flatten);

        $flatten = array_flatten($array, 1);

        $this->assertEquals(
            [
                [
                    'third' => 'value',
                ],
            ],
            $flatten
        );
    }

    public function testRaw()
    {
        $expression = raw('test');

        $this->assertInstanceOf(Expression::class, $expression);
    }

    protected function putInTempFile(string $content): string
    {
        $fileName = tempnam(sys_get_temp_dir(), 'builder_');
        file_put_contents($fileName, $content);

        return $fileName;
    }

    public function testInsertIntoMemory()
    {
        $server = new Server('127.0.0.1');
        $client = new Client((new ServerProvider())->addServer($server));

        $realFiles = [
            $this->putInTempFile('5'.PHP_EOL.'6'.PHP_EOL),
            $this->putInTempFile('7'.PHP_EOL.'8'.PHP_EOL),
            $this->putInTempFile('9'.PHP_EOL.'10'.PHP_EOL),
        ];

        $files = [
            '1'.PHP_EOL.'2'.PHP_EOL,
            new FileFromString('3'.PHP_EOL.'4'.PHP_EOL),
            new File($realFiles[0]),
            $realFiles[2],
        ];

        $client->write([
            ['query' => 'drop table if exists default.builder_test'],
            ['query' => 'create table if not exists default.builder_test (number UInt64, string String) engine = Memory'],
        ], 1);

        foreach ($files as $file) {
            $builder = new Builder($client);
            $builder->table(new Identifier('default.builder_test'))->format(Format::TSV)->values($file);

            $this->assertTrue(into_memory_table($builder, ['number' => 'UInt64']));

            $builder = new Builder($client);
            $result = $builder->table('builder_test')->get();

            $this->assertEquals(2, count($result->rows));
        }

        $tempTable = new TempTable('builder_test', new File($realFiles[1]), ['number' => 'UInt64'], Format::TSV);
        $builder = new Builder($client);
        $builder->values($tempTable);

        $this->assertTrue(into_memory_table($builder));

        $builder = new Builder($client);
        $result = $builder->table('builder_test')->get();

        $this->assertEquals(2, count($result->rows), 'Correctly inserts data using info from TempTable');

        $this->expectException(BuilderException::class);
        $this->expectExceptionMessage('No structure provided for insert in memory table');

        $builder = new Builder($client);
        $builder->table(new Identifier('default.builder_test'))->format(Format::TSV)->values($files[0]);

        into_memory_table($builder);
    }

    public function testFileFrom()
    {
        $realFiles = [
            $this->putInTempFile('5'.PHP_EOL.'6'.PHP_EOL),
            $this->putInTempFile('7'.PHP_EOL.'8'.PHP_EOL),
            $this->putInTempFile('9'.PHP_EOL.'10'.PHP_EOL),
        ];

        $files = [
            '1'.PHP_EOL.'2'.PHP_EOL,
            new FileFromString('3'.PHP_EOL.'4'.PHP_EOL),
            new File($realFiles[0]),
            new TempTable('test', new File($realFiles[1]), ['number' => 'UInt64']),
            $realFiles[2],
        ];

        foreach ($files as $file) {
            $file = file_from($file);

            $this->assertInstanceOf(FileInterface::class, $file);
        }
    }
}
