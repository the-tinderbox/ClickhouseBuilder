<?php

namespace Tinderbox\ClickhouseBuilder\Integrations\Laravel;

use Illuminate\Support\ServiceProvider;

/**
 * Service provider to connect Clickhouse driver in Laravel.
 */
class ClickhouseServiceProvider extends ServiceProvider
{
    public function boot()
    {
        $db = $this->app->make('db');

        $db->extend('clickhouse', function ($config, $name) {
            $config['name'] = $name;

            $connection = new Connection($config);

            if ($this->app->bound('events')) {
                $connection->setEventDispatcher($this->app['events']);
            }

            return $connection;
        });
    }
}
