<?php

namespace Tinderbox\ClickhouseBuilder\Integrations\Laravel;

use Illuminate\Support\ServiceProvider;

/**
 * Service provider to connect Clickhouse driver in Laravel
 */
class ClickhouseServiceProvider extends ServiceProvider
{

    public function boot()
    {
        $db = $this->app->make('db');

        $db->extend('clickhouse', function ($config, $name) {
            $config['name'] = $name;
            return new Connection($config);
        });
    }
}