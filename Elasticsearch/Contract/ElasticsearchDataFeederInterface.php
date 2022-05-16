<?php

namespace App\Services\Elasticsearch\Contract;

use Illuminate\Support\Collection;

interface ElasticsearchDataFeederInterface
{
    public function sync():void;
}