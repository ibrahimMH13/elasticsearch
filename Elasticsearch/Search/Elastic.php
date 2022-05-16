<?php

namespace App\Services\Search;

use App\Models\ElasticIndexs\AutoComplete;

class Elastic
{

    public function lookFor($query,$size){
     return AutoComplete::search($query,$size);
    }
    public function autoComplete($query){
         $records = AutoComplete::search($query);
         $result = array_map(function ($record) {
          return $record['title'];
        },$records);
         return array_values($result);
    }
}
