<?php

namespace App\Services\Elasticsearch\Feeder;

use App\Models\ElasticIndexs\AutoComplete;
use App\Models\Video;
use App\Services\Elasticsearch\Contract\ElasticsearchDataFeederInterface;
use App\Support\Logger;
use Illuminate\Support\Collection;

class AutoCompleteSearch implements ElasticsearchDataFeederInterface
{

    private AutoComplete $autoCompleteSearch;

    public function __construct()
    {
      $this->autoCompleteSearch  = new AutoComplete();
    }

    public function sync(): void
    {
        $this->autoCompleteSearch
             ->deleteIndexIfExists()
             ->sleep()
             ->createNewIndex();
        Video::query()->chunk(100, function (Collection $record) {
            $starttime = microtime(true);
            echo "Indexing 100 \n";
            $this->autoCompleteSearch->sendDataToElasticsearch($this->format($record));
            echo "Indexing finished in ".(microtime(true) - $starttime)." seconds \n";
        });
    }


    public function format(Collection $collection): array
    {
        $data = [];
        $collection->each(function (Video $video) use (&$data) {
            $data[] = $this->getIndexParamsFor($video->id);
            $data[] = $video->toResources();
        });
         return $data;
    }

    private function getIndexParamsFor($id):array
    {
        return [
            'index' => [
                '_index' => $this->autoCompleteSearch->getIndex(),
                '_id'    => $id,
            ]
        ];
    }
}
