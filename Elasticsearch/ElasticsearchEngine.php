<?php


namespace App\Services\Elasticsearch;


use Elasticsearch\Client;
use Illuminate\Support\Arr;

class ElasticsearchEngine
{


    private Client $clientBuilder;

    public function __construct(Client $clientBuilder)
    {
        $this->clientBuilder = $clientBuilder;
    }

    public function update($models)
    {
        //its function there in Searchable treat
        // $model->searchableAs() get index name , we can overwrite
        //toSearchableArray this select what should index. also we can overwirte as return array with
        //column want we indexs ,alloverwrite should be in model
        $models->each(function ($model) {
            $params = [
/*                'index' => $model->searchableAs(),
                'type'  => $model->searchableAs(),*/
                'id'    => $model->id,
                'body'  => $model->toSearchableArray()
            ];
            $this->clientBuilder->index($this->getRequestBody($model,$params));
        });
    }

    public function delete($models)
    {
       $models->each(function ($model){
           $params =[
/*               'index'=> $model->searchableAs(),
               'type'=> $model->searchableAs(),*/
               'id'=> $model->id,
           ];
           $this->clientBuilder->delete($this->getRequestBody($model,$params));
       });
    }

    public function search(Builder $builder)
    {
        //movie to performSeasrch to make flexibility params
        if ($builder->model::ELASTIC_limited){
            $op =[
                "from"=>$builder->model::ELASTIC_FROM,//0,CAN SET HERE OR FROM MODEL
                "size"=> $builder->model::ELASTIC_SIZE,//555,
            ];
        }

        //if want disable limit just comment the $op array
       return $this->performSearch($builder,$op??[]);
    }

    protected function performSearch(Builder $builder, array $options = [])
    {
        //write the role or setting if elastric search
        $params = array_merge_recursive(
            $this->getRequestBody($builder->model),
            ['body' => [
                "query" => [
                    "multi_match" => [
                        "query" => $builder->query??'',//its have what we are looking for
                        "fields" => $this->getFields($builder->model),
                        "type" => "phrase_prefix",
                    ]
                ]
            ]
            ],
            $options);
        return $this->clientBuilder->search($params);
    }

        public function paginate(Builder $builder, $perPage, $page)
    {
       return $this->performSearch($builder,[
           'from'=>($page - 1) * $perPage,
           'size'=>10,
       ]);
    }

    public function mapIds($results)
    {
      return Collect(Arr::get($results,'hits.hits'))->pluck('_id')->values();
    }

    public function map(Builder $builder, $results, $model)
    {
       // return $results; // return result from search function above
       $hits = Arr::get($results,'hits.hits');
      // dd(Collect($hits)->pluck('_id')->values()->all());
      //this for back data to collection as model from elastic
      return $model->getScoutModelsByIds($builder,Collect($hits)->pluck('_id')->values()->all());
    }

    public function lazyMap(Builder $builder, $results, $model)
    {
        // TODO: Implement lazyMap() method.
    }

    public function getTotalCount($results)
    {
        return Arr::get($results,'hits.total',0)['value'];
    }

    public function flush($model)
    {
      $this->clientBuilder->indices()->delete([
          'index'=> $model->searchableAs()
      ]);
    }

    public function createIndex($name, array $options = [])
    {
        // TODO: Implement createIndex() method.
        $this->clientBuilder->indices()->create();
    }

    public function deleteIndex($name)
    {
        // TODO: Implement deleteIndex() method.
    }
    private function getRequestBody($model, $options=[]): array
    {
        return array_merge_recursive([
            'index' => $model->searchableAs(),
            'type'  => $model->searchableAs(),
        ],$options);
    }

    /**
     * @param $model
     * @return array
     */
    protected function getFields($model): array
    {
        if (!method_exists($model,'searchInFileds'))return [];
         return $model->searchInFileds();
    }
}
