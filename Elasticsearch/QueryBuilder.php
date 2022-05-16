<?php


namespace App\Services\Elasticsearch;

use App\Libraries\Phonetics;
use App\Models\GoogleTranslation;
use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use Elasticsearch\Common\Exceptions\Missing404Exception;
use Illuminate\Support\Collection;
use Illuminate\Support\Str;

abstract class QueryBuilder
{
    /**
     * @var Client
     */
    protected $client;

    /**
     * @var string
     */
    protected $index;

    /**
     * @var int
     */
    protected $offset = 0;

    /**
     * @var int
     */
    protected $limit = 50;

    /**
     * @var array
     */
    protected $filters = [];

    /**
     * @var array
     */
    protected $sorts = [];

    /**
     * @var array
     */
    protected $queryParams = [];

    /**
     * Elastic constructor.
     */
    public function __construct()
    {
        $this->client = ClientBuilder::create()
                                     ->setHosts([config('elasticsearch.host')])
                                     ->build();
    }

    /**
     * @param string $key
     * @param string $value
     * @param float $boost
     * @param bool $mandatory
     * @param bool $exact
     * @return $this
     */
    public function where(string $key, string $value, float $boost = 1.0, bool $mandatory = false, bool $exact = false)
    {
        $constraint = $mandatory ? 'must' : 'should';
        $match = $exact ? 'match_phrase' : 'match';
        $this->queryParams[$constraint][] = [$match => $this->getMatchParams($key, $value, $boost)];

        return $this;
    }

    /**
     * @param string $index
     * @return $this
     */
    public function setIndex(string $index)
    {
        $this->index = $index;

        return $this;
    }

    /**
     * @param string $constraint
     * @param string $order
     * @return $this
     */
    public function setSort(string $constraint, string $order = 'asc')
    {
        $this->sorts[] = [$constraint => ['order' => $order]];

        return $this;
    }

    /**
     * @param string $filter
     * @param string $value
     * @param float $boost
     * @param bool $mandatory
     * @param bool $exact
     * @return $this
     */
    public function setFilter(string $filter, string $value, float $boost = 1.0, bool $mandatory = false, bool $exact = false)
    {
        $constraint = $mandatory ? 'must' : 'should';
        $match = $exact ? 'match_phrase' : 'match';
        $this->filters[$constraint][] = [$match => $this->getMatchParams($filter, $value, $boost)];

        return $this;
    }

    /**
     * @param int $offset
     * @return $this
     */
    public function setOffset(int $offset)
    {
        $this->offset = $offset;

        return $this;
    }

    /**
     * @param int $limit
     * @return $this
     */
    public function setLimit(int $limit)
    {
        $this->limit = $limit;

        return $this;
    }

    /**
     * @param string $key
     * @param int $from
     * @param int $to
     * @return $this
     */
    public function setBetween(string $key, int $from, int $to)
    {
        $this->queryParams['must'][] = [
            'range' => [$key => ['gte' => $from, 'lte' => $to]],
        ];

        return $this;
    }

    /**
     * @param string $key
     * @param int $from
     * @param int $to
     *
     * @return $this
     */
    public function setDateBetweenInDays(string $key, int $from, int $to = 0)
    {
        $startDate = "now-{$from}d/d";
        $endDate = $to ? "now-{$to}d/d" : 'now/d';
        $this->queryParams['must'][] = [
            'range' => [$key => ['gte' => $startDate, 'lte' => $endDate]]
        ];

        return $this;
    }

    /**
     * @return Collection
     */
    public function get(): Collection
    {
        return $this->formatResponse(
            $this->query(
                $this->buildQuery()
            )
        );
    }

    /**
     * @return array
     */
    public function getRaw(): array
    {
        return $this->query(
            $this->buildQuery()
        );
    }

    /**
     * @return mixed
     */
    public function first()
    {
        return $this->get()->first();
    }

    /**
     * @param int $id
     * @return array
     */
    public function find(int $id): ?array
    {
        try {
            $response = $this->client->get([
                'index' => $this->index,
                'id'    => $id,
            ]);

            if ($response['found']) {
                return $response['_source'];
            }
        } catch (Missing404Exception $e) {}

        return null;
    }

    /**
     * @return int
     */
    public function count(): int
    {
        return $this->getCount(
            $this->query(
                $this->buildQuery()
            )
        );
    }

    /**
     * @param array $data
     * @param int|null $id
     */
    public function create(array $data, int $id = null): void
    {
        $params = [
            'index' => $this->index,
            'body'  => $data,
        ];

        if ($id) {
            $params['id'] = $id;
        }

        $this->client->index($params);
    }

    /**
     * @param array $data
     * @param int $id
     */
    public function update(array $data, int $id): void
    {
        $this->create($data, $id);
    }

    /**
     * @param int $id
     */
    public function delete(int $id): void
    {
        $this->client->delete([
            'index' => $this->index,
            'id'    => $id,
        ]);
    }

    /**
     * @return bool
     */
    public function exists()
    {
        return $this->client->indices()
                            ->exists($this->getIndexParams());
    }

    /**
     * @return void
     */
    public function createIfNotExists(): void
    {
        if (!$this->exists()) {
            $this->createNewIndex();
        }
    }

    public function deleteIndexIfExists():QueryBuilder
    {
        if ($this->exists()) {
            $this->client->indices()
                         ->delete($this->getIndexParams());
        }
        return $this;
    }

    public function sleep():QueryBuilder{
        sleep(10);
        return $this;
    }

    /**
     * @return void
     */
    public function createNewIndex(): void
    {
        $params = $this->getIndexParams();
        $params['body'] = $this->getBodyParams();
        $this->client->indices()
                     ->create($this->getIndexParams());
    }

    /**
     * @param array $data
     * @return void
     */
    public function sendDataToElasticsearch(array $data): void
    {
        $response = $this->client->bulk([
            'index' => $this->getIndexParams(),
            'body'  => $data
        ]);
    }

    /**
     * @param array $elasticResponse
     * @return Collection
     */
    protected function formatResponse(array $elasticResponse)
    {
        $count = $this->getCount($elasticResponse);
        if ($count) {
            return collect($elasticResponse['hits']['hits'])->map(function ($hit) {
                return $hit['_source'];
            });
        }
        return collect();
    }

    /**
     * @return string[]
     */
    protected function getIndexParams(): array
    {
        return ['index' => $this->index];
    }

    /**
     * @param string $key
     * @param string $keyword
     * @param float $boost
     * @return array[]
     */
    protected function getMatchParams(string $key, string $keyword, float $boost = 1.0): array
    {
        return [$key => ['query' => $keyword, 'boost' => $boost]];
    }

    /**
     * @param array $params
     * @return array
     */
    protected function query(array $params): array
    {
        $response = $this->client->search($params);
        $this->reset();
        return $response;
    }

    /**
     * @return $this
     */
    protected function reset()
    {
        $this->offset = 0;

        $this->limit = 50;

        $this->filters = [];

        $this->sorts = [];

        $this->queryParams = [];

        return $this;
    }

    /**
     * @return array
     */
    protected function buildQuery(): array
    {
        $params = [
            'index' => $this->index,
            'from'  => $this->offset,
            'size'  => $this->limit,
            'body'  => []
        ];

        $this->buildQueryParams($params);
        $this->buildSorts($params);
        $this->buildFilters($params);

        return $params;
    }

    /**
     * @param array $params
     */
    protected function buildFilters(array &$params)
    {
        if ($this->hasFilters()) {
            $params['body']['query']['bool']['filter']['bool'] = $this->filters;
        }
    }

    /**
     * @param string $keyword
     * @return string
     */
    protected function abstractKeyword(string $keyword): string
    {
        return $this->translatedKeyword(
            $this->sanitizeKeyword($keyword)
        );
    }

    /**
     * @param string $keyword
     * @return string
     */
    protected function translatedKeyword(string $keyword): string
    {
        if (Phonetics::is_arabic($keyword)) {
            $translation = GoogleTranslation::translate('ar', 'en', $keyword);

            return $translation . ' ' . $keyword;
        }

        return $keyword;
    }

    /**
     * @param string $keyword
     * @return string
     */
    protected function sanitizeKeyword(string $keyword): string
    {
        return Str::sanitize($keyword);
    }

    /**
     * @param array $params
     */
    protected function buildSorts(array &$params)
    {
        $sort = [
            ['_score' => ['order' => 'desc']],
        ];

        if ($this->hasSorts()) {
            $sort = array_merge($sort, $this->sorts);
        }
        $params['body']['sort'] = $sort;
    }

    /**
     * @return bool
     */
    protected function hasFilters(): bool
    {
        return !empty($this->filters);
    }

    /**
     * @return bool
     */
    protected function hasSorts(): bool
    {
        return !empty($this->sorts);
    }

    /**
     * @param array $elasticResponse
     * @return int
     */
    protected function getCount(array $elasticResponse): int
    {
        return $elasticResponse['hits']['total']['value'] ?? 0;
    }

    /**
     * @param array $params
     */
    protected function buildQueryParams(array &$params)
    {
        if ($this->hasQueryParams()) {
            $params['body']['query'] = [
                'bool' => $this->queryParams,
            ];
        }
    }

    /**
     * @return bool
     */
    protected function hasQueryParams(): bool
    {
        return !empty($this->queryParams);
    }

    protected  function extractResultHits($resultHits)
    {
        $items = [];
        foreach ($resultHits as $hit) {
            $item = (object)$hit['_source'];
            $item->score = $hit['_score'];
            $items[] = $item;
        }
        return $items;
    }

    /**
     * @return array
     */
    protected function getBodyParams(): array
    {
        return [
            'settings' => [
                'number_of_shards'   => 1,
                'number_of_replicas' => 0,
                'analysis'           => [
                    'filter'   => [
                        'autocomplete_filter' => [
                            'type'     => 'edge_ngram',
                            'min_gram' => 1,
                            'max_gram' => 20,
                        ]
                    ],
                    'analyzer' => [
                        'autocomplete' => [
                            'type'      => 'custom',
                            'tokenizer' => 'standard',
                            'filter'    => [
                                'lowercase',
                                'autocomplete_filter',
                            ]
                        ]
                    ]
                ]
            ],
            'mappings' => [
                'properties' => [
                    'title' => [
                        'type'     => 'text',
                        'analyzer' => 'autocomplete',
                    ],
                ]
            ]
        ];
    }
}