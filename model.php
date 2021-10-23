<?php

namespace nastaveni_udaje_realna_adresa\model;

use nastaveni_udaje_realna_adresa\process\Sink;
use ws\lib\commons\interfaces\LoggerInterface;

use InvalidArgumentException, BadMethodCallException, RuntimeException;
use SplObjectStorage, Serializable;
use React\Promise\PromiseInterface;
use React\Cache\CacheInterface;
use React\MySQL\ConnectionInterface;
use React\MySQL\QueryResult;
use ws\lib\utils\DBReactUtils;
use ws\lib\commons\interfaces\Arrayable;


class AddressDataProvider {
  
  const
    LIMIT           = null,
    ADDRESS_FIELDS  = ["ulice", "cislo_popisne", "cislo_orientacni", "obec", "psc"];
  
  /** @var ConnectionInterface */
  private $connection;
  /** @var AddressEntities */
  private $addressEntities;
  
  private $idOrganizace;
  private $database;
  
  // pro urychlení testování vyřadí záznamy s nevyplněnou adresou
  private $onlyNonEmpty = false;
  // vyřadí záznamy, kde již je údaj realna_adresa nastaven (tedy nebude provádět aktualizaci již nastavených údajů realna_adresa, ale jen doplnění chybějících)
  private $onlyNotSet   = false;
  // zda spustit bez ukládání
  private $dryRun       = false;
          
  
  public function __construct(ConnectionInterface $connection, AddressEntities $addressEntities, int $idOrganizace) {
    $this->connection       = $connection;
    $this->addressEntities  = $addressEntities;
    $this->idOrganizace     = $idOrganizace;
    
    $this->database = vrat_databazi_organizace($this->idOrganizace);
    if (!$this->database) throw new InvalidArgumentException();
  }
  
  public function getData() : PromiseInterface {
    // databázi nelze vybírat pomocí USE, protože není garantováno, že některý z dalších asynchronních dotazů databázi nepřepne
    //    - db. proto v SQL dotazech uvádíme doslovně
    return
      //$this->connection->query("USE `{$this->database}`")
      \React\Promise\resolve()
      ->then(function () {
        list($query, $params) = $this->getSelectQuery();
        return $this->connection->queryStream($query, $params);
      });
  }
  
  public function saveData(AddressEntity $addressEntity, array $addressEntityValues) : PromiseInterface {
    return
      //$this->connection->query("USE `{$this->database}`")
      \React\Promise\resolve()
      ->then(function () use ($addressEntity, $addressEntityValues) {
        list($query, $params) = $this->getUpdateQuery($addressEntity, $addressEntityValues);
        if ($this->dryRun) {
          $result = new QueryResult();
          $result->affectedRows = count($addressEntityValues);
          return \React\Promise\resolve($result);
        }
        else return $this->connection->query($query, $params);
      });
  }
  
  private function getUpdateQuery(AddressEntity $addressEntity, array $addressEntityValues) : array {
    $whenArray = [];
    $identifikatorZaznamuArray = [];
    
    /** @var AddressEntityValue $addressEntityValue */
    foreach ($addressEntityValues as $addressEntityValue) {
      $realnaAdresaSql = vrat_realna_adresa_sql($addressEntityValue->getIsValid(), false);
      $identifikatorZaznamu = $addressEntityValue->getidentifikatorZaznamu();
      $whenArray[] = "WHEN $identifikatorZaznamu THEN $realnaAdresaSql";
      $identifikatorZaznamuArray[] = $identifikatorZaznamu;
    }
    
    $whenStr = implode("
              ", $whenArray);
    $identifikatorZaznamuStr = implode(", ", $identifikatorZaznamuArray);
    
    $query = "
      UPDATE `{$this->database}`.`{$addressEntity->table}`
      SET `{$addressEntity->sloupecRealnaAdresa}` = (
        CASE `{$addressEntity->nazevIdentifikatoru}`
          $whenStr
          ELSE `{$addressEntity->sloupecRealnaAdresa}`
        END
      )
      WHERE `{$addressEntity->nazevIdentifikatoru}` IN ($identifikatorZaznamuStr)
        AND do IS NULL
      ORDER BY
        `{$addressEntity->nazevIdentifikatoru}`
    ";
    $params = [];
    return [$query, $params];
  }
  
  private function getSelectSubQuery(AddressEntity $addressEntity) : array {
    $name                 = $addressEntity->name;
    $table                = $addressEntity->table;
    $nazevIdentifikatoru  = $addressEntity->nazevIdentifikatoru;
    $onlyNonEmpty         = $this->onlyNonEmpty;
    $onlyNotSet           = $this->onlyNotSet;
    $limit                = static::LIMIT;
    
    if ($onlyNonEmpty) {
      $conditionsNonEmpty = implode(" OR ", array_map(function ($addressField) {
        return "($addressField IS NOT NULL AND $addressField != '')";
      }, static::ADDRESS_FIELDS));
    }
    $addressFields        = implode("\n,", array_map(function ($addressField) use ($name) {
      return "$name.$addressField";
    }, static::ADDRESS_FIELDS));
    
    $params = [];
    $subQuery = "(
      SELECT
        '$name' AS name,
        $name.$nazevIdentifikatoru AS identifikator_zaznamu,
        $addressFields
      FROM `{$this->database}`.$table AS $name
      WHERE $name.do IS NULL
        -- AND $name.ulice IN ('Mikuláškova', 'Močiarna')
        -- AND $name.$nazevIdentifikatoru = 3788".($onlyNotSet ? "
        AND $name.realna_adresa IS NULL" : "").($onlyNonEmpty ? "
        AND ($conditionsNonEmpty)" : "").($limit !== null ? "
      LIMIT ?" : "")."
    )";
    if ($limit !== null) $params[] = $limit;
    return [$subQuery, $params];
  }
  
  private function getSelectQuery() : array {
    $subQueries = array_map(function (AddressEntity $addressEntity) {
      return $this->getSelectSubQuery($addressEntity);
    }, $this->addressEntities->toArray());
    
    $subQueriesUnion = implode("
      UNION ALL
    ", array_column($subQueries, 0));
    $paramsUnion = array_merge(array_column($subQueries, 1));
    
    $names = $this->addressEntities->getEntitiesNames();
    $namesSql = implode(", ", array_map(function ($name) {
      return "'$name'";
    }, $names));
    
    $query = "
      $subQueriesUnion
      ORDER BY
        FIELD(name, $namesSql),
        identifikator_zaznamu
    ";
    return [$query, $paramsUnion];
  }
  
  public static function getDataProvider(int $idOrganizace, AddressEntities $addressEntities) : AddressDataProvider {
    $connection = DBReactUtils::getInstance()->getConnection();
    $dataProvider = new self($connection, $addressEntities, $idOrganizace);
    return $dataProvider;
  }
  
}


class AddressEntities extends SplObjectStorage implements Arrayable {
  
  private $addressEntitiesNames;
  
  public function __construct($names = null) {
    /** @var AddressEntity $addressEntity */
    foreach ($this->getAllEntities() as $addressEntity) {
      if ($names === null || in_array($addressEntity->name, $names))
        $this->attach($addressEntity);
    }
  }
  
  public function getEntityByName(string $name) : AddressEntity {
    /** @var AddressEntity $addressEntity */
    foreach ($this as $addressEntity) {
      if ($addressEntity->name === $name)
        return $addressEntity;
    }
    throw new BadMethodCallException();
  }
  
  public function toArray() {
    $pole = [];
    foreach ($this as $object) $pole[] = $object;
    return $pole;
  }
  
  public function getEntitiesNames() : array {
    if (!isset($this->addressEntitiesNames)) {
      $this->addressEntitiesNames = array_map(function (AddressEntity $addressEntity) {
        return $addressEntity->name;
      }, $this->toArray());
    }
    return $this->addressEntitiesNames;
  }
  
  private function getAllEntities() : array {
    $ports = Sink::PORTS;
    $addressEntititesArray = [
      new AddressEntity("e1", "entity1", "id_entity", "realna_adresa",                $ports["e1"]),
      new AddressEntity("e2", "entity2", "id_entity", "realna_adresa",                $ports["e2"]),
      new AddressEntity("e3", "entity3", "id_entity", "korespondencni_realna_adresa", $ports["e3"]),
    ];
    return $addressEntititesArray;
  }
  
}


class AddressEntity {
  
  public $name;
  public $table;
  public $nazevIdentifikatoru;
  public $sloupecRealnaAdresa;
  public $sinkPort;
  
  public function __construct(string $name, string $table, string $nazevIdentifikatoru, string $sloupecRealnaAdresa, int $sinkPort) {
    $this->name                 = $name;
    $this->table                = $table;
    $this->nazevIdentifikatoru  = $nazevIdentifikatoru;
    $this->sloupecRealnaAdresa  = $sloupecRealnaAdresa;
    $this->sinkPort             = $sinkPort;
  }
  
}


class AddressEntityValue implements Serializable {
  
  private $address;
  private $identifikatorZaznamu;
  private $entityName;
  private $idOrganizace;
  private $verze;
  private $isValid;
  
  public function __construct(array $address, int $identifikatorZaznamu, string $entityName, int $idOrganizace, string $verze) {
    $this->address                = $address;
    $this->identifikatorZaznamu   = $identifikatorZaznamu;
    $this->entityName             = $entityName;
    $this->idOrganizace           = $idOrganizace;
    $this->verze                  = $verze;
  }
  
  public function __toString() {
    $string = format_adresa_pole($this->address, "", false);
    if (!$string) $string = "(nevyplněno)";
    return $string;
  }
  
  public function serialize() : string {
    $data = [
      "address"               => $this->address,
      "identifikatorZaznamu"  => $this->identifikatorZaznamu,
      "entityName"            => $this->entityName,
      "idOrganizace"          => $this->idOrganizace,
      "verze"                 => $this->verze,
      "isValid"               => $this->isValid,
    ];
    $str = json_encode($data);
    return $str;
  }
  
  public function unserialize($serialized) {
    $data = json_decode($serialized, true);
    if ($data === null) throw new RuntimeException();
    $this->address              = $data["address"];
    $this->identifikatorZaznamu = $data["identifikatorZaznamu"];
    $this->entityName           = $data["entityName"];
    $this->idOrganizace              = $data["idOrganizace"];
    $this->verze                = $data["verze"];
    $this->isValid              = $data["isValid"];
  }
  
  public function getAddressEntity(AddressEntities $addressEntities) : AddressEntity {
    return $addressEntities->getEntityByName($this->entityName);
  }
  
  public function setIsValid(bool $isValid) {
    $this->isValid = $isValid;
  }
  
  public function getAddress()              { return $this->address; }
  public function getidentifikatorZaznamu() { return $this->identifikatorZaznamu; }
  public function getEntityName()           { return $this->entityName; }
  public function getidOrganizace()         { return $this->idOrganizace; }
  public function getVerze()                { return $this->verze; }
  public function getIsValid()              { return $this->isValid; }
  
}


class AddressValidator {
  
  private $cache;
  private $logger;
  private $verboseLogger = false;
  
  public function __construct(CacheInterface $cache, LoggerInterface $logger) {
    $this->cache = $cache;
    $this->logger = $logger;
  }
  
  public function validate(array $address, string $verze) : PromiseInterface {
    $key = $this->address2key($address, $verze);
    return $this->cache
      ->get($key)
      ->then(function ($isValid) use ($address, $key, $verze) {
        $addressStr = format_adresa_pole($address, "", false);
        if ($isValid === null) {
          // volání obsahuje synchronní curl požadavek na validaci přes externí server
          //  - vhodnější by bylo zasílat asynchronní požadavek např. pomocí react/http
          $isValid = je_postovni_adresa_realna($address, true, $verze);
          //$isValid = false;
          $this->cache->set($key, $isValid);
          
          if ($this->verboseLogger)
            $this->logger->logVerbose(sprintf("cache miss: %s", $addressStr));
        }
        else {
          if ($this->verboseLogger)
            $this->logger->logVerbose(sprintf("cache hit: %s", $addressStr));
        }
        return $isValid;
      });
  }
  
  private function address2key(array $address, string $verze) {
    return json_encode($address + ["verze" => $verze]);
  }
  
}
