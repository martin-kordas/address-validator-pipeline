<?php

namespace nastaveni_udaje_realna_adresa\process;

use nastaveni_udaje_realna_adresa\utils\Signal;
use nastaveni_udaje_realna_adresa\model\AddressDataProvider;
use nastaveni_udaje_realna_adresa\model\AddressEntities;
use nastaveni_udaje_realna_adresa\model\AddressEntity;
use nastaveni_udaje_realna_adresa\model\AddressEntityValue;
use nastaveni_udaje_realna_adresa\model\AddressValidator;

use Throwable, Exception;
use ZMQ;
use React\Stream\{
  ReadableStreamInterface,
  WritableStreamInterface,
  ThroughStream
};
use React\Promise\{
  Deferred,
  PromiseInterface
};
use React\MySQL\QueryResult;
use React\Cache\ArrayCache;
use React\ZMQ\Context;
use ws\lib\commons\interfaces\LoggerInterface;
use ws\lib\utils\ReactLoop;


abstract class ZMQProcess {
  
  protected $loop;
  protected $logger;
  protected $context;
  protected $addressEntities;
  
  abstract public function run() : PromiseInterface;
  abstract protected function initSockets() : void;
  
  public function __construct(AddressEntities $addressEntities, LoggerInterface $logger) {
    $this->loop             = ReactLoop::getInstance();
    $this->context          = new Context($this->loop->getLoop());
    $this->addressEntities  = $addressEntities;
    $this->logger           = $logger;
    $this->initSockets();
  }
  
  protected function getDataProvider(int $idOrganizace) {
    return AddressDataProvider::getDataProvider($idOrganizace, $this->addressEntities);
  }
  
  protected function resolveDsn(int $port) {
    return sprintf("tcp://127.0.0.1:%s", $port);
  }
  
  protected function listenToController() : void {
    if ($this instanceof Worker || $this instanceof Sink) {
      $this->controller->on("message", function ($data) {
        $object = unserialize($data);
        if ($object instanceof Signal) {
          if ($object->signal === Signal::SIGNAL_KILL) {
            $this->receiver->end();
            $this->controller->end();
            $this->logger->log("Přijat SIGNAL_KILL.");
          }
        }
      });
    }
  }
  
}

class Ventilator extends ZMQProcess {
  
  const
    PORT            = 5960,
    PORT_CONTROLLER = 5961,
    END_TIMEOUT     = 2,
    KILL_TIMEOUT    = 30;
  
  protected $sender;
  protected $controller;
  
  private $organizace;
  private $poleidOrganizace;
  
  public function __construct(AddressEntities $addressEntities, LoggerInterface $logger, array $poleidOrganizace) {
    $this->organizace = vrat_organizace($poleidOrganizace);
    $this->poleidOrganizace = array_keys($this->organizace);
    parent::__construct($addressEntities, $logger);
  }
  
  protected function initSockets() : void {
    $dsn = $this->resolveDsn(static::PORT);
    $this->sender = $this->context->getSocket(ZMQ::SOCKET_PUSH);
    $this->sender->bind($dsn);
    $this->logger->logVerbose("Proveden bind na PUSH socket.");
    
    $dsn = $this->resolveDsn(static::PORT_CONTROLLER);
    $this->controller = $this->context->getSocket(ZMQ::SOCKET_PUB);
    $this->controller->bind($dsn);
    $this->logger->logVerbose("Proveden bind na PUB (controller) socket.");
  }
  
  public function run() : PromiseInterface {
    $addressCount     = 0;
    $initialValue     = true;
    $error            = null;
    $lastidOrganizace = null;
    
    $promise
      = \React\Promise\reduce(
        $this->organizace,
        // $success = akumulovaná hodnota (carry), $organizace = prvek procházeného pole (item)
        function ($success, array $organizace) use (&$lastidOrganizace, &$addressCount, &$error) {
          if ($success) {
            if ($this->loop->isStopped()) {
              if (isset($lastidOrganizace))
                $this->signalEndOfOrganisation($lastidOrganizace);
              throw new Exception("Proces byl zvnějšku zastaven.");
            }
            
            $idOrganizace = (int)$organizace["id_organizace"];
            $verze = $organizace["verze"];
            $this->logger->log("Začíná zpracování organizace $idOrganizace.");

            // vrácený příslib určuje, zda pokračovat dál (pravdivostní hodnota bude v další iteraci předána v $success)
            $promise = $this->processOrganisation($idOrganizace, $verze, $addressCount)
              ->then(function ($addressCountOrganisation) use ($idOrganizace) {
                $this->logger->log("Pro organizaci $idOrganizace bylo načteno $addressCountOrganisation adres.");
                return true;
              })
              ->otherwise(function (Throwable $ex) use (&$error) {
                $error = $ex->getMessage();
                return false;
              });
            $lastidOrganizace = $idOrganizace;
          }
          else $promise = \React\Promise\resolve(false);
          
          return $promise;
        },
        $initialValue
      )
      ->then(function ($success) use (&$addressCount, &$error) {
        if ($success) return "Celkem bylo načteno $addressCount adres.";
        else throw new Exception($error);
      });
      
    $promise->always(function () {
      $this->stop();
    });
    return $promise;
  }
  
  public function stop() : void {
    // nastavené zpoždění zabrání tomu, aby KILL signál byl doručen dříve, než Sink stihne zpracovat data
    $this->loop->getLoop()->addTimer(static::KILL_TIMEOUT, function () {
      $signal = new Signal(Signal::SIGNAL_KILL);
      $this->controller->send(serialize($signal));
      $this->logger->log("Zaslán SIGNAL_KILL.");
    });
  }
  
  private function processOrganisation(int $idOrganizace, string $verze, int &$addressCount) : PromiseInterface {
    $deferred                 = new Deferred;
    $dataProvider             = $this->getDataProvider($idOrganizace);
    $addressCountOrganisation = 0;
    
    $dataProvider->getData()
      ->then(function (ReadableStreamInterface $data) use (
        $idOrganizace, $verze, $deferred, &$addressCount, &$addressCountOrganisation
      ) {
        $dataStream = $data->pipe($this->getToAddressEntityValueStream($idOrganizace, $verze));

        $dataStream->on("data", function (AddressEntityValue $addressEntityValue) use (&$addressCount, &$addressCountOrganisation) {
          $this->sender->send(serialize($addressEntityValue));
          $this->logger->logVerbose(sprintf("načtena adresa a přeposlána do Worker: %s", $addressEntityValue));
          $addressCount++;
          $addressCountOrganisation++;
        });
        // událost "error" nefunguje, při chybě SQL dotazu tedy promise zůstane nerozřešen
        $dataStream->on("error", function (Throwable $ex) use ($deferred) {
          $deferred->reject($ex);
        });
        $dataStream->on("end", function () use ($deferred, $idOrganizace, &$addressCountOrganisation) {
          $this->signalEndOfOrganisation($idOrganizace);
          $deferred->resolve($addressCountOrganisation);
        });
      })
      ->otherwise(function (Throwable $ex) use ($deferred) {
        $deferred->reject($ex);
      });
    
    return $deferred->promise();
  }
  
  private function signalEndOfOrganisation(int $idOrganizace) : void {
    $loop = $this->loop->getLoop();
    $loop->addTimer(static::END_TIMEOUT, function () use ($idOrganizace) {
      /** @var AddressEntity $addressEntity */
      foreach ($this->addressEntities as $addressEntity) {
        $signal = new Signal(Signal::SIGNAL_END, [
          "addressEntityName" => $addressEntity->name,
          "idOrganizace"      => $idOrganizace,
        ]);
        $this->sender->send(serialize($signal));
        $this->logger->log("Zaslán SIGNAL_END pro organizaci $idOrganizace pro Sink[{$addressEntity->name}]");
      }
    });
  }
  
  private function getToAddressEntityValueStream(int $idOrganizace, string $verze) : WritableStreamInterface {
    return new ThroughStream(function ($row) use ($idOrganizace, $verze) {
      $address = array_intersect_key($row, array_flip(AddressDataProvider::ADDRESS_FIELDS));
      return new AddressEntityValue($address, $row["identifikator_zaznamu"], $row["name"], $idOrganizace, $verze);
    });
  }
  
}


class Worker extends ZMQProcess {
  
  protected $receiver;
  protected $senders;
  protected $controller;
  
  private $validator;
  
  public function __construct(AddressEntities $addressEntities, LoggerInterface $logger) {
    $this->validator = new AddressValidator(new ArrayCache, $logger);
    parent::__construct($addressEntities, $logger);
  }
  
  protected function initSockets() : void {
    $dsn = $this->resolveDsn(Ventilator::PORT);
    $this->receiver = $this->context->getSocket(ZMQ::SOCKET_PULL);
    $this->receiver->connect($dsn);
    $this->logger->logVerbose("Proveden connect na PULL socket (Ventilator).");
    
    /** @var AddressEntity $addressEntity */
    foreach ($this->addressEntities as $addressEntity) {
      $dsn = $this->resolveDsn($addressEntity->sinkPort);
      $sender = $this->context->getSocket(ZMQ::SOCKET_PUSH);
      $sender->connect($dsn);
      $this->logger->logVerbose("Proveden connect na PUSH socket (Sink).");
      $this->senders[$addressEntity->name] = $sender;
    }
    
    $dsn = $this->resolveDsn(Ventilator::PORT_CONTROLLER);
    $this->controller = $this->context->getSocket(ZMQ::SOCKET_SUB);
    $this->controller->connect($dsn);
    $this->controller->setSockOpt(ZMQ::SOCKOPT_SUBSCRIBE, "");
    $this->logger->logVerbose("Proveden connect na SUB (controller) socket (Ventilator).");
    
    $this->listenToController();
  }
  
  public function run() : PromiseInterface {
    $deferred             = new Deferred;
    $addressCount         = 0;
    $validationPromises   = [];
    
    $this->receiver->on("message", function ($data) use (&$addressCount, &$validationPromises) {
      $object = unserialize($data);
      if ($object instanceof Signal) {
        $addressEntityName = $object->data["addressEntityName"];
        $sender = $this->senders[$addressEntityName];
        $sender->send(serialize($object));
        
        if ($object->signal === Signal::SIGNAL_END)
          $this->logger->log("Přeposlán SIGNAL_END pro organizaci {$object->data["idOrganizace"]} do Sink[$addressEntityName]");
      }
      elseif ($object instanceof AddressEntityValue) {
        $addressEntityValue = $object;
        $promise 
          = $this->validateAddress($addressEntityValue)
          ->then(function ($isValid) use ($addressEntityValue, &$addressCount) {
            $addressEntityName = $addressEntityValue->getEntityName();
            $addressEntityValue->setIsValid($isValid);

            $sender = $this->senders[$addressEntityName];
            $sender->send(serialize($addressEntityValue));
            $addressCount++;
            
            $boolStr = $isValid ? "TRUE" : "FALSE";
            $this->logger->logVerbose(
              "Načtena adresa $addressEntityValue ($addressEntityName), validována jako $boolStr, přeposlána do Sink[$addressEntityName]"
            );
          });

        $validationPromises[] = $promise;
      }
      else $this->logger->logError("Data přijímaná z Ventilatoru jsou v chybném tvaru.");
    });
    $this->receiver->on("error", function (Throwable $ex) use ($deferred) {
      $deferred->reject($ex);
    });
    $this->receiver->on("end", function () use ($deferred, &$addressCount, &$validationPromises) {
      \React\Promise\all($validationPromises)
        ->then(function () use ($deferred, &$addressCount) {
          $deferred->resolve("Celkem bylo validováno $addressCount adres.");
        })
        ->otherwise(function (Throwable $ex) use ($deferred) {
          $deferred->reject($ex);
        });
    });
    return $deferred->promise();
  }
  
  private function validateAddress(AddressEntityValue $addressEntityValue) : PromiseInterface {
    $address = $addressEntityValue->getAddress();
    $verze = $addressEntityValue->getVerze();
    return $this->validator->validate($address, $verze);
  }
  
}


class Sink extends ZMQProcess {
  
  const
    PORTS                     = [
      "e1"                    => 5970,
      "e2"                    => 5971,
      "e3"                    => 5972,
    ];
  
  protected $receiver;
  protected $controller;
  
  private $addressEntity;
  private $addressEntityValues = [];
  
  public function __construct(AddressEntities $addressEntities, LoggerInterface $logger, AddressEntity $addressEntity) {
    $this->addressEntity = $addressEntity;
    parent::__construct($addressEntities, $logger);
  }
  
  protected function initSockets() : void {
    $dsn = $this->resolveDsn($this->addressEntity->sinkPort);
    $this->receiver = $this->context->getSocket(ZMQ::SOCKET_PULL);
    $this->receiver->bind($dsn);
    $this->logger->logVerbose("Proveden bind na PULL socket.");
    
    $dsn = $this->resolveDsn(Ventilator::PORT_CONTROLLER);
    $this->controller = $this->context->getSocket(ZMQ::SOCKET_SUB);
    $this->controller->connect($dsn);
    $this->controller->setSockOpt(ZMQ::SOCKOPT_SUBSCRIBE, "");
    $this->logger->logVerbose("Proveden connect na SUB (controller) socket (Ventilator).");
    
    $this->listenToController();
  }
  
  public function run() : PromiseInterface {
    $deferred             = new Deferred;
    $savePromises         = [];
    
    $this->receiver->on("message", function ($data) use ($deferred, &$savePromises) {
      $object = unserialize($data);
      if ($object instanceof Signal) {
        $signal = $object;
        if ($signal->signal === Signal::SIGNAL_END) {
          $idOrganizace = $signal->data["idOrganizace"];
          $addressEntityValues = $this->addressEntityValues[$idOrganizace] ?? [];
          
          if (!empty($addressEntityValues)) {
            $dataProvider = $this->getDataProvider($idOrganizace);
            $promise = $dataProvider->saveData($this->addressEntity, $addressEntityValues);
            $savePromises[] = $promise;
            
            $promise->then(function (QueryResult $result) use ($idOrganizace) {
              $this->logger->log("Bylo uloženo {$result->affectedRows} adres pro organizaci $idOrganizace.");
            });
          }
          $this->logger->log("Přijat SIGNAL_END pro organizaci $idOrganizace");
        }
        else $deferred->reject(new Exception("Neznámý signál."));
      }
      elseif ($object instanceof AddressEntityValue) {
        $addressEntityValue = $object;
        $idOrganizace = $addressEntityValue->getidOrganizace();
        if (!isset($this->addressEntityValues[$idOrganizace])) $this->addressEntityValues[$idOrganizace] = [];
        $this->addressEntityValues[$idOrganizace][] = $addressEntityValue;
        $this->logger->logVerbose("Načtena adresa organizace $idOrganizace: $addressEntityValue");
      }
      else $this->logger->logError("Data přijímaná z Workeru jsou v chybném tvaru.");
    });
    $this->receiver->on("error", function (Throwable $ex) use ($deferred) {
      $deferred->reject($ex);
    });
    $this->receiver->on("end", function () use ($deferred, &$savePromises) {
      \React\Promise\all($savePromises)
        ->then(function ($all) use ($deferred) {
          $addressCount = array_reduce($all, function ($sum, QueryResult $result) {
            return $sum + $result->affectedRows;
          }, 0);
          $deferred->resolve("Celkem bylo uloženo $addressCount adres.");
        })
        ->otherwise(function (Throwable $ex) use ($deferred) {
          $deferred->reject($ex);
        });
    });
    return $deferred->promise();
  }
  
}
