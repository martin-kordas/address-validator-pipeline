<?php

namespace nastaveni_udaje_realna_adresa\runner;

use nastaveni_udaje_realna_adresa\process\Ventilator;
use nastaveni_udaje_realna_adresa\process\Worker;
use nastaveni_udaje_realna_adresa\process\Sink;
use nastaveni_udaje_realna_adresa\model\AddressEntities;
use nastaveni_udaje_realna_adresa\model\AddressEntity;
use nastaveni_udaje_realna_adresa\utils\StdIO;
use nastaveni_udaje_realna_adresa\utils\ZMQStdIOLogger;

use Throwable, Exception, InvalidArgumentException;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;
use React\ChildProcess\Process;
use ws\lib\utils\ReactLoop;


class Runner {
  
  const
    ROLE = [
      "ventilator"                => Ventilator::class,
      "worker"                    => Worker::class,
      "sink"                      => Sink::class,
    ],
    RUN_PROCESS_TIMEOUT           = 2;
          
  
  /** @var StdIO */
  private $stdio;
  /** @var ZMQStdIoLogger */
  private $logger;
  /** @var ReactLoop */
  private $loop;
  /** @var ZMQProcess */
  private $zmqProcess;
  /** @var AddressEntities */
  private $addressEntities;
  /** @var AddressEntity */
  private $addressEntity        = null;
  
  private $role                 = "ventilator";
  private $help                 = false;
  private $poleIdOrganizace     = [];
  private $workerCount          = 4;
  
  private $workerProcesses      = [];
  private $sinkProcesses        = [];
  
  
  public function run() {
    $this->init();
    $loop = $this->loop->getLoop();
    
    // kód umístěn do ::futureTick(), aby se začal provádět až po spuštění event loop
    $loop->futureTick(function () {
      $this->runAsync()
        ->otherwise(function (Throwable $ex) {
          $message = $ex->getMessage();
          if (isset($this->logger)) $this->logger->logError($message);
          else $this->stdio->stderr($message);
        })
        ->always(function () {
          // bez odebrání signálu event loop stále naslouchá dalšímu signálu a program se neukončí
          $this->loop->removeInterruptHandler();
        });
    });

    // event loop musíme spustit vždy, jinak by ani neprovedl výpis pomocí $this->stdio
    $this->loop->runLoop();
  }
  
  private function runAsync() : PromiseInterface {
    $loop = $this->loop->getLoop();
    $deferred = new Deferred;
    
    try {
      $this->parseInput();
      
      if ($this->help) {
        $this->showHelp();
        $deferred->resolve();
      }
      else {
        $this->logger = $this->getLogger();
        $this->logger->log("Spuštění procesu.");
        
        $timeout = 0;
        if ($this->role === "ventilator") {
          $this->runWorkerProcesses();
          $this->runSinkProcesses();
          $timeout = static::RUN_PROCESS_TIMEOUT;
        }

        // nastavené zpoždění řeší "slow joiner problem" - při spuštění ventilátoru již musí běžet všechny podprocesy
        $loop->addTimer($timeout, function () use ($deferred) {
          $this->runZMQ($deferred);
        });
      }
    }
    catch (Throwable $ex) {
      if ($ex instanceof InvalidArgumentException)
        $this->showHelp();
      $deferred->reject($ex);
    }
    finally {
      return $deferred->promise();
    }
  }
  
  private function runZMQ(Deferred $deferred) : void {
    try {
      $roleClass = static::ROLE[$this->role];
      switch ($this->role) {
        case "ventilator" : $args = [$this->addressEntities, $this->logger, $this->poleIdOrganizace]; break;
        case "worker"     : $args = [$this->addressEntities, $this->logger];                          break;
        case "sink"       : $args = [$this->addressEntities, $this->logger, $this->addressEntity];    break;
      }
      $this->zmqProcess = new $roleClass(...$args);
      
      $promise = $this->zmqProcess->run()
        ->then(function ($message) {
          $message1 = "Operace byly úspěšně dokončeny: ";
          $message1 .= $message;
          $this->logger->logInfo($message1);
        })
        ->otherwise(function (Throwable $ex) {
          $message = "Operace se nepodařilo dokončit: ";
          $message .= $ex->getMessage();
          $this->logger->logError($message);
        });
        
      $deferred->resolve($promise);
    }
    catch (Throwable $ex) {
      $deferred->reject($ex);
    }
  }
  
  private function init() {
    $this->stdio = StdIO::getInstance();
    $this->loop = ReactLoop::getInstance();
    $this->loop->handleInterrupt();
    $this->addressEntities = new AddressEntities;
  }
  
  private function getLogger() {
    $role = $this->role;
    if ($this->role === "sink") $role .= " [{$this->addressEntity->name}]";
    return new ZMQStdIOLogger($this->stdio, $role, getmypid());
  }
  
  private function showHelp() : void {
    $program = "php ".__FILE__;
    $help = <<<EOD
Základní použití: $program --seznam-id-organizace=1,2,4
Další použití:    $program --role=ventilator --seznam-id-organizace=1,2,4 [--worker-count=4] 
                  $program --role=worker
                  $program --role=sink --address-entity={ouz|zm|...}
                  $program --help
EOD;
    $this->stdio->stderr($help);
  }
  
  private function parseInput() : bool {
    $options = getopt("", [
      "role:",
      "seznam-id-organizace:",
      "worker-count:",
      "address-entity:",
      "help",
    ]);

    if (isset($options["help"]))
      $this->help = true;
    else {
      if (isset($options["role"])) {
        if (!in_array($options["role"], array_keys(static::ROLE)))
          throw new InvalidArgumentException("Neplatný parametr role.");
        else $this->role = $options["role"];
      }

      switch ($this->role) {
        case "ventilator":
          if (isset($options["seznam-id-organizace"]))
            $this->poleIdOrganizace = vyfiltruj_seznam_id($options["seznam-id-organizace"], ",", true);
          if (empty($this->poleIdOrganizace))
            throw new InvalidArgumentException("Nebyla zadána žádná platná ID organizace.");

          if (isset($options["worker-count"]) && is_numeric($options["worker-count"]))
            $this->workerCount = (int)$options["worker-count"];
          break;

        case "sink":
          if (!isset($options["address-entity"]))
            throw new InvalidArgumentException("Pro roli sink je nutné zadat parametr address-entity.");
          else {
            $addressEntitiesNames = $this->addressEntities->getEntitiesNames();
            if (!in_array($options["address-entity"], $addressEntitiesNames))
              throw new InvalidArgumentException("Neplatný parametr address-entity.");
            $this->addressEntity = $this->addressEntities->getEntityByName($options["address-entity"]);
          }
          break;
      }
    }
    
    return true;
  }
  
  private function runWorkerProcesses() : void {
    for ($i = 0; $i < $this->workerCount; $i++) {
      $cmd = $this->prepareCmd(["--role=worker"]);
      $pid = -1;
      $workerProcess = $this->createProcess($cmd, $pid);
      $this->workerProcesses[$pid] = $workerProcess;
    }
  }
  
  private function runSinkProcesses() : void {
    /** @var AddressEntity $addressEntity */
    foreach ($this->addressEntities as $addressEntity) {
      $addressEntityName = $addressEntity->name;
      $cmd = $this->prepareCmd(["--role=sink", "--address-entity=$addressEntityName"]);
      $pid = -1;
      $sinkProcess = $this->createProcess($cmd, $pid);
      $this->sinkProcesses[$pid] = $sinkProcess;
    }
  }
  
  private function createProcess(string $cmd, int &$pid) : Process {
    $workerProcess = new Process($cmd);
    $workerProcess->start($this->loop->getLoop());
    // pokud proces nebyl volán přes exec, $pid je PID shellu, ne samotného procesu
    $pid = $workerProcess->getPid();
    
    $workerProcess->stdout->on("data", function ($data) {
      // nepoužíváme $this->logger, zpráva musí zůstat tak, jak byla zaslána procesem
      $this->stdio->stdout($data);
    });
    $workerProcess->stderr->on("data", function ($data) {
      $this->stdio->stderr($data);
    });
    $workerProcess->on("exit", function ($code, $term) use ($pid) {
      if ($term === null) $this->stdio->stdout("Podproces $pid skončil s kódem $code.");
      else $this->stdio->stdout("Podproces $pid byl ukončen signálem $term.");
    });
    
    return $workerProcess;
  }
  
  private function prepareCmd(array $args) : string {
    $program = ["exec", "php", $_SERVER["SCRIPT_FILENAME"]];
    $argsAll = array_merge($program, $args);
    $cmd = implode(" ", array_map("escapeshellarg", $argsAll));
    return $cmd;
  }
   
}
