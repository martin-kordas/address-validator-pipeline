<?php

namespace nastaveni_udaje_realna_adresa\utils;

use Serializable;
use React\Stream\WritableResourceStream;
use ws\lib\commons\classes\Singletone;
use ws\lib\commons\interfaces\LoggerInterface;
use ws\lib\utils\ReactLoop;


/**
 * @method static StdIO getInstance()
 */
class StdIO extends Singletone {
  
  protected static $_instance;
  protected static $_isloaded = false;
  
  private $stdout;
  private $stderr;
  
  protected function init() {
    $loop = ReactLoop::getInstance()->getLoop();
    $this->stdout = new WritableResourceStream(STDOUT, $loop);
    $this->stderr = new WritableResourceStream(STDERR, $loop);
  }
  
  public function stdout($text) {
    return $this->stdout->write($text.PHP_EOL);
  }
  
  public function stderr($text) {
    return $this->stderr->write($text.PHP_EOL);
  }
  
  public function __destruct() {
    $this->stdout->close();
    $this->stderr->close();
  }
  
}


class StdIOLogger implements LoggerInterface {
  
  private $stdio;
  private $dateFormat = "d/M/Y H:i:s";
  
  public function __construct(StdIO $stdio) {
    $this->stdio = $stdio;
  }
  
  public function log($entries, $type = "INFO", $trace = []) {
    $message = sprintf("[%s] %s", date($this->dateFormat), $entries);
    if ($type === "ERROR") return $this->stdio->stderr($message);
    else return $this->stdio->stdout($message);
  }
  
  public function logInfo($message) {
    return $this->log($message, "INFO");
  }
  
  public function logError($message) {
    return $this->log($message, "ERROR");
  }

}


class ZMQStdIOLogger extends StdIOLogger {
  
  private $role;
  private $pid;
  private $verbose = false;
  
  public function __construct(StdIO $stdio, string $role, int $pid) {
    $this->role = $role;
    $this->pid = $pid;
    parent::__construct($stdio);
  }
  
  public function log($message, $type = "INFO", $trace = []) {
    $shouldLog = $this->verbose || $type !== "VERBOSE";
    
    if ($shouldLog) {
      $message = sprintf("[%-10s:%4s] %s", $this->role, $this->pid, $message);
      return parent::log($message, $type, $trace);
    }
  }
  
  public function logVerbose($message) {
    return $this->log($message, "VERBOSE");
  }
  
}


class Signal implements Serializable {
  
  const
    SIGNAL_END      =  "END",
    SIGNAL_KILL     =  "KILL";
  
  public $signal;
  public $data;
  
  public function __construct(string $signal, array $data = []) { 
    $this->signal = $signal;
    $this->data = $data;
  }
  
  public function serialize() { return serialize([$this->signal, $this->data]); }
  public function unserialize($str) { list($this->signal, $this->data) = unserialize($str); }
  
}
