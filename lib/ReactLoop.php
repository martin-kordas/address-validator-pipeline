<?php

use ws\lib\commons\classes\Singletone;
use React\EventLoop\Factory;
use React\EventLoop\LoopInterface;
use React\EventLoop\StreamSelectLoop;
use ws\lib\exceptions\ApplicationLogicException;

/**
 * @method static ReactLoop getInstance()
 */
class ReactLoop extends Singletone
{

    protected static $_instance;
    protected static $_isloaded = false;
  
    private $loop;
    private $interruptHandler;
    private $stopped            = false;
    private $wasLaunched        = false;
    private $isRunning          = false;
    
    protected function init() {
        $this->loop = Factory::create();
    }
    
    public function getLoop() : LoopInterface {
        return $this->loop;
    }
    
    public function handleSignal(int $signal, callable $handler) {
        if (!$this->loop instanceof StreamSelectLoop || $this->canHandleSignals()) {
            $this->loop->addSignal($signal, $handler);
        }
    }
    
    public function handleInterrupt() {
        if ($this->canHandleSignals()) {
            $this->interruptHandler = function () {
                // nezastavuje reálně event loop, zastavení musí být implementováno přímo ve funkcích
                // prováděných v event loop přečtením ReactLoop::$stopped
                $this->stopped = true;
                $this->removeInterruptHandler();
            };
            $this->handleSignal(SIGINT, $this->interruptHandler);
        }
    }
    
    public function removeInterruptHandler() {
        // bez odebrání signálu event loop stále naslouchá dalšímu signálu a program se neukončí
        if (isset($this->interruptHandler)) {
            $this->loop->removeSignal(SIGINT, $this->interruptHandler);
        }
    }
    
    private function canHandleSignals() {
        return extension_loaded("pcntl");
    }
    
    public function isStopped() {
        return $this->stopped;
    }
    
    public function runLoop() : void {
        // loop lze spustit jen jednou
        if (!$this->wasLaunched) {
            $this->wasLaunched = true;
            $this->isRunning = true;
            $this->loop->run();
            $this->isRunning = false;
        }
        else throw new ApplicationLogicException();
    }
    
}
