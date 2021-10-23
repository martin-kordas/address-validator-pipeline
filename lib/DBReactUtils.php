<?php

use Closure;
use Exception;
use ws\lib\commons\classes\Singletone;
use ws\lib\utils\{LogManager, Config, ReactLoop};
use React\MySQL\{Factory, ConnectionInterface, QueryResult};
use React\Promise\PromiseInterface;
use function React\Promise\resolve;

/**
 * @method static DBReactUtils getInstance()
 */
class DBReactUtils extends Singletone
{
  
    private $reactLoop;
  
    private $databases = [];

    private $connection;
    
    
    protected function init()
    {
        $this->reactLoop = ReactLoop::getInstance();
        $this->getConnection();
    }

    public function getConnection(): ConnectionInterface
    {
        if (!isset($this->connection)) {
            try {
                $factory = new Factory($this->reactLoop->getLoop());
                $props = Config::$CONNECTION_PROPS;
                $params = "idle=0";       // jinak se event loop nespustila před voláním $this->connection->quit()
                $defaultDb = str_replace("`", "", Config::getCurrentSchoolDBName());
                $connectionUri = sprintf(
                    "%s:%s@%s:%s/%s?%s",
                    $props["db_user"], $props["db_pass"], $props["db_host"], $props["db_port"], $defaultDb, $params
                );
                $this->connection = $factory->createLazyConnection($connectionUri."?idle=0.1");
            } catch (Exception $e) {
                // vytvoření nové výjimky zabrání zalogování hesla
                throw new Exception($e->getMessage());
            }
        }
        return $this->connection;
    }
    
    public function __destruct()
    {
        $this->connection->quit();
    }

    /**
     * @param Closure $callback       příkazy transakce; callback typicky vrací PromiseInterface, protože provádí asynchronní ::query()
     * @param int &$time              sem bude uložena délka trvání příkazů v transakci
     * @return PromiseInterface
     */
    public function runInTransaction(Closure $callback, &$time = null) : PromiseInterface
    {
        $time_start = microtime(true);
        return $this->connection
            ->query("START TRANSACTION")
            ->then(function () use ($callback) {
                return $callback();
            })
            ->then(function ($result) {
                return $this->connection
                    ->query("COMMIT")
                    ->then(function () use ($result) {
                        return $result;
                    });
            })
            ->otherwise(function (Exception $ex) {
                return $this->connection
                    ->query("ROLLBACK")
                    ->always(function () use ($ex) {
                        framework()->log($ex, LogManager::LOG_LEVEL_TRANSACTION);
                        throw $ex;
                    });
            })
            ->always(function () use ($time_start, &$time) {
                $time = microtime_rozdil($time_start);
            });
    }

    public function databaseExists(string $dbName) : PromiseInterface
    {
        static $queue = null;
        if (!isset($queue)) $queue = resolve();
      
        // řazení do fronty, aby se počkalo na výsledek předchozího asynchronního volání (jinak by cachování nefungovalo)
        $res = $queue->then(function () use ($dbName) {
            $dbName = str_replace('`', "", $dbName);
            if (array_key_exists($dbName, $this->databases)) {
                return $this->databases[$dbName];
            }
            return $this->connection
                ->query("SELECT COUNT(*) AS count FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?", [$dbName])
                ->then(function (QueryResult $result) use ($dbName) {
                    $dbExists = (bool)$result->resultRows[0]["count"] ?? false;
                    $this->databases[$dbName] = $dbExists;
                    return $dbExists;
                });
        });
        $queue = $res->otherwise(function () {});
        return $res;
    }
}
