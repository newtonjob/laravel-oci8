<?php

/**
 * Oracle Bridge Daemon — Ephemeral, per-connection, PHP 5.3 compatible
 *
 * This daemon is spawned by the polyfill's oci_connect() and lives for exactly
 * the duration of one Laravel request's database connection. It holds a single
 * OCI8 connection and exits when it receives a 'shutdown' command (triggered by
 * oci_close()) or when the idle timeout expires.
 *
 * Because each PHP request gets its own private daemon process and its own
 * private Unix socket, concurrent requests are completely isolated from each
 * other with no shared state, no connection pooling, and no routing logic.
 *
 * Spawned by the polyfill as:
 *   php daemon.php \
 *     --socket  /tmp/oci-<unique>.sock \
 *     --log     /var/log/oracle_bridge.log \
 *     --level   info \
 *     --timeout 30
 *
 * The socket path is unique per spawn (contains the parent PID + microtime),
 * so there is never a collision between concurrent connections.
 */

error_reporting(E_ALL);
ini_set('display_errors', 0);

defined('OCI_COMMIT_ON_SUCCESS') or define('OCI_COMMIT_ON_SUCCESS', 32);
defined('OCI_NO_AUTO_COMMIT') or define('OCI_NO_AUTO_COMMIT', 0);
defined('OCI_ASSOC') or define('OCI_ASSOC', 2);
defined('OCI_RETURN_NULLS') or define('OCI_RETURN_NULLS', 4);
defined('OCI_RETURN_LOBS') or define('OCI_RETURN_LOBS', 8);

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

final class Args
{
    private $values;

    private static $defaults = array(
        'socket' => '',
        'user' => '',
        'pass' => '',
        'dsn' => '',
        'charset' => 'UTF8',
        'log' => '/dev/null',
        'level' => 'info',
        'timeout' => 30,
        'retries' => 3,
        'delay' => 2,
        'idle' => 30,
    );

    public function __construct(array $argv)
    {
        $this->values = self::$defaults;

        for ($i = 1, $len = count($argv); $i < $len; $i++) {
            $arg = $argv[$i];
            if (strpos($arg, '--') === 0 && isset($argv[$i + 1])) {
                $key = substr($arg, 2);
                if (array_key_exists($key, self::$defaults)) {
                    $this->values[$key] = $argv[++$i];
                }
            }
        }
    }

    public function get($key, $default = null)
    {
        return isset($this->values[$key]) ? $this->values[$key] : $default;
    }

    public function int($key, $default = 0)
    {
        return (int) $this->get($key, $default);
    }
}

// ---------------------------------------------------------------------------
// Logger
// ---------------------------------------------------------------------------

final class Logger
{
    private $logFile;

    private $minLevel;

    private $pid;

    private static $levels = array('debug' => 0, 'info' => 1, 'warn' => 2, 'error' => 3);

    public function __construct($logFile, $minLevel = 'info')
    {
        $this->logFile = $logFile;
        $this->minLevel = isset(self::$levels[$minLevel]) ? $minLevel : 'info';
        $this->pid = getmypid();
    }

    public function log($level, $message)
    {
        if (self::$levels[$level] < self::$levels[$this->minLevel]) {
            return;
        }

        $line = sprintf(
            "[%s] [%-5s] [PID:%d] %s\n",
            date('Y-m-d H:i:s'),
            strtoupper($level),
            $this->pid,
            $message
        );

        file_put_contents($this->logFile, $line, FILE_APPEND | LOCK_EX);
    }

    public function debug($msg)
    {
        $this->log('debug', $msg);
    }

    public function info($msg)
    {
        $this->log('info', $msg);
    }

    public function warn($msg)
    {
        $this->log('warn', $msg);
    }

    public function error($msg)
    {
        $this->log('error', $msg);
    }
}

// ---------------------------------------------------------------------------
// OracleConnection — owns the single OCI8 handle for this daemon's lifetime
// ---------------------------------------------------------------------------

final class OracleConnection
{
    private $handle = null;

    private $inTransaction = false;

    /** @var Logger */
    private $logger;

    public function __construct(Logger $logger)
    {
        $this->logger = $logger;
    }

    public function connect($username, $password, $connection_string, $character_set, $session_mode)
    {
        $this->logger->info("Connecting: $connection_string as $username");

        if ($this->handle = oci_connect($username, $password, $connection_string, $character_set, $session_mode)) {
            $this->logger->info("Connected: $connection_string as $username");

            return array('ok' => true);
        }

        $e = oci_error();

        $this->logger->error('Connect attempt failed: '.$e['message']);

        return $this->ociError($e);
    }

    public function ping()
    {
        if ($this->handle === null) {
            return false;
        }

        $stmt = @oci_parse($this->handle, 'SELECT 1 FROM DUAL');

        if (! $stmt) {
            return false;
        }

        $ok = @oci_execute($stmt, OCI_NO_AUTO_COMMIT);

        oci_free_statement($stmt);

        return $ok;
    }

    public function close()
    {
        if ($this->handle === null) {
            return;
        }

        oci_rollback($this->handle);
        oci_close($this->handle);

        $this->handle = null;
        $this->logger->info('Oracle connection closed.');
    }

    public function commit()
    {
        $this->inTransaction = false;

        if (! oci_commit($this->handle)) {
            return $this->ociError(oci_error($this->handle));
        }

        $this->logger->debug('Transaction committed.');

        return array('ok' => true);
    }

    public function rollback()
    {
        $this->inTransaction = false;

        if (! oci_rollback($this->handle)) {
            return $this->ociError(oci_error($this->handle));
        }

        $this->logger->debug('Transaction rolled back.');

        return array('ok' => true);
    }

    // ---- Query execution ----

    public function select($sql, $bindings, $arrayBindings)
    {
        $this->logger->debug('SELECT: '.substr($sql, 0, 200));

        if (! $stmt = oci_parse($this->handle, $sql)) {
            return $this->ociError(oci_error($this->handle), $sql);
        }

        $this->bindAll($stmt, $bindings, $arrayBindings, $vars);

        if (! oci_execute($stmt, OCI_NO_AUTO_COMMIT)) {
            $err = oci_error($stmt);

            oci_free_statement($stmt);

            return $this->ociError($err);
        }

        $columns = $this->columnMeta($stmt);
        $rows = $this->fetchRows($stmt);

        oci_free_statement($stmt);

        return array('ok' => true, 'rows' => $rows, 'columns' => $columns, 'out' => $vars);
    }

    public function query($sql, $bindings, $arrayBindings, $mode)
    {
        if (! $stmt = oci_parse($this->handle, $sql)) {
            return $this->ociError(oci_error($this->handle));
        }

        $this->bindAll($stmt, $bindings, $arrayBindings, $vars);

        $execMode = ($this->inTransaction || (int) $mode === OCI_NO_AUTO_COMMIT)
            ? OCI_NO_AUTO_COMMIT
            : OCI_COMMIT_ON_SUCCESS;

        if ($execMode === OCI_NO_AUTO_COMMIT) {
            $this->inTransaction = true;
        }

        $this->logger->debug(sprintf(
            'QUERY [%s]: %s',
            $execMode === OCI_NO_AUTO_COMMIT ? 'no-autocommit' : 'autocommit',
            substr($sql, 0, 200)
        ));

        if (! oci_execute($stmt, $execMode)) {
            $err = oci_error($stmt);

            oci_free_statement($stmt);

            return $this->ociError($err);
        }

        $affected = oci_num_rows($stmt);

        oci_free_statement($stmt);

        return array('ok' => true, 'affected' => $affected, 'out' => $vars);
    }

    public function serverVersion()
    {
        return array('ok' => true, 'result' => oci_server_version($this->handle));
    }

    // ---- Private helpers ----

    /**
     * Bind all parameters onto a statement — $vars receives OUT values on execute.
     */
    private function bindAll($statement, $bindings, $arrayBindings, &$vars)
    {
        $vars = array();

        foreach ($bindings as $i => $bind) {
            $vars[$i] = $bind['variable'];

            oci_bind_by_name(
                $statement, $bind['bv_name'], $vars[$i], $bind['maxlength'], $bind['type']
            );
        }

        foreach ($arrayBindings as $bind) {
            oci_bind_array_by_name(
                $statement, $bind['name'], $bind['var_array'], $bind['max_array_size'],
                $bind['max_item_size'], $bind['type']
            );
        }
    }

    private function columnMeta($stmt)
    {
        $columns = array();
        $n = oci_num_fields($stmt);

        for ($i = 1; $i <= $n; $i++) {
            $name = strtolower(oci_field_name($stmt, $i));
            $columns[$name] = array(
                'type' => oci_field_type($stmt, $i),
                'size' => oci_field_size($stmt, $i),
            );
        }

        return $columns;
    }

    private function fetchRows($stmt)
    {
        $rows = array();

        while (($row = oci_fetch_array($stmt, OCI_ASSOC + OCI_RETURN_NULLS + OCI_RETURN_LOBS)) !== false) {
            $rows[] = $row;
        }

        return $rows;
    }

    private function ociError($e)
    {
        $e['ok'] = false;

        return $e;
    }
}

// ---------------------------------------------------------------------------
// SocketServer — length-prefixed message I/O over a Unix domain socket
// ---------------------------------------------------------------------------

final class SocketServer
{
    private $server;

    private $path;

    private $maxMessage;

    /** @var Logger */
    private $logger;

    public function __construct($path, $maxMessage, Logger $logger)
    {
        $this->path = $path;
        $this->maxMessage = (int) $maxMessage;
        $this->logger = $logger;

        if (file_exists($path)) {
            unlink($path);
        }

        if (! $server = stream_socket_server('unix://'.$path, $errno, $errstr)) {
            throw new RuntimeException("Cannot create socket at '$path': $errstr ($errno)");
        }

        // Only owner-readable by default; chmod to allow the web server user
        chmod($path, 0660);
        stream_set_blocking($server, false);

        $this->server = $server;
        $this->logger->info("Listening on $path");
    }

    /**
     * Wait up to $timeout seconds for a client, return stream or null on timeout.
     */
    public function accept($timeout = 1)
    {
        return @stream_socket_accept($this->server, $timeout) ?: null;
    }

    public function readMessage($client)
    {
        $header = $this->readExact($client, 4);

        if ($header === null) {
            return null;
        }

        $unpacked = unpack('N', $header);

        $length = $unpacked[1];

        if ($length === 0 || $length > $this->maxMessage) {
            $this->logger->warn("Bad message length: $length");

            return null;
        }

        $body = $this->readExact($client, $length);

        if ($body === null) {
            return null;
        }

        $data = json_decode($body, true);

        if (json_last_error() !== JSON_ERROR_NONE) {
            $this->logger->warn('JSON decode failed: '.json_last_error_msg());

            return null;
        }

        return $data;
    }

    public function writeMessage($client, $data)
    {
        $json = json_encode($data);
        $frame = pack('N', strlen($json)).$json;

        $written = 0;
        $total = strlen($frame);
        while ($written < $total) {
            $n = fwrite($client, substr($frame, $written));
            if ($n === false || $n === 0) {
                return false;
            }
            $written += $n;
        }

        return true;
    }

    public function cleanup()
    {
        if (file_exists($this->path)) {
            unlink($this->path);

            $this->logger->info("Socket removed: {$this->path}");
        }
    }

    private function readExact($client, $length)
    {
        $buf = '';

        while (strlen($buf) < $length) {
            $chunk = fread($client, $length - strlen($buf));
            if ($chunk === false || $chunk === '') {
                return null;
            }
            $buf .= $chunk;
        }

        return $buf;
    }
}

// ---------------------------------------------------------------------------
// RequestDispatcher — routes commands to OracleConnection methods
// ---------------------------------------------------------------------------

final class RequestDispatcher
{
    /** @var OracleConnection */
    private $conn;

    /** @var Logger */
    private $logger;

    public function __construct(OracleConnection $conn, Logger $logger)
    {
        $this->conn = $conn;
        $this->logger = $logger;
    }

    /**
     * @return array|'shutdown' A response array, or the string 'shutdown' to signal the daemon should exit.
     */
    public function dispatch($request)
    {
        $command = isset($request['command']) ? $request['command'] : '';

        switch ($command) {
            case 'connect':
                return $this->conn->connect(
                    $request['username'],
                    $request['password'],
                    $request['connection_string'],
                    $request['character_set'],
                    $request['session_mode']
                );

            case 'ping':
                return array('ok' => true, 'pid' => getmypid());

            case 'server_version':
                return $this->conn->serverVersion();

            case 'commit':
                return $this->conn->commit();

            case 'rollback':
                return $this->conn->rollback();

            case 'select':
                return $this->conn->select(
                    $request['sql'], $request['bindings'], $request['arrayBindings']
                );

            case 'query':
                return $this->conn->query(
                    $request['sql'], $request['bindings'], $request['arrayBindings'], (int) $request['mode']
                );

            case 'shutdown':
                return 'shutdown';

            default:
                $this->logger->warn("Unknown command: $command");

                return array('ok' => false, 'message' => "Unknown command: $command");
        }
    }
}

// ---------------------------------------------------------------------------
// Daemon — top-level orchestrator, single connection, ephemeral lifetime
// ---------------------------------------------------------------------------

final class Daemon
{
    /** @var Args */
    private $args;

    /** @var Logger */
    private $logger;

    /** @var SocketServer */
    private $socket;

    /** @var OracleConnection */
    private $conn;

    /** @var RequestDispatcher */
    private $dispatcher;

    private $running = true;

    private $signalsEnabled = false;

    private $lastActive = 0;

    public function __construct(Args $args)
    {
        $this->args = $args;

        $this->logger = new Logger($args->get('log'), $args->get('level'));
    }

    public function run()
    {
        $this->listenForSignals();
        $this->openConnection();
        $this->startSocket();
        $this->loop();
    }

    private function openConnection()
    {
        $this->conn = new OracleConnection($this->logger);

        $this->dispatcher = new RequestDispatcher($this->conn, $this->logger);
        $this->lastActive = time();
    }

    private function startSocket()
    {
        $this->socket = new SocketServer(
            $this->args->get('socket'),
            $this->args->int('max_message', 10485760),
            $this->logger
        );

        $this->logger->info(sprintf(
            'Oracle Bridge ready [socket=%s] [dsn=%s]',
            $this->args->get('socket'),
            $this->args->get('dsn')
        ));
    }

    private function loop()
    {
        $socketTimeout = $this->args->int('timeout', 30);
        $idleTimeout = $this->args->int('idle', 30);

        while ($this->running) {
            if ($this->signalsEnabled) {
                pcntl_signal_dispatch();
            }

            // Self-terminate if idle too long (means oci_close was never called,
            // e.g. due to a PHP fatal error in the Laravel request)
            if ($idleTimeout > 0 && (time() - $this->lastActive) >= $idleTimeout) {
                $this->logger->warn("Idle timeout reached ({$idleTimeout}s). Self-terminating.");
                break;
            }

            $client = $this->socket->accept();

            if ($client === null) {
                continue; // tick, check idle, loop
            }

            $this->lastActive = time();
            stream_set_timeout($client, $socketTimeout);

            $request = $this->socket->readMessage($client);

            if ($request !== null) {
                $result = $this->dispatcher->dispatch($request);

                if ($result === 'shutdown') {
                    $this->socket->writeMessage($client, array('ok' => true));

                    fclose($client);

                    break;
                }

                $this->socket->writeMessage($client, $result);
            }

            fclose($client);
        }

        $this->shutdown();
    }

    private function listenForSignals()
    {
        if (! function_exists('pcntl_signal')) {
            $this->logger->warn('pcntl unavailable; only the idle timeout can reap this daemon.');

            return;
        }

        foreach (array(SIGTERM, SIGINT, SIGHUP) as $signal) {
            pcntl_signal($signal, array($this, 'handleSignal'));
        }

        $this->signalsEnabled = true;
    }

    /**
     * Ask the loop to stop after the request it is currently serving.
     */
    public function handleSignal($signal)
    {
        $this->logger->info("Signal $signal received; shutting down after the current request.");

        $this->running = false;
    }

    public function shutdown()
    {
        $this->conn->close();
        $this->socket->cleanup();
        $this->logger->info('Daemon exited cleanly.');

        exit(0);
    }
}

$daemon = (new Daemon(new Args($argv)));
$daemon->run();
