<?php

/**
 * OCI8 Bridge Polyfill — Ephemeral per-connection daemon model
 *
 * Each call to oci_connect() / oci_pconnect() spawns a fresh PHP 5.3 daemon
 * process. That daemon owns exactly one OCI8 connection for its entire life.
 * oci_close() sends a shutdown command to the daemon and waits for it to exit,
 * cleaning up both the process and the socket.
 *
 * All behaviour lives on Oci8BridgeConnection / Oci8BridgeStatement; the global
 * oci_* functions at the bottom are the extension's ABI and nothing more.
 */

defined('OCI_ASSOC') or define('OCI_ASSOC', 2);
defined('OCI_NUM') or define('OCI_NUM', 1);
defined('OCI_BOTH') or define('OCI_BOTH', 3);
defined('OCI_RETURN_NULLS') or define('OCI_RETURN_NULLS', 4);
defined('OCI_RETURN_LOBS') or define('OCI_RETURN_LOBS', 8);

defined('OCI_COMMIT_ON_SUCCESS') or define('OCI_COMMIT_ON_SUCCESS', 32);
defined('OCI_NO_AUTO_COMMIT') or define('OCI_NO_AUTO_COMMIT', 0);
defined('OCI_DEFAULT') or define('OCI_DEFAULT', 0);
defined('OCI_DESCRIBE_ONLY') or define('OCI_DESCRIBE_ONLY', 16);

defined('OCI_FETCHSTATEMENT_BY_ROW') or define('OCI_FETCHSTATEMENT_BY_ROW', 16);
defined('OCI_FETCHSTATEMENT_BY_COLUMN') or define('OCI_FETCHSTATEMENT_BY_COLUMN', 32);

defined('SQLT_CHR') or define('SQLT_CHR', 96);
defined('SQLT_NUM') or define('SQLT_NUM', 2);
defined('SQLT_INT') or define('SQLT_INT', 3);
defined('SQLT_FLT') or define('SQLT_FLT', 4);
defined('SQLT_CLOB') or define('SQLT_CLOB', 112);
defined('SQLT_BLOB') or define('SQLT_BLOB', 113);
defined('SQLT_BIN') or define('SQLT_BIN', 23);
defined('SQLT_LNG') or define('SQLT_LNG', 8);
defined('SQLT_AFC') or define('SQLT_AFC', 96);
defined('SQLT_AVC') or define('SQLT_AVC', 96);

defined('OCI_B_CLOB') or define('OCI_B_CLOB', 112);
defined('OCI_B_BLOB') or define('OCI_B_BLOB', 113);
defined('OCI_B_INT') or define('OCI_B_INT', 3);
defined('OCI_B_NUM') or define('OCI_B_NUM', 2);
defined('OCI_B_BIN') or define('OCI_B_BIN', 23);

defined('OCI_DTYPE_LOB') or define('OCI_DTYPE_LOB', 50);
defined('OCI_DTYPE_FILE') or define('OCI_DTYPE_FILE', 51);
defined('OCI_DTYPE_ROWID') or define('OCI_DTYPE_ROWID', 52);
defined('OCI_D_LOB') or define('OCI_D_LOB', 50);
defined('OCI_D_FILE') or define('OCI_D_FILE', 51);
defined('OCI_D_ROWID') or define('OCI_D_ROWID', 52);

defined('OCI_SYSDBA') or define('OCI_SYSDBA', 2);
defined('OCI_SYSOPER') or define('OCI_SYSOPER', 4);
defined('OCI_CRED_EXT') or define('OCI_CRED_EXT', -2147483648);

/**
 * Bridge settings, resolved once per process.
 */
class Oci8BridgeConfig
{
    private static ?array $values = null;

    public static function get(string $key): mixed
    {
        self::$values ??= [
            'daemon' => __DIR__.'/daemon.php',
            'php' => '/usr/bin/php',
            'socket_dir' => storage_path('app/private'),
            'log' => storage_path('logs/oci.log'),
            'log_level' => self::env('OCI_LOG_LEVEL', 'info'),
            'timeout' => (int) self::env('OCI_TIMEOUT', '30'),
            'idle' => (int) self::env('OCI_IDLE', '30'),
            'ready_wait' => (int) self::env('OCI_READY_WAIT', '5'),
        ];

        return self::$values[$key];
    }

    private static function env(string $key, string $default): string
    {
        $value = getenv($key);

        return ($value !== false && $value !== '') ? $value : $default;
    }
}

class Oci8BridgeConnection
{
    private const MAX_RESPONSE = 10 * 1024 * 1024;

    /** Last OCI error set on this connection */
    public ?array $lastError = null;

    /** Failures that happen before a connection handle exists */
    public static array|false $lastConnectionError = false;

    public function __construct(public string $socketPath, public mixed $pid)
    {
        self::$lastConnectionError = false;
    }

    /**
     * Spawn a private daemon and have it open the Oracle connection.
     */
    public static function open(
        string $username,
        string $password,
        ?string $connectionString,
        ?string $characterSet,
        ?int $sessionMode
    ): static|false {
        $socketPath = self::socketPath();

        try {
            $pid = self::spawn($socketPath);

            $response = self::request($socketPath, [
                'command' => 'connect',
                'username' => $username,
                'password' => $password,
                'connection_string' => $connectionString,
                'character_set' => $characterSet,
                'session_mode' => $sessionMode,
            ]);
        } catch (RuntimeException $e) {
            self::$lastConnectionError = ['code' => 0, 'message' => $e->getMessage(), 'offset' => 0, 'sqltext' => ''];

            return false;
        }

        if (! $response['ok']) {
            self::$lastConnectionError = $response;

            return false;
        }

        return new static($socketPath, $pid);
    }

    public function parse(string $sql): Oci8BridgeStatement
    {
        return new Oci8BridgeStatement($this, $sql);
    }

    public function newDescriptor(int $type): Oci8BridgeLob
    {
        return new Oci8BridgeLob($type);
    }

    /**
     * Tell the daemon to shut down, then drop its socket.
     */
    public function close(): bool
    {
        try {
            self::request($this->socketPath, ['command' => 'shutdown']);
        } catch (RuntimeException) {
        }

        if (file_exists($this->socketPath)) {
            unlink($this->socketPath);
        }

        return true;
    }

    public function commit(): bool
    {
        return $this->command('commit');
    }

    public function rollback(): bool
    {
        return $this->command('rollback');
    }

    public function serverVersion(): string|false
    {
        try {
            return $this->send(['command' => 'server_version'])['result'];
        } catch (RuntimeException) {
            return false;
        }
    }

    /**
     * Send a request to this connection's daemon, recording any error it reports.
     *
     * @throws RuntimeException if the daemon is unreachable or speaks nonsense
     */
    public function send(array $payload): array
    {
        try {
            $response = self::request($this->socketPath, $payload);
        } catch (RuntimeException $e) {
            $this->lastError = ['code' => 0, 'message' => $e->getMessage(), 'offset' => 0, 'sqltext' => ''];

            throw $e;
        }

        $this->lastError = $response['ok'] ? null : $response;

        return $response;
    }

    /**
     * Run a command that only reports success, swallowing transport failures.
     */
    private function command(string $command): bool
    {
        try {
            return $this->send(['command' => $command])['ok'];
        } catch (RuntimeException) {
            return false;
        }
    }

    private static function socketPath(): string
    {
        return sprintf(
            '%s/oci-%d-%s.sock',
            rtrim(Oci8BridgeConfig::get('socket_dir'), '/'),
            getmypid(),
            substr(str_replace('.', '', (string) microtime(true)), -8)
        );
    }

    /**
     * Start a daemon and wait for its socket to appear.
     *
     * @throws RuntimeException if the daemon doesn't start in time
     */
    private static function spawn(string $socketPath): int
    {
        $log = Oci8BridgeConfig::get('log');

        $cmd = implode(' ', array_map('escapeshellarg', [
            Oci8BridgeConfig::get('php'),
            Oci8BridgeConfig::get('daemon'),
            '--socket', $socketPath,
            '--log', $log,
            '--level', Oci8BridgeConfig::get('log_level'),
            '--timeout', Oci8BridgeConfig::get('timeout'),
            '--idle', Oci8BridgeConfig::get('idle'),
        ]));

        $pid = (int) shell_exec($cmd.' >> '.escapeshellarg($log).' 2>&1 & echo $!');

        if ($pid === 0) {
            throw new RuntimeException("Failed to spawn Oracle Bridge daemon for $socketPath.");
        }

        $readyWait = Oci8BridgeConfig::get('ready_wait');
        $deadline = microtime(true) + $readyWait;

        while (! file_exists($socketPath)) {
            if (microtime(true) >= $deadline) {
                throw new RuntimeException(
                    "Oracle Bridge daemon did not become ready within {$readyWait}s. ".
                    "Check $log for errors."
                );
            }

            usleep(10000); // 10ms
        }

        return $pid;
    }

    /**
     * One request, one response, over a socket path — used before a handle exists too.
     */
    private static function request(string $socketPath, array $payload): array
    {
        $timeout = Oci8BridgeConfig::get('timeout');

        $socket = @stream_socket_client('unix://'.$socketPath, $errno, $errstr, $timeout);

        if (! $socket) {
            throw new RuntimeException("ORA-03135: Cannot connect to Oracle Bridge at '$socketPath': $errstr ($errno)");
        }

        stream_set_timeout($socket, $timeout);

        try {
            self::writeFrame($socket, $payload);

            return self::readFrame($socket);
        } finally {
            fclose($socket);
        }
    }

    private static function writeFrame(mixed $socket, array $payload): void
    {
        $json = json_encode($payload);
        $frame = pack('N', strlen($json)).$json;

        $written = 0;
        $total = strlen($frame);

        while ($written < $total) {
            if (($n = fwrite($socket, substr($frame, $written))) === false) {
                throw new RuntimeException('Write to Oracle Bridge socket failed.');
            }

            $written += $n;
        }
    }

    private static function readFrame(mixed $socket): array
    {
        $header = self::readExact($socket, 4, 'Oracle Bridge closed connection before responding.');

        $length = unpack('N', $header)[1];

        if ($length === 0 || $length > self::MAX_RESPONSE) {
            throw new RuntimeException("Oracle Bridge returned invalid response length: $length");
        }

        $body = self::readExact($socket, $length, 'Incomplete response from Oracle Bridge.');

        $data = json_decode($body, true);

        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new RuntimeException('Invalid JSON from Oracle Bridge: '.substr($body, 0, 200));
        }

        return $data;
    }

    private static function readExact(mixed $socket, int $length, string $onEof): string
    {
        $buffer = '';

        while (strlen($buffer) < $length) {
            $chunk = fread($socket, $length - strlen($buffer));

            if ($chunk === false || $chunk === '') {
                throw new RuntimeException($onEof);
            }

            $buffer .= $chunk;
        }

        return $buffer;
    }
}

class Oci8BridgeBinding
{
    public function __construct(
        public string $name,
        public mixed &$variable,
        public int $maxlength,
        public int $type
    ) {}

    /**
     * The wire form, with LOB descriptors flattened to their payload.
     */
    public function payload(): array
    {
        $value = $this->variable;

        return [
            'bv_name' => $this->name,
            'variable' => $value instanceof Oci8BridgeLob ? $value->value : $value,
            'maxlength' => $this->maxlength,
            'type' => $this->type,
        ];
    }

    /**
     * Take an OUT value from the daemon back into the caller's variable.
     */
    public function receive(mixed $value): void
    {
        if ($this->variable instanceof Oci8BridgeLob) {
            $this->variable->value = $value;
        } elseif ($this->type === SQLT_INT && is_numeric($value)) {
            $this->variable = (int) $value;
        } else {
            $this->variable = $value;
        }
    }
}

class Oci8BridgeStatement
{
    private const TYPES = [
        'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP',
        'ALTER', 'BEGIN', 'DECLARE', 'CALL', 'TRUNCATE', 'MERGE',
    ];

    /** @var Oci8BridgeBinding[] */
    public array $bindings = [];

    public array $arrayBindings = [];

    public ?array $rows = null;

    public ?array $columns = null;

    public int $affectedRows = 0;

    public int $rowIndex = 0;

    public bool $executed = false;

    public ?array $lastError = null;

    public function __construct(public Oci8BridgeConnection $connection, public string $sql) {}

    public function type(): string
    {
        $keyword = strtoupper(strtok(ltrim($this->sql), " \t\n\r("));

        return in_array($keyword, self::TYPES, true) ? $keyword : 'UNKNOWN';
    }

    public function bind(string $name, mixed &$variable, int $maxlength, int $type): bool
    {
        $this->bindings[] = new Oci8BridgeBinding($name, $variable, $maxlength, $type);

        return true;
    }

    public function bindArray(
        string $name,
        array &$values,
        int $maxArraySize,
        int $maxItemSize,
        int $type
    ): bool {
        $this->arrayBindings[] = [
            'name' => $name,
            'var_array' => $values,
            'max_array_size' => $maxArraySize,
            'max_item_size' => $maxItemSize,
            'type' => $type,
        ];

        return true;
    }

    public function execute(int $mode): bool
    {
        try {
            $response = $this->connection->send([
                'command' => $this->type() === 'SELECT' ? 'select' : 'query',
                'sql' => $this->sql,
                'bindings' => array_map(fn (Oci8BridgeBinding $bind) => $bind->payload(), $this->bindings),
                'arrayBindings' => $this->arrayBindings,
                'mode' => $mode,
            ]);
        } catch (RuntimeException $e) {
            $this->lastError = $this->connection->lastError = [
                'code' => 0, 'message' => $e->getMessage(), 'offset' => 0, 'sqltext' => $this->sql,
            ];

            return false;
        }

        if (! $response['ok']) {
            $this->lastError = $this->connection->lastError;

            return false;
        }

        $this->executed = true;
        $this->lastError = null;
        $this->rows = $response['rows'] ?? [];
        $this->columns = $response['columns'] ?? [];
        $this->affectedRows = $response['affected'] ?? 0;
        $this->rowIndex = 0;

        $this->applyOutBindings($response['out'] ?? []);

        return true;
    }

    public function free(): bool
    {
        $this->rows = null;
        $this->columns = null;
        $this->bindings = [];
        $this->arrayBindings = [];
        $this->executed = false;

        return true;
    }

    public function fetch(int $mode): array|false
    {
        if ($this->rows === null || $this->rowIndex >= count($this->rows)) {
            return false;
        }

        return self::formatRow($this->rows[$this->rowIndex++], $mode);
    }

    public function fetchAll(mixed &$output, int $skip, int $maxrows, int $flags): int|false
    {
        if ($this->rows === null) {
            return false;
        }

        $rows = array_slice($this->rows, $skip, $maxrows === -1 ? null : $maxrows);

        if ($flags & OCI_FETCHSTATEMENT_BY_ROW) {
            $rowFlags = $flags & ~OCI_FETCHSTATEMENT_BY_ROW;

            $output = array_map(fn ($row) => self::formatRow($row, $rowFlags ?: OCI_ASSOC), $rows);
        } else {
            $output = [];

            foreach ($rows as $row) {
                foreach ($row as $column => $value) {
                    $output[$column][] = $value;
                }
            }
        }

        return count($rows);
    }

    /**
     * Column keys in select order, from the result metadata or the first row.
     */
    public function columnKeys(): array
    {
        if ($this->columns !== null) {
            return array_keys($this->columns);
        }

        return empty($this->rows) ? [] : array_keys($this->rows[0]);
    }

    public function fieldName(int|string $column): string|false
    {
        $key = $this->columnKey($column);

        return $key !== null ? strtoupper($key) : false;
    }

    /**
     * Read one attribute of a column's metadata, falling back when it is unknown.
     */
    public function fieldMeta(int|string $column, string $attribute, mixed $default): mixed
    {
        $key = $this->columnKey($column);

        return $this->columns[$key][$attribute] ?? $default;
    }

    /**
     * Resolve a 1-based column position or a column name to a metadata key.
     */
    private function columnKey(int|string $column): ?string
    {
        $keys = $this->columnKeys();

        if (is_int($column)) {
            return $keys[$column - 1] ?? null;
        }

        $key = strtolower($column);

        return in_array($key, $keys, true) ? $key : null;
    }

    /**
     * Hand each OUT value to the binding that owns the caller's variable.
     */
    private function applyOutBindings(array $out): void
    {
        foreach ($out as $key => $value) {
            if (isset($this->bindings[$key])) {
                $this->bindings[$key]->receive($value);
            }
        }
    }

    private static function formatRow(array $row, int $mode): array
    {
        $formatted = [];
        $index = 0;

        foreach ($row as $column => $value) {
            if ($mode & OCI_ASSOC) {
                $formatted[$column] = $value;
            }

            if ($mode & OCI_NUM) {
                $formatted[$index] = $value;
            }

            $index++;
        }

        return $formatted;
    }
}

/**
 * Fake LOB descriptor for CLOB/BLOB binding.
 */
class Oci8BridgeLob
{
    public ?string $value = null;

    public function __construct(public int $type) {}

    public function save(string $data): bool
    {
        $this->value = $data;

        return true;
    }

    public function load(): ?string
    {
        return $this->value;
    }

    public function size(): int
    {
        return $this->value !== null ? strlen($this->value) : 0;
    }

    public function free(): bool
    {
        $this->value = null;

        return true;
    }
}

// ---------------------------------------------------------------------------
// OCI8 polyfill functions — the extension's ABI, delegating to the classes above
// ---------------------------------------------------------------------------

// ---- Connection ----

if (! function_exists('oci_connect')) {
    function oci_connect(
        string $username,
        string $password,
        ?string $connection_string = null,
        ?string $character_set = null,
        ?int $session_mode = null
    ): Oci8BridgeConnection|false {
        return Oci8BridgeConnection::open(
            $username, $password, $connection_string, $character_set, $session_mode
        );
    }
}

if (! function_exists('oci_pconnect')) {
    /**
     * In this model "persistent" means the daemon lives as long as the
     * connection handle — identical to oci_connect. The daemon itself
     * uses a non-persistent OCI8 connection internally.
     */
    function oci_pconnect(
        string $username,
        string $password,
        string $connection_string,
        string $encoding = 'AL32UTF8',
        int $session_mode = 0
    ): Oci8BridgeConnection|false {
        return oci_connect($username, $password, $connection_string, $encoding, $session_mode);
    }
}

if (! function_exists('oci_new_connect')) {
    function oci_new_connect(
        string $username,
        string $password,
        string $connection_string,
        string $encoding = 'AL32UTF8',
        int $session_mode = 0
    ): Oci8BridgeConnection|false {
        return oci_connect($username, $password, $connection_string, $encoding, $session_mode);
    }
}

if (! function_exists('oci_close')) {
    function oci_close(mixed $connection): bool
    {
        return $connection instanceof Oci8BridgeConnection && $connection->close();
    }
}

if (! function_exists('oci_server_version')) {
    function oci_server_version(mixed $connection): string|false
    {
        return $connection instanceof Oci8BridgeConnection ? $connection->serverVersion() : false;
    }
}

// ---- Statement lifecycle ----

if (! function_exists('oci_parse')) {
    function oci_parse(mixed $connection, string $sql): Oci8BridgeStatement|false
    {
        return $connection instanceof Oci8BridgeConnection ? $connection->parse($sql) : false;
    }
}

if (! function_exists('oci_free_statement')) {
    function oci_free_statement(mixed $statement): bool
    {
        return $statement instanceof Oci8BridgeStatement && $statement->free();
    }
}

if (! function_exists('oci_statement_type')) {
    function oci_statement_type(mixed $statement): string|false
    {
        return $statement instanceof Oci8BridgeStatement ? $statement->type() : false;
    }
}

if (! function_exists('oci_set_prefetch')) {
    function oci_set_prefetch(mixed $statement, int $rows): bool
    {
        return true; // daemon fetches all rows at once; this is a no-op
    }
}

// ---- Binding ----

if (! function_exists('oci_bind_by_name')) {
    function oci_bind_by_name(
        mixed $statement,
        string $bv_name,
        mixed &$variable,
        int $maxlength = -1,
        int $type = SQLT_CHR
    ): bool {
        return $statement->bind($bv_name, $variable, $maxlength, $type);
    }
}

if (! function_exists('oci_bind_array_by_name')) {
    function oci_bind_array_by_name(
        mixed $statement,
        string $name,
        array &$var_array,
        int $max_array_size,
        int $max_item_size = -1,
        int $type = SQLT_AFC
    ): bool {
        return $statement->bindArray($name, $var_array, $max_array_size, $max_item_size, $type);
    }
}

// ---- Execution ----

if (! function_exists('oci_execute')) {
    function oci_execute(mixed $statement, int $mode = OCI_COMMIT_ON_SUCCESS): bool
    {
        return $statement->execute($mode);
    }
}

// ---- Fetching ----

if (! function_exists('oci_fetch_array')) {
    function oci_fetch_array(mixed $statement, int $mode = OCI_BOTH + OCI_RETURN_NULLS): array|false
    {
        return $statement instanceof Oci8BridgeStatement ? $statement->fetch($mode) : false;
    }
}

if (! function_exists('oci_fetch_assoc')) {
    function oci_fetch_assoc(mixed $statement): array|false
    {
        return oci_fetch_array($statement, OCI_ASSOC + OCI_RETURN_NULLS);
    }
}

if (! function_exists('oci_fetch_row')) {
    function oci_fetch_row(mixed $statement): array|false
    {
        return oci_fetch_array($statement, OCI_NUM + OCI_RETURN_NULLS);
    }
}

if (! function_exists('oci_fetch_object')) {
    function oci_fetch_object(mixed $statement): object|false
    {
        $row = oci_fetch_array($statement, OCI_ASSOC + OCI_RETURN_NULLS);

        return $row === false ? false : (object) array_change_key_case($row);
    }
}

if (! function_exists('oci_fetch_all')) {
    function oci_fetch_all(
        mixed $statement,
        mixed &$output,
        int $skip = 0,
        int $maxrows = -1,
        int $flags = OCI_FETCHSTATEMENT_BY_COLUMN + OCI_ASSOC
    ): int|false {
        if (! $statement instanceof Oci8BridgeStatement) {
            return false;
        }

        return $statement->fetchAll($output, $skip, $maxrows, $flags);
    }
}

// ---- Row / field metadata ----

if (! function_exists('oci_num_rows')) {
    function oci_num_rows(mixed $statement): int|false
    {
        return $statement instanceof Oci8BridgeStatement ? $statement->affectedRows : false;
    }
}

if (! function_exists('oci_num_fields')) {
    function oci_num_fields(mixed $statement): int|false
    {
        return $statement instanceof Oci8BridgeStatement ? count($statement->columnKeys()) : false;
    }
}

if (! function_exists('oci_field_name')) {
    function oci_field_name(mixed $statement, int|string $column): string|false
    {
        return $statement instanceof Oci8BridgeStatement ? $statement->fieldName($column) : false;
    }
}

if (! function_exists('oci_field_type')) {
    function oci_field_type(mixed $statement, int|string $column): string|int|false
    {
        return $statement instanceof Oci8BridgeStatement
            ? $statement->fieldMeta($column, 'type', 'VARCHAR2')
            : 'VARCHAR2';
    }
}

if (! function_exists('oci_field_type_raw')) {
    function oci_field_type_raw(mixed $statement, int|string $column): int|false
    {
        return SQLT_CHR;
    }
}

if (! function_exists('oci_field_size')) {
    function oci_field_size(mixed $statement, int|string $column): int|false
    {
        return $statement instanceof Oci8BridgeStatement
            ? (int) $statement->fieldMeta($column, 'size', 255)
            : 255;
    }
}

if (! function_exists('oci_field_precision')) {
    function oci_field_precision(mixed $statement, int|string $column): int|false
    {
        return 0;
    }
}

if (! function_exists('oci_field_scale')) {
    function oci_field_scale(mixed $statement, int|string $column): int|false
    {
        return 0;
    }
}

if (! function_exists('oci_field_is_null')) {
    function oci_field_is_null(mixed $statement, int|string $column): int|false
    {
        return 0;
    }
}

// ---- Transaction control ----

if (! function_exists('oci_commit')) {
    function oci_commit(mixed $connection): bool
    {
        return $connection instanceof Oci8BridgeConnection && $connection->commit();
    }
}

if (! function_exists('oci_rollback')) {
    function oci_rollback(mixed $connection): bool
    {
        return $connection instanceof Oci8BridgeConnection && $connection->rollback();
    }
}

// ---- Error handling ----

if (! function_exists('oci_error')) {
    function oci_error(mixed $resource = null): array|false
    {
        if ($resource instanceof Oci8BridgeStatement || $resource instanceof Oci8BridgeConnection) {
            return $resource->lastError ?? Oci8BridgeConnection::$lastConnectionError;
        }

        return Oci8BridgeConnection::$lastConnectionError;
    }
}

// ---- LOB descriptors ----

if (! function_exists('oci_new_descriptor')) {
    function oci_new_descriptor(mixed $connection, int $type = OCI_DTYPE_LOB): Oci8BridgeLob|false
    {
        return $connection instanceof Oci8BridgeConnection ? $connection->newDescriptor($type) : false;
    }
}

// ---- Miscellaneous stubs ----

if (! function_exists('oci_internal_debug')) {
    function oci_internal_debug(int $onoff): void {}
}

if (! function_exists('oci_password_change')) {
    function oci_password_change(mixed $c, string $u, string $o, string $n): bool
    {
        trigger_error('oci_password_change() is not supported by the OCI8 bridge.', E_USER_WARNING);

        return false;
    }
}
