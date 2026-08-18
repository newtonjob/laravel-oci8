<?php

/**
 * OCI8 Bridge Polyfill — Ephemeral per-connection daemon model
 *
 * Each call to oci_connect() / oci_pconnect() spawns a fresh PHP 5.3 daemon
 * process. That daemon owns exactly one OCI8 connection for its entire life.
 * oci_close() sends a shutdown command to the daemon and waits for it to exit,
 * cleaning up both the process and the socket.
 *
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

function _oci_env(string $key, string $default = ''): string
{
    $val = getenv($key);

    return ($val !== false && $val !== '') ? $val : $default;
}

function _oci_config(): array
{
    static $cfg = null;

    if ($cfg !== null) {
        return $cfg;
    }

    $cfg = [
        'daemon' => dirname(__FILE__).'/daemon.php',
        'php' => '/usr/bin/php',
        'socket_dir' => storage_path('app/private'),
        'log' => storage_path('logs/oci.log'),
        'log_level' => _oci_env('OCI_LOG_LEVEL', 'info'),
        'timeout' => (int) _oci_env('OCI_TIMEOUT', '30'),
        'idle' => (int) _oci_env('OCI_IDLE', '30'),
        'ready_wait' => (int) _oci_env('OCI_READY_WAIT', '5'),
    ];

    return $cfg;
}

/**
 * Represents an open connection.
 * Holds the socket path and the daemon process handle so oci_close() can
 * send a shutdown command and reap the process.
 */
class OCI8BridgeConnection
{
    /** Last OCI error set on this connection */
    public ?array $lastError = null;

    public static array|false $lastConnectionError = false;

    public function __construct(public string $socketPath, public mixed $pid)
    {
        OCI8BridgeConnection::$lastConnectionError = false;
    }
}

/**
 * Represents a parsed statement.
 * Accumulates bindings locally, flushes to the daemon on oci_execute.
 */
class OCI8BridgeStatement
{
    public string $sql;

    public OCI8BridgeConnection $connection;

    public array $bindings = [];

    public array $arrayBindings = [];

    public ?array $rows = null;

    public ?array $columns = null;

    public int $affectedRows = 0;

    public int $rowIndex = 0;

    public bool $executed = false;

    public ?array $lastError = null;

    public function __construct(OCI8BridgeConnection $connection, string $sql)
    {
        $this->connection = $connection;
        $this->sql = $sql;
    }
}

/**
 * Fake LOB descriptor for CLOB/BLOB binding.
 */
class OCI8BridgeLob
{
    public ?string $value = null;

    public int $type;

    public function __construct(int $type)
    {
        $this->type = $type;
    }

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

/**
 * Spawns a daemon process and waits for its socket to become available.
 * Returns the process handle (from proc_open).
 *
 * @throws RuntimeException if the daemon doesn't start in time
 */
function _oci_spawn_daemon(string $socketPath): mixed
{
    $config = _oci_config();

    $cmd = implode(' ', array_map('escapeshellarg', [
        $config['php'],
        $config['daemon'],
        '--socket', $socketPath,
        '--log', $config['log'],
        '--level', $config['log_level'],
        '--timeout', $config['timeout'],
        '--idle', $config['idle'],
    ]));

    $pid = (int) shell_exec($cmd.' >> '.escapeshellarg($config['log']).' 2>&1 & echo $!');

    if ($pid === 0) {
        throw new RuntimeException("Failed to spawn Oracle Bridge daemon for $socketPath.");
    }

    // Wait for the socket file to appear — this confirms the daemon is up
    $deadline = microtime(true) + $config['ready_wait'];

    while (! file_exists($socketPath)) {
        if (microtime(true) >= $deadline) {
            throw new RuntimeException(
                "Oracle Bridge daemon did not become ready within {$config['ready_wait']}s. ".
                "Check {$config['log']} for errors."
            );
        }

        usleep(10000); // 10ms
    }

    return $pid;
}

// ---------------------------------------------------------------------------
// Socket I/O — send one request to a specific daemon, get one response
// ---------------------------------------------------------------------------

/**
 * Send a payload to the daemon at the given socket path and return the response.
 */
function _oci_send(string $socketPath, array $payload): array
{
    $timeout = _oci_config()['timeout'];

    $socket = @stream_socket_client('unix://'.$socketPath, $errno, $errstr, $timeout);

    if (! $socket) {
        throw new RuntimeException("Cannot connect to Oracle Bridge at '$socketPath': $errstr ($errno)");
    }

    stream_set_timeout($socket, $timeout);

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

    // Read a 4-byte length prefix
    $header = '';

    while (strlen($header) < 4) {
        $chunk = fread($socket, 4 - strlen($header));

        if ($chunk === false || $chunk === '') {
            throw new RuntimeException('Oracle Bridge closed connection before responding.');
        }

        $header .= $chunk;
    }

    $length = unpack('N', $header)[1];

    if ($length === 0 || $length > 10 * 1024 * 1024) {
        throw new RuntimeException("Oracle Bridge returned invalid response length: $length");
    }

    $body = '';

    while (strlen($body) < $length) {
        $chunk = fread($socket, $length - strlen($body));

        if ($chunk === false || $chunk === '') {
            throw new RuntimeException('Incomplete response from Oracle Bridge.');
        }

        $body .= $chunk;
    }

    fclose($socket);

    $data = json_decode($body, true);

    if (json_last_error() !== JSON_ERROR_NONE) {
        throw new RuntimeException('Invalid JSON from Oracle Bridge: '.substr($body, 0, 200));
    }

    return $data;
}

/**
 * Send a request using the socket path from a connection handle.
 * Populates lastError on the handle if the daemon responds with an error.
 */
function _oci_conn_send(OCI8BridgeConnection $connection, array $payload): array
{
    try {
        $response = _oci_send($connection->socketPath, $payload);
    } catch (RuntimeException $e) {
        $connection->lastError = ['code' => 0, 'message' => $e->getMessage(), 'offset' => 0, 'sqltext' => ''];

        throw $e;
    }

    $connection->lastError = $response['ok'] ? null : $response;

    return $response;
}

// ---------------------------------------------------------------------------
// Miscellaneous internal helpers
// ---------------------------------------------------------------------------

function _oci_format_row(array $row, int $mode): array
{
    $out = [];

    $i = 0;

    foreach ($row as $lk => $v) {
        if ($mode & OCI_ASSOC) {
            $out[$lk] = $v;
        }

        if ($mode & OCI_NUM) {
            $out[$i] = $v;
        }

        $i++;
    }

    return $out;
}

function _oci_resolve_bindings(array $bindings): array
{
    foreach ($bindings as $key => $value) {
        if ($value['value'] instanceof OCI8BridgeLob) {
            $bindings[$key]['value'] = $value['value']->value;
        }
    }

    return $bindings;
}

// ---------------------------------------------------------------------------
// OCI8 polyfill functions
// ---------------------------------------------------------------------------

// ---- Connection ----

if (! function_exists('oci_connect')) {
    /**
     * Spawns a private daemon process for this connection.
     * The daemon connects to Oracle immediately during startup.
     */
    function oci_connect(
        string $username,
        string $password,
        ?string $connection_string = null,
        ?string $character_set = null,
        ?int $session_mode = null
    ): OCI8BridgeConnection|false {
        $cfg = _oci_config();

        $socketPath = sprintf(
            '%s/oci-%d-%s.sock',
            rtrim($cfg['socket_dir'], '/'),
            getmypid(),
            substr(str_replace('.', '', (string) microtime(true)), -8)
        );

        try {
            $pid = _oci_spawn_daemon($socketPath);
        } catch (RuntimeException $e) {
            OCI8BridgeConnection::$lastConnectionError = ['code' => 0, 'message' => $e->getMessage()];

            return false;
        }

        $response = _oci_send($socketPath, [
            'command' => 'connect',
            'username' => $username,
            'password' => $password,
            'connection_string' => $connection_string,
            'character_set' => $character_set,
            'session_mode' => $session_mode,
        ]);

        if (! $response['ok']) {
            OCI8BridgeConnection::$lastConnectionError = $response;

            return false;
        }

        return new OCI8BridgeConnection($socketPath, $pid);
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
    ): OCI8BridgeConnection|false {
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
    ): OCI8BridgeConnection|false {
        return oci_connect($username, $password, $connection_string, $encoding, $session_mode);
    }
}

if (! function_exists('oci_close')) {
    /**
     * Sends a shutdown command to the daemon, waits for it to exit cleanly,
     * then removes the socket file.
     */
    function oci_close(mixed $connection): bool
    {
        if (! $connection instanceof OCI8BridgeConnection) {
            return false;
        }

        try {
            _oci_send($connection->socketPath, ['command' => 'shutdown']);
        } catch (RuntimeException) {
        }

        if (file_exists($connection->socketPath)) {
            unlink($connection->socketPath);
        }

        return true;
    }
}

if (! function_exists('oci_server_version')) {
    function oci_server_version(mixed $connection): string|false
    {
        try {
            return _oci_conn_send($connection, ['command' => 'server_version'])['result'];
        } catch (RuntimeException) {
            return false;
        }
    }
}

// ---- Statement lifecycle ----

if (! function_exists('oci_parse')) {
    function oci_parse(mixed $connection, string $sql): OCI8BridgeStatement|false
    {
        return new OCI8BridgeStatement($connection, $sql);
    }
}

if (! function_exists('oci_free_statement')) {
    function oci_free_statement(mixed $statement): bool
    {
        $statement->rows = null;
        $statement->columns = null;
        $statement->bindings = [];
        $statement->executed = false;

        return true;
    }
}

if (! function_exists('oci_statement_type')) {
    function oci_statement_type(mixed $statement): string|false
    {
        $kw = strtoupper(strtok(ltrim($statement->sql), " \t\n\r("));

        $map = [
            'SELECT' => 'SELECT', 'INSERT' => 'INSERT',
            'UPDATE' => 'UPDATE', 'DELETE' => 'DELETE',
            'CREATE' => 'CREATE', 'DROP' => 'DROP',
            'ALTER' => 'ALTER',  'BEGIN' => 'BEGIN',
            'DECLARE' => 'DECLARE', 'CALL' => 'CALL',
            'TRUNCATE' => 'TRUNCATE', 'MERGE' => 'MERGE',
        ];

        return $map[$kw] ?? 'UNKNOWN';
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
        $statement->bindings[] = compact('bv_name', 'variable', 'maxlength', 'type');

        return true;
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
        $statement->arrayBindings[] = compact(
            'name', 'var_array', 'max_array_size', 'max_item_size', 'type'
        );

        return true;
    }
}

// ---- Execution ----

if (! function_exists('oci_execute')) {
    function oci_execute(mixed $statement, int $mode = OCI_COMMIT_ON_SUCCESS): bool
    {
        $connection = $statement->connection;

        try {
            $response = _oci_conn_send($connection, [
                'command' => oci_statement_type($statement) === 'SELECT' ? 'select' : 'query',
                'sql' => $statement->sql,
                'bindings' => _oci_resolve_bindings($statement->bindings),
                'arrayBindings' => _oci_resolve_bindings($statement->arrayBindings),
                'mode' => $mode,
            ]);
        } catch (RuntimeException $e) {
            $error = ['code' => 0, 'message' => $e->getMessage(), 'offset' => 0, 'sqltext' => $statement->sql];

            $statement->lastError = $connection->lastError = $error;

            return false;
        }

        if (! $response['ok']) {
            $statement->lastError = $connection->lastError;

            return false;
        }

        $statement->executed = true;
        $statement->lastError = null;
        $statement->rows = $response['rows'] ?? [];
        $statement->columns = $response['columns'] ?? [];
        $statement->affectedRows = $response['affected'] ?? 0;
        $statement->rowIndex = 0;

        return true;
    }
}

// ---- Fetching ----

if (! function_exists('oci_fetch_array')) {
    function oci_fetch_array(mixed $statement, int $mode = OCI_BOTH + OCI_RETURN_NULLS): array|false
    {
        if (! $statement instanceof OCI8BridgeStatement) {
            return false;
        }

        if ($statement->rows === null || $statement->rowIndex >= count($statement->rows)) {
            return false;
        }

        return _oci_format_row($statement->rows[$statement->rowIndex++], $mode);
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

        if ($row === false) {
            return false;
        }

        return (object) array_change_key_case($row);
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
        if (! $statement instanceof OCI8BridgeStatement || $statement->rows === null) {
            return false;
        }

        $rows = array_slice($statement->rows, $skip, $maxrows === -1 ? null : $maxrows);
        $count = count($rows);

        if ($flags & OCI_FETCHSTATEMENT_BY_ROW) {
            $rf = $flags & ~OCI_FETCHSTATEMENT_BY_ROW;

            $output = array_map(fn ($r) => _oci_format_row($r, $rf ?: OCI_ASSOC), $rows);
        } else {
            $output = [];

            foreach ($rows as $row) {
                foreach ($row as $col => $val) {
                    $output[$col][] = $val;
                }
            }
        }

        return $count;
    }
}

// ---- Row / field metadata ----

if (! function_exists('oci_num_rows')) {
    function oci_num_rows(mixed $statement): int|false
    {
        if (! $statement instanceof OCI8BridgeStatement) {
            return false;
        }

        return $statement->affectedRows;
    }
}

if (! function_exists('oci_num_fields')) {
    function oci_num_fields(mixed $statement): int|false
    {
        if (! $statement instanceof OCI8BridgeStatement) {
            return false;
        }
        if ($statement->columns !== null) {
            return count($statement->columns);
        }
        if (! empty($statement->rows)) {
            return count($statement->rows[0]);
        }

        return 0;
    }
}

if (! function_exists('oci_field_name')) {
    function oci_field_name(mixed $statement, int|string $column): string|false
    {
        if (! $statement instanceof OCI8BridgeStatement) {
            return false;
        }
        $keys = $statement->columns !== null
            ? array_keys($statement->columns)
            : (! empty($statement->rows) ? array_keys($statement->rows[0]) : []);
        $key = is_int($column)
            ? ($keys[$column - 1] ?? null)
            : (in_array(strtolower($column), $keys, true) ? strtolower($column) : null);

        return $key !== null ? strtoupper($key) : false;
    }
}

if (! function_exists('oci_field_type')) {
    function oci_field_type(mixed $statement, int|string $column): string|int|false
    {
        if (! $statement instanceof OCI8BridgeStatement || empty($statement->columns)) {
            return 'VARCHAR2';
        }
        $keys = array_keys($statement->columns);
        $key = is_int($column) ? ($keys[$column - 1] ?? null) : strtolower($column);

        return ($key && isset($statement->columns[$key])) ? $statement->columns[$key]['type'] : 'VARCHAR2';
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
        if (! $statement instanceof OCI8BridgeStatement || empty($statement->columns)) {
            return 255;
        }
        $keys = array_keys($statement->columns);
        $key = is_int($column) ? ($keys[$column - 1] ?? null) : strtolower($column);

        return ($key && isset($statement->columns[$key])) ? (int) $statement->columns[$key]['size'] : 255;
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
        if (! $connection instanceof OCI8BridgeConnection) {
            return false;
        }

        try {
            return _oci_conn_send($connection, ['command' => 'commit'])['ok'];
        } catch (RuntimeException) {
            return false;
        }
    }
}

if (! function_exists('oci_rollback')) {
    function oci_rollback(mixed $connection): bool
    {
        if (! $connection instanceof OCI8BridgeConnection) {
            return false;
        }

        try {
            return _oci_conn_send($connection, ['command' => 'rollback'])['ok'];
        } catch (RuntimeException) {
            return false;
        }
    }
}

// ---- Error handling ----

if (! function_exists('oci_error')) {
    function oci_error(mixed $resource = null): array|false
    {
        if ($resource instanceof OCI8BridgeStatement && $resource->lastError !== null) {
            return $resource->lastError;
        }

        if ($resource instanceof OCI8BridgeConnection && $resource->lastError !== null) {
            return $resource->lastError;
        }

        return OCI8BridgeConnection::$lastConnectionError;
    }
}

// ---- LOB descriptors ----

if (! function_exists('oci_new_descriptor')) {
    function oci_new_descriptor(mixed $connection, int $type = OCI_DTYPE_LOB): OCI8BridgeLob|false
    {
        if (! $connection instanceof OCI8BridgeConnection) {
            return false;
        }

        return new OCI8BridgeLob($type);
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
