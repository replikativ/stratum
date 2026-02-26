package stratum.internal;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Minimal PostgreSQL wire protocol (v3) server for Stratum.
 *
 * <p>Implements the Simple Query protocol only — enough for psql, DBeaver,
 * JDBC drivers, and Python/pandas to connect and execute SQL queries.
 *
 * <p>Protocol flow:
 * <ol>
 *   <li>SSL negotiation → reject with 'N'</li>
 *   <li>StartupMessage → AuthenticationOk + ParameterStatus + BackendKeyData + ReadyForQuery</li>
 *   <li>Query ('Q') → delegate to QueryHandler → RowDescription + DataRow* + CommandComplete + ReadyForQuery</li>
 *   <li>Terminate ('X') → close connection</li>
 * </ol>
 *
 * <p><b>Internal API</b> — subject to change without notice.
 */
public final class PgWireServer {

    // PostgreSQL type OIDs
    public static final int OID_INT8 = 20;       // bigint
    public static final int OID_FLOAT8 = 701;     // double precision
    public static final int OID_TEXT = 25;         // text
    public static final int OID_DATE = 1082;       // date
    public static final int OID_TIMESTAMP = 1114;  // timestamp

    /**
     * Callback interface for query execution.
     */
    @FunctionalInterface
    public interface QueryHandler {
        QueryResult execute(String sql);
    }

    /**
     * Result of a SQL query execution.
     */
    public static final class QueryResult {
        public final String[] columnNames;
        public final int[] columnOids;
        public final String[][] rows;
        public final String commandTag;
        public final String error;

        /** Successful result with rows. */
        public QueryResult(String[] columnNames, int[] columnOids,
                           String[][] rows, String commandTag) {
            this.columnNames = columnNames;
            this.columnOids = columnOids;
            this.rows = rows;
            this.commandTag = commandTag;
            this.error = null;
        }

        /** Error result. */
        public QueryResult(String error) {
            this.columnNames = null;
            this.columnOids = null;
            this.rows = null;
            this.commandTag = null;
            this.error = error;
        }

        /** Empty result (e.g., SET command). */
        public static QueryResult empty(String commandTag) {
            return new QueryResult(new String[0], new int[0], new String[0][], commandTag);
        }
    }

    /** Maximum message body size: 64 MB. */
    private static final int MAX_MESSAGE_LENGTH = 64 * 1024 * 1024;

    /** Maximum SSL/startup negotiation rounds before closing connection. */
    private static final int MAX_STARTUP_ROUNDS = 5;

    private final int port;
    private final String host;
    private final QueryHandler handler;
    private ServerSocket serverSocket;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread acceptThread;

    /**
     * Create a server bound to localhost (127.0.0.1) on the given port.
     */
    public PgWireServer(int port, QueryHandler handler) {
        this(port, "127.0.0.1", handler);
    }

    /**
     * Create a server bound to the specified host and port.
     * Use "0.0.0.0" to listen on all interfaces.
     */
    public PgWireServer(int port, String host, QueryHandler handler) {
        this.port = port;
        this.host = host;
        this.handler = handler;
    }

    /**
     * Start the server. Non-blocking — spawns a daemon thread to accept connections.
     */
    public void start() throws IOException {
        serverSocket = new ServerSocket(port, 50, InetAddress.getByName(host));
        serverSocket.setReuseAddress(true);
        running.set(true);

        acceptThread = Thread.ofVirtual().name("pgwire-accept").start(() -> {
            while (running.get()) {
                try {
                    Socket client = serverSocket.accept();
                    Thread.ofVirtual().name("pgwire-conn-" + client.getRemoteSocketAddress())
                        .start(() -> handleConnection(client));
                } catch (IOException e) {
                    if (running.get()) {
                        System.err.println("PgWire accept error: " + e.getMessage());
                    }
                }
            }
        });

        System.out.println("PgWire server listening on " + host + ":" + port);
    }

    /**
     * Graceful shutdown.
     */
    public void stop() {
        running.set(false);
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignored) {}
        System.out.println("PgWire server stopped.");
    }

    public int getPort() {
        return serverSocket != null ? serverSocket.getLocalPort() : port;
    }

    // ========================================================================
    // Connection handling
    // ========================================================================

    private void handleConnection(Socket client) {
        try (client;
             DataInputStream in = new DataInputStream(new BufferedInputStream(client.getInputStream()));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(client.getOutputStream()))) {

            // Phase 1: Startup negotiation
            if (!handleStartup(in, out)) {
                return;
            }

            // Per-connection state for the extended query protocol (unnamed prepared statement)
            String[] lastParsedSql = new String[]{""};

            // Phase 2: Query loop
            while (running.get() && !client.isClosed()) {
                int msgType = in.read();
                if (msgType == -1) break; // EOF

                int msgLen = in.readInt();
                if (msgLen < 4 || msgLen > MAX_MESSAGE_LENGTH) {
                    sendError(out, "FATAL", "08P01",
                            "Invalid message length: " + msgLen);
                    return;
                }
                byte[] body = new byte[msgLen - 4];
                in.readFully(body);

                switch (msgType) {
                    case 'Q' -> handleQuery(body, out);
                    case 'X' -> { return; } // Terminate
                    case 'P' -> handleParse(body, out, lastParsedSql);   // Extended protocol: Parse
                    case 'B' -> handleBind(body, out);     // Extended protocol: Bind
                    case 'D' -> handleDescribe(body, out); // Extended protocol: Describe
                    case 'E' -> handleExecuteMsg(body, out, lastParsedSql); // Extended protocol: Execute
                    case 'S' -> handleSync(out);           // Extended protocol: Sync
                    case 'C' -> handleClose(body, out);    // Extended protocol: Close
                    default -> {
                        // Unknown message type — send error and continue
                        sendError(out, "ERROR", "XX000",
                                "Unsupported message type: " + (char) msgType);
                        sendReadyForQuery(out, 'I');
                    }
                }
            }
        } catch (IOException e) {
            // Client disconnected — normal
        } catch (Exception e) {
            System.err.println("PgWire connection error: " + e.getMessage());
        }
    }

    /**
     * Handle the startup sequence: SSL negotiation and StartupMessage.
     * Returns true if startup succeeded, false to close connection.
     */
    private boolean handleStartup(DataInputStream in, DataOutputStream out) throws IOException {
        for (int round = 0; round < MAX_STARTUP_ROUNDS; round++) {
            int len = in.readInt();
            int code = in.readInt();

            if (code == 80877103) {
                // SSLRequest — reject with 'N'
                out.writeByte('N');
                out.flush();
                continue;
            }

            if (code == 196608) {
                // StartupMessage v3.0
                // Read parameters (key=value pairs, null-terminated)
                byte[] params = new byte[len - 8];
                in.readFully(params);
                // We ignore user/database for now

                // AuthenticationOk — trust mode (no password required).
                // Security relies on binding to localhost by default.
                out.writeByte('R');
                out.writeInt(8); // length
                out.writeInt(0); // auth ok
                out.flush();

                // ParameterStatus messages (required by many clients)
                sendParameterStatus(out, "server_version", "15.0");
                sendParameterStatus(out, "server_encoding", "UTF8");
                sendParameterStatus(out, "client_encoding", "UTF8");
                sendParameterStatus(out, "DateStyle", "ISO, MDY");
                sendParameterStatus(out, "integer_datetimes", "on");
                sendParameterStatus(out, "standard_conforming_strings", "on");
                sendParameterStatus(out, "TimeZone", "UTC");
                sendParameterStatus(out, "is_superuser", "on");

                // BackendKeyData
                out.writeByte('K');
                out.writeInt(12); // length
                out.writeInt(1);  // process ID
                out.writeInt(0);  // secret key

                // ReadyForQuery
                sendReadyForQuery(out, 'I');
                return true;
            }

            if (code == 80877102) {
                // CancelRequest — ignore
                byte[] rest = new byte[len - 8];
                in.readFully(rest);
                return false;
            }

            // Unknown startup code
            return false;
        }
        // Exceeded MAX_STARTUP_ROUNDS — likely a misbehaving client
        return false;
    }

    // ========================================================================
    // Simple Query protocol
    // ========================================================================

    private void handleQuery(byte[] body, DataOutputStream out) throws IOException {
        // Body is null-terminated SQL string
        String sql = new String(body, 0, body.length - 1, StandardCharsets.UTF_8).trim();

        if (sql.isEmpty()) {
            sendEmptyQueryResponse(out);
            sendReadyForQuery(out, 'I');
            return;
        }

        // Handle multiple statements separated by semicolons
        // (psql sends "BEGIN;" type things, and \d sends multi-statement queries)
        String[] statements = splitStatements(sql);

        for (String stmt : statements) {
            stmt = stmt.trim();
            if (stmt.isEmpty()) continue;

            try {
                QueryResult result = handler.execute(stmt);

                if (result.error != null) {
                    sendError(out, "ERROR", "42000", result.error);
                } else if (result.columnNames.length == 0) {
                    // Empty result (SET, etc.)
                    sendCommandComplete(out, result.commandTag);
                } else {
                    // Send RowDescription
                    sendRowDescription(out, result.columnNames, result.columnOids);

                    // Send DataRows
                    for (String[] row : result.rows) {
                        sendDataRow(out, row);
                    }

                    // Send CommandComplete
                    sendCommandComplete(out, result.commandTag);
                }
            } catch (Exception e) {
                sendError(out, "ERROR", "XX000", e.getMessage());
            }
        }

        sendReadyForQuery(out, 'I');
    }

    /**
     * Split SQL text on semicolons, respecting single-quoted string literals.
     * Semicolons inside '...' are not treated as statement separators.
     */
    private String[] splitStatements(String sql) {
        if (!sql.contains(";")) {
            return new String[]{sql};
        }
        List<String> stmts = new ArrayList<>();
        boolean inQuote = false;
        int start = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'') {
                // Handle SQL escaped quotes ('') — skip both, don't toggle
                if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                    i++; // skip the second quote
                } else {
                    inQuote = !inQuote;
                }
            } else if (c == ';' && !inQuote) {
                String part = sql.substring(start, i).trim();
                if (!part.isEmpty()) {
                    stmts.add(part);
                }
                start = i + 1;
            }
        }
        // Trailing statement after last semicolon
        if (start < sql.length()) {
            String part = sql.substring(start).trim();
            if (!part.isEmpty()) {
                stmts.add(part);
            }
        }
        return stmts.isEmpty() ? new String[]{sql} : stmts.toArray(new String[0]);
    }

    // ========================================================================
    // Extended Query protocol (minimal stubs)
    // ========================================================================

    private void handleParse(byte[] body, DataOutputStream out, String[] lastParsedSql) throws IOException {
        // Parse message: name(str) + query(str) + numParams(int16) + paramOids...
        ByteBuffer buf = ByteBuffer.wrap(body);
        String stmtName = readCString(buf);
        String query = readCString(buf);
        lastParsedSql[0] = query;

        // ParseComplete
        out.writeByte('1');
        out.writeInt(4);
        out.flush();
    }

    private void handleBind(byte[] body, DataOutputStream out) throws IOException {
        // Bind message — we ignore params for now (unnamed portal)
        // BindComplete
        out.writeByte('2');
        out.writeInt(4);
        out.flush();
    }

    private void handleDescribe(byte[] body, DataOutputStream out) throws IOException {
        // Describe message — send NoData for now
        // We'll send real RowDescription during Execute
        out.writeByte('n'); // NoData
        out.writeInt(4);
        out.flush();
    }

    private void handleExecuteMsg(byte[] body, DataOutputStream out, String[] lastParsedSql) throws IOException {
        // Execute the last parsed query via the handler
        String sql = lastParsedSql[0];
        if (sql != null && !sql.isEmpty()) {
            try {
                QueryResult result = handler.execute(sql);

                if (result.error != null) {
                    sendError(out, "ERROR", "42000", result.error);
                } else if (result.columnNames.length == 0) {
                    sendCommandComplete(out, result.commandTag);
                } else {
                    sendRowDescription(out, result.columnNames, result.columnOids);
                    for (String[] row : result.rows) {
                        sendDataRow(out, row);
                    }
                    sendCommandComplete(out, result.commandTag);
                }
            } catch (Exception e) {
                sendError(out, "ERROR", "XX000", e.getMessage());
            }
        } else {
            sendCommandComplete(out, "SELECT 0");
        }
        out.flush();
    }

    private void handleSync(DataOutputStream out) throws IOException {
        sendReadyForQuery(out, 'I');
    }

    private void handleClose(byte[] body, DataOutputStream out) throws IOException {
        // CloseComplete
        out.writeByte('3');
        out.writeInt(4);
        out.flush();
    }

    // ========================================================================
    // Protocol message builders
    // ========================================================================

    private void sendParameterStatus(DataOutputStream out, String name, String value) throws IOException {
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
        int len = 4 + nameBytes.length + 1 + valueBytes.length + 1;

        out.writeByte('S');
        out.writeInt(len);
        out.write(nameBytes);
        out.writeByte(0);
        out.write(valueBytes);
        out.writeByte(0);
        out.flush();
    }

    private void sendReadyForQuery(DataOutputStream out, char status) throws IOException {
        out.writeByte('Z');
        out.writeInt(5);
        out.writeByte(status);
        out.flush();
    }

    private void sendRowDescription(DataOutputStream out, String[] names, int[] oids) throws IOException {
        // Calculate total message length
        int bodyLen = 2; // field count (int16)
        byte[][] nameBytes = new byte[names.length][];
        for (int i = 0; i < names.length; i++) {
            nameBytes[i] = names[i].getBytes(StandardCharsets.UTF_8);
            bodyLen += nameBytes[i].length + 1  // name + null
                    + 4   // table OID
                    + 2   // column attr number
                    + 4   // type OID
                    + 2   // type size
                    + 4   // type modifier
                    + 2;  // format code (text=0)
        }

        out.writeByte('T');
        out.writeInt(4 + bodyLen);
        out.writeShort(names.length);

        for (int i = 0; i < names.length; i++) {
            out.write(nameBytes[i]);
            out.writeByte(0);         // null terminator
            out.writeInt(0);          // table OID
            out.writeShort(0);        // column attr number
            out.writeInt(oids[i]);    // type OID
            out.writeShort(typeSize(oids[i])); // type size
            out.writeInt(-1);         // type modifier
            out.writeShort(0);        // format code (text)
        }
        out.flush();
    }

    private void sendDataRow(DataOutputStream out, String[] values) throws IOException {
        int bodyLen = 2; // column count (int16)
        byte[][] valBytes = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                valBytes[i] = null;
                bodyLen += 4; // -1 for NULL
            } else {
                valBytes[i] = values[i].getBytes(StandardCharsets.UTF_8);
                bodyLen += 4 + valBytes[i].length; // length + data
            }
        }

        out.writeByte('D');
        out.writeInt(4 + bodyLen);
        out.writeShort(values.length);

        for (byte[] vb : valBytes) {
            if (vb == null) {
                out.writeInt(-1); // NULL
            } else {
                out.writeInt(vb.length);
                out.write(vb);
            }
        }
        out.flush();
    }

    private void sendCommandComplete(DataOutputStream out, String tag) throws IOException {
        byte[] tagBytes = tag.getBytes(StandardCharsets.UTF_8);
        out.writeByte('C');
        out.writeInt(4 + tagBytes.length + 1);
        out.write(tagBytes);
        out.writeByte(0);
        out.flush();
    }

    private void sendError(DataOutputStream out, String severity, String code, String message) throws IOException {
        byte[] sevBytes = severity.getBytes(StandardCharsets.UTF_8);
        byte[] codeBytes = code.getBytes(StandardCharsets.UTF_8);
        byte[] msgBytes = message.getBytes(StandardCharsets.UTF_8);

        int bodyLen = 1 + sevBytes.length + 1
                    + 1 + codeBytes.length + 1
                    + 1 + msgBytes.length + 1
                    + 1; // terminator

        out.writeByte('E');
        out.writeInt(4 + bodyLen);

        // Severity field
        out.writeByte('S');
        out.write(sevBytes);
        out.writeByte(0);

        // Code field
        out.writeByte('C');
        out.write(codeBytes);
        out.writeByte(0);

        // Message field
        out.writeByte('M');
        out.write(msgBytes);
        out.writeByte(0);

        // Terminator
        out.writeByte(0);
        out.flush();
    }

    private void sendEmptyQueryResponse(DataOutputStream out) throws IOException {
        out.writeByte('I');
        out.writeInt(4);
        out.flush();
    }

    // ========================================================================
    // Helpers
    // ========================================================================

    private static short typeSize(int oid) {
        return switch (oid) {
            case OID_INT8 -> 8;
            case OID_FLOAT8 -> 8;
            case OID_TEXT -> -1;   // variable length
            case OID_DATE -> 4;
            case OID_TIMESTAMP -> 8;
            default -> -1;
        };
    }

    private static String readCString(ByteBuffer buf) {
        int start = buf.position();
        while (buf.get() != 0) {}
        int end = buf.position() - 1;
        byte[] bytes = new byte[end - start];
        buf.position(start);
        buf.get(bytes);
        buf.get(); // skip null terminator
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
