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
 * <p>Implements Simple Query and Extended Query protocols — enough for psql,
 * DBeaver, JDBC drivers, and Python/pandas to connect and execute SQL queries.
 *
 * <p>Protocol flow:
 * <ol>
 *   <li>SSL negotiation → reject with 'N'</li>
 *   <li>StartupMessage → AuthenticationOk + ParameterStatus + BackendKeyData + ReadyForQuery</li>
 *   <li>Query ('Q') → delegate to QueryHandler → RowDescription + DataRow* + CommandComplete + ReadyForQuery</li>
 *   <li>Extended: Parse/Bind/Describe/Execute/Sync</li>
 *   <li>Terminate ('X') → close connection</li>
 * </ol>
 *
 * <p><b>Internal API</b> — subject to change without notice.
 */
public final class PgWireServer {

    // PostgreSQL type OIDs
    public static final int OID_BOOL      = 16;
    public static final int OID_INT2      = 21;
    public static final int OID_INT4      = 23;
    public static final int OID_INT8      = 20;
    public static final int OID_FLOAT4    = 700;
    public static final int OID_FLOAT8    = 701;
    public static final int OID_TEXT      = 25;
    public static final int OID_NAME      = 19;
    public static final int OID_OID       = 26;
    public static final int OID_VARCHAR   = 1043;
    public static final int OID_DATE      = 1082;
    public static final int OID_TIMESTAMP = 1114;
    public static final int OID_TIMESTAMPTZ = 1184;
    public static final int OID_NUMERIC   = 1700;
    public static final int OID_UUID      = 2950;
    public static final int OID_JSONB     = 3802;

    /**
     * Callback interface for query execution.
     */
    @FunctionalInterface
    public interface QueryHandler {
        QueryResult execute(String sql);
    }

    /**
     * Factory for per-connection QueryHandler instances.
     * Each connection gets its own handler with independent transaction state.
     */
    @FunctionalInterface
    public interface QueryHandlerFactory {
        QueryHandler create();
    }

    /**
     * Result of a SQL query execution.
     *
     * <p>{@link #rows} holds the text-formatted values used directly by the
     * text wire path. {@link #typedRows} (optional, may be {@code null})
     * holds the parallel raw Java values (Long, Double, BigDecimal, String,
     * Boolean, UUID, …) used by the binary wire path. When the client
     * requests binary output via Bind, the wire layer reads from
     * {@code typedRows[r][c]} and passes it to {@link PgBinaryCodec#encode}.
     */
    public static final class QueryResult {
        public final String[] columnNames;
        public final int[] columnOids;
        public final String[][] rows;
        /** Parallel typed values for binary output. {@code null} when only text was prepared. */
        public final Object[][] typedRows;
        public final String commandTag;
        public final String error;
        /** Transaction status: 'I'=idle, 'T'=in transaction, 'E'=error in transaction. */
        public char txStatus;

        /** Successful result with text rows only — binary output not supported for this result. */
        public QueryResult(String[] columnNames, int[] columnOids,
                           String[][] rows, String commandTag) {
            this(columnNames, columnOids, rows, null, commandTag);
        }

        /**
         * Successful result with both text rows and typed rows. {@code typedRows}
         * is the parallel raw-value matrix used when the client requests binary
         * output for one or more columns.
         */
        public QueryResult(String[] columnNames, int[] columnOids,
                           String[][] rows, Object[][] typedRows, String commandTag) {
            this.columnNames = columnNames;
            this.columnOids = columnOids;
            this.rows = rows;
            this.typedRows = typedRows;
            this.commandTag = commandTag;
            this.error = null;
            this.txStatus = 'I';
        }

        /** Error result. */
        public QueryResult(String error) {
            this.columnNames = null;
            this.columnOids = null;
            this.rows = null;
            this.typedRows = null;
            this.commandTag = null;
            this.error = error;
            this.txStatus = 'I';
        }

        /** Empty result (e.g., SET command). */
        public static QueryResult empty(String commandTag) {
            return new QueryResult(new String[0], new int[0], new String[0][], commandTag);
        }

        /** Set transaction status and return this for chaining. */
        public QueryResult withTxStatus(char status) {
            this.txStatus = status;
            return this;
        }
    }

    /** Maximum message body size: 64 MB. */
    private static final int MAX_MESSAGE_LENGTH = 64 * 1024 * 1024;

    /** Maximum SSL/startup negotiation rounds before closing connection. */
    private static final int MAX_STARTUP_ROUNDS = 5;

    private final int port;
    private final String host;
    private final QueryHandlerFactory handlerFactory;
    private ServerSocket serverSocket;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread acceptThread;

    /** Create a server with a single shared handler (backward compatible). */
    public PgWireServer(int port, QueryHandler handler) {
        this(port, "127.0.0.1", () -> handler);
    }

    /** Create a server with a single shared handler bound to a specific host (backward compatible). */
    public PgWireServer(int port, String host, QueryHandler handler) {
        this(port, host, () -> handler);
    }

    /**
     * Create a server with a per-connection handler factory.
     * Each connection creates a fresh handler with independent transaction state.
     */
    public PgWireServer(int port, String host, QueryHandlerFactory factory) {
        this.port = port;
        this.host = host;
        this.handlerFactory = factory;
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

            if (!handleStartup(in, out)) {
                return;
            }

            // Per-connection handler (independent transaction state per connection)
            QueryHandler handler = handlerFactory.create();

            // Per-connection extended query protocol state
            String[] lastParsedSql = new String[]{""};
            String[][] lastBoundParams = new String[][]{null};
            // Param type OIDs from the most recent Parse — needed at Bind
            // time to dispatch binary decoders. Length-0 array means "no
            // type hints sent" (the client expects server-side inference).
            int[][] paramTypeOIDs = new int[][]{new int[0]};
            QueryResult[] cachedResult = new QueryResult[]{null};
            // Per-portal result-column format codes (Bind → Describe/Execute). One of:
            //   null       — client sent 0 codes (all text, default)
            //   length 1   — broadcast to every result column
            //   length N   — one per result column
            // Step W1: captured & validated but only format 0 (text) is honoured;
            // any binary request triggers ErrorResponse 0A000 at send time.
            int[][] resultFormatCodes = new int[][]{null};
            // Transaction status for ReadyForQuery: 'I'=idle, 'T'=in tx, 'E'=error in tx
            char[] txStatus = new char[]{'I'};

            while (running.get() && !client.isClosed()) {
                int msgType = in.read();
                if (msgType == -1) break; // EOF

                int msgLen = in.readInt();
                if (msgLen < 4 || msgLen > MAX_MESSAGE_LENGTH) {
                    sendError(out, "FATAL", "08P01", "Invalid message length: " + msgLen);
                    return;
                }
                byte[] body = new byte[msgLen - 4];
                in.readFully(body);

                switch (msgType) {
                    case 'Q' -> handleQuery(body, out, txStatus, handler);
                    case 'X' -> { return; } // Terminate
                    case 'P' -> handleParse(body, out, lastParsedSql, paramTypeOIDs);
                    case 'B' -> handleBind(body, out, lastBoundParams, resultFormatCodes, paramTypeOIDs);
                    case 'D' -> handleDescribe(body, out, lastParsedSql, lastBoundParams, cachedResult, resultFormatCodes, handler);
                    case 'E' -> handleExecuteMsg(body, out, lastParsedSql, lastBoundParams, cachedResult, resultFormatCodes, txStatus, handler);
                    case 'S' -> handleSync(out, txStatus, resultFormatCodes);
                    case 'C' -> handleClose(body, out);
                    default -> {
                        sendError(out, "ERROR", "XX000", "Unsupported message type: " + (char) msgType);
                        sendReadyForQuery(out, txStatus[0]);
                    }
                }
            }
        } catch (IOException e) {
            // Client disconnected — normal
        } catch (Exception e) {
            System.err.println("PgWire connection error: " + e.getMessage());
        }
    }

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
                byte[] params = new byte[len - 8];
                in.readFully(params);

                // AuthenticationOk — trust mode
                out.writeByte('R');
                out.writeInt(8);
                out.writeInt(0);
                out.flush();

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
                out.writeInt(12);
                out.writeInt(1);
                out.writeInt(0);

                sendReadyForQuery(out, 'I');
                return true;
            }

            if (code == 80877102) {
                // CancelRequest — ignore
                byte[] rest = new byte[len - 8];
                in.readFully(rest);
                return false;
            }

            return false;
        }
        return false;
    }

    // ========================================================================
    // Simple Query protocol
    // ========================================================================

    private void handleQuery(byte[] body, DataOutputStream out, char[] txStatus,
                             QueryHandler handler) throws IOException {
        String sql = new String(body, 0, body.length - 1, StandardCharsets.UTF_8).trim();

        if (sql.isEmpty()) {
            sendEmptyQueryResponse(out);
            sendReadyForQuery(out, txStatus[0]);
            return;
        }

        String[] statements = splitStatements(sql);

        for (String stmt : statements) {
            stmt = stmt.trim();
            if (stmt.isEmpty()) continue;

            try {
                QueryResult result = handler.execute(stmt);

                txStatus[0] = result.txStatus;

                if (result.error != null) {
                    sendError(out, "ERROR", "42000", result.error);
                } else if (result.columnNames.length == 0) {
                    sendCommandComplete(out, result.commandTag);
                } else {
                    // Simple Query ('Q') always uses text format — no Bind step.
                    sendRowDescription(out, result.columnNames, result.columnOids, null);
                    for (String[] row : result.rows) {
                        sendDataRow(out, row, null);
                    }
                    sendCommandComplete(out, result.commandTag);
                }
            } catch (Exception e) {
                sendError(out, "ERROR", "XX000",
                        e.getMessage() != null ? e.getMessage() : e.getClass().getName());
            }
        }

        sendReadyForQuery(out, txStatus[0]);
    }

    /**
     * Split SQL on semicolons, respecting single-quoted strings, double-quoted identifiers,
     * dollar-quoted strings ($$ and $tag$), and -- / /* * / comments.
     */
    private static String[] splitStatements(String sql) {
        if (!sql.contains(";")) {
            return new String[]{sql};
        }
        List<String> stmts = new ArrayList<>();
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inDollarQuote = false;
        String dollarTag = null;
        int start = 0;
        int len = sql.length();

        for (int i = 0; i < len; i++) {
            char c = sql.charAt(i);

            if (!inSingleQuote && !inDoubleQuote) {
                if (inDollarQuote) {
                    if (c == '$' && i + dollarTag.length() <= len
                            && sql.substring(i, i + dollarTag.length()).equals(dollarTag)) {
                        i += dollarTag.length() - 1;
                        inDollarQuote = false;
                        dollarTag = null;
                    }
                    continue;
                }
                // Check for dollar-quote start: $$ or $tag$
                if (c == '$') {
                    int tagEnd = i + 1;
                    while (tagEnd < len && (Character.isLetterOrDigit(sql.charAt(tagEnd))
                            || sql.charAt(tagEnd) == '_')) tagEnd++;
                    if (tagEnd < len && sql.charAt(tagEnd) == '$') {
                        dollarTag = sql.substring(i, tagEnd + 1);
                        i = tagEnd;
                        inDollarQuote = true;
                        continue;
                    }
                }
            }

            // Single-quoted string
            if (c == '\'' && !inDoubleQuote && !inDollarQuote) {
                if (inSingleQuote && i + 1 < len && sql.charAt(i + 1) == '\'') {
                    i++; // escaped ''
                } else {
                    inSingleQuote = !inSingleQuote;
                }
                continue;
            }

            // Double-quoted identifier
            if (c == '"' && !inSingleQuote && !inDollarQuote) {
                inDoubleQuote = !inDoubleQuote;
                continue;
            }

            // Line comment: -- to end of line
            if (c == '-' && i + 1 < len && sql.charAt(i + 1) == '-'
                    && !inSingleQuote && !inDoubleQuote && !inDollarQuote) {
                while (i < len && sql.charAt(i) != '\n') i++;
                continue;
            }

            // Block comment: /* ... */
            if (c == '/' && i + 1 < len && sql.charAt(i + 1) == '*'
                    && !inSingleQuote && !inDoubleQuote && !inDollarQuote) {
                i += 2;
                while (i + 1 < len && !(sql.charAt(i) == '*' && sql.charAt(i + 1) == '/')) i++;
                i++; // skip closing /
                continue;
            }

            if (c == ';' && !inSingleQuote && !inDoubleQuote && !inDollarQuote) {
                String part = sql.substring(start, i).trim();
                if (!part.isEmpty()) stmts.add(part);
                start = i + 1;
            }
        }

        if (start < len) {
            String part = sql.substring(start).trim();
            if (!part.isEmpty()) stmts.add(part);
        }
        return stmts.isEmpty() ? new String[]{sql} : stmts.toArray(new String[0]);
    }

    // ========================================================================
    // Extended Query protocol
    // ========================================================================

    private void handleParse(byte[] body, DataOutputStream out,
                             String[] lastParsedSql, int[][] paramTypeOIDs) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(body);
        String stmtName = readCString(buf);
        String query = readCString(buf);
        lastParsedSql[0] = query;

        // Param type OIDs declared by the client. A 0 in any slot means
        // "unspecified" — the server is free to infer the type. Stratum
        // doesn't do inference for binary inputs, so an unspecified type
        // combined with a binary format request at Bind time is rejected
        // with 0A000 (W3).
        short numParamOids = buf.getShort();
        int[] oids = new int[numParamOids];
        for (int i = 0; i < numParamOids; i++) oids[i] = buf.getInt();
        paramTypeOIDs[0] = oids;

        // ParseComplete
        out.writeByte('1');
        out.writeInt(4);
        out.flush();
    }

    private void handleBind(byte[] body, DataOutputStream out,
                            String[][] lastBoundParams,
                            int[][] resultFormatCodes,
                            int[][] paramTypeOIDs) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(body);
        String portalName = readCString(buf);
        String stmtName = readCString(buf);

        // Per-param format codes. Three valid shapes from the client:
        //   0 codes  → all params are text
        //   1 code   → broadcast to every param
        //   N codes  → one per param
        short numParamFormats = buf.getShort();
        int[] paramFormats = new int[numParamFormats];
        for (int i = 0; i < numParamFormats; i++) {
            paramFormats[i] = buf.getShort() & 0xffff;
        }

        short numParams = buf.getShort();
        int[] paramOids = paramTypeOIDs[0];

        // Validate every param's (format, type-OID) up-front before we read
        // any bytes. Refusing late means we'd have to drain the rest of the
        // message; refusing early keeps the protocol predictable.
        for (int i = 0; i < numParams; i++) {
            int fmt = paramFormatFor(paramFormats, i);
            if (fmt == 0) continue;                              // text — always fine
            if (fmt != 1) {
                sendError(out, "ERROR", "0A000",
                    "Unknown wire format code " + fmt + " for param $" + (i + 1) + ".");
                lastBoundParams[0] = null;
                resultFormatCodes[0] = null;
                return;
            }
            int oid = (i < paramOids.length) ? paramOids[i] : 0;
            if (oid == 0) {
                sendError(out, "ERROR", "0A000",
                    "Binary input for param $" + (i + 1)
                        + " requires a type OID at Parse time, but the client did not declare one. "
                        + "Either send the param as text, or declare its type in the Parse message.");
                lastBoundParams[0] = null;
                resultFormatCodes[0] = null;
                return;
            }
            if (!PgBinaryCodec.supportsBinaryInput(oid)) {
                sendError(out, "ERROR", "0A000",
                    "Binary input format not supported for param $" + (i + 1)
                        + " (OID=" + oid + "). Send this parameter as text.");
                lastBoundParams[0] = null;
                resultFormatCodes[0] = null;
                return;
            }
        }

        String[] params = new String[numParams];
        for (int i = 0; i < numParams; i++) {
            int paramLen = buf.getInt();
            if (paramLen == -1) {
                params[i] = null; // SQL NULL
            } else {
                byte[] paramBytes = new byte[paramLen];
                buf.get(paramBytes);
                int fmt = paramFormatFor(paramFormats, i);
                if (fmt == 0) {
                    params[i] = new String(paramBytes, StandardCharsets.UTF_8);
                } else {
                    // Binary — decode through PgBinaryCodec, then render as a
                    // text fragment for the existing substituteParams pass.
                    int oid = paramOids[i];
                    try {
                        Object decoded = PgBinaryCodec.decode(oid, paramBytes);
                        params[i] = binaryToSqlText(oid, decoded);
                    } catch (Exception ex) {
                        sendError(out, "ERROR", "22P03",
                            "Could not decode binary input for param $" + (i + 1)
                                + " (OID=" + oid + "): "
                                + (ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName()));
                        lastBoundParams[0] = null;
                        resultFormatCodes[0] = null;
                        return;
                    }
                }
            }
        }
        lastBoundParams[0] = params;

        // Per-result-column format codes (same 0/1/N shape as params). We
        // store the raw int[] and resolve per column at Describe/Execute
        // time, since the column count isn't known until then.
        int[] resultFormats = null;
        if (buf.hasRemaining()) {
            short numResultFormats = buf.getShort();
            if (numResultFormats > 0) {
                resultFormats = new int[numResultFormats];
                for (int i = 0; i < numResultFormats; i++) {
                    resultFormats[i] = buf.getShort() & 0xffff;
                }
            }
        }
        resultFormatCodes[0] = resultFormats;

        // BindComplete
        out.writeByte('2');
        out.writeInt(4);
        out.flush();
    }

    /** Resolve the format code for a single param given the raw code array from Bind. */
    private static int paramFormatFor(int[] formatCodes, int idx) {
        if (formatCodes.length == 0) return 0;
        if (formatCodes.length == 1) return formatCodes[0];
        return idx < formatCodes.length ? formatCodes[idx] : 0;
    }

    /**
     * Convert a binary-decoded parameter value into the SQL text fragment
     * that {@link #substituteParams} will splice into the prepared SQL.
     *
     * <p>The existing text-substitution path treats values as either
     * "numeric" (inlined verbatim) or "string" (single-quoted, with escapes).
     * Binary-decoded values must round-trip through that same path, so we
     * pick the formatting carefully:
     *
     * <ul>
     *   <li>Numeric types render as their decimal representation so
     *       {@code isNumeric()} catches them.</li>
     *   <li>DATE/TIMESTAMP render as ISO-8601 strings, so the text path
     *       quotes them and JSqlParser parses them as date/timestamp
     *       literals against the column type.</li>
     *   <li>UUID / JSONB / TEXT render as plain strings (quoted by
     *       {@code substituteParams}).</li>
     *   <li>BOOL renders as {@code TRUE}/{@code FALSE} — uppercase so the
     *       text path treats them as bare SQL keywords (not numeric and not
     *       string-quoted; but it's also fine if they were quoted, since
     *       most parsers accept {@code 'true'} as a bool literal).</li>
     * </ul>
     */
    private static String binaryToSqlText(int oid, Object v) {
        if (v == null) return null;
        return switch (oid) {
            case OID_BOOL -> ((Boolean) v) ? "true" : "false";
            case OID_DATE ->
                java.time.LocalDate.ofEpochDay(((Number) v).longValue()).toString();
            case OID_TIMESTAMP, OID_TIMESTAMPTZ ->
                formatTimestampMicros(((Number) v).longValue());
            case OID_NUMERIC -> ((java.math.BigDecimal) v).toPlainString();
            default -> v.toString();
        };
    }

    /**
     * Format Unix-epoch microseconds as a SQL TIMESTAMP literal body
     * (no surrounding quotes; the substitute pass adds them).
     * Format: {@code YYYY-MM-DD HH:MM:SS[.ffffff]} (space separator, no
     * timezone) — matches what JSqlParser accepts for TIMESTAMP literals.
     */
    private static String formatTimestampMicros(long micros) {
        long secs = Math.floorDiv(micros, 1_000_000L);
        long us   = Math.floorMod(micros, 1_000_000L);
        java.time.LocalDateTime ldt =
            java.time.LocalDateTime.ofEpochSecond(secs, (int) (us * 1000L), java.time.ZoneOffset.UTC);
        return ldt.toLocalDate() + " " + ldt.toLocalTime();
    }

    /**
     * Materialize a per-column format-code array of length {@code numCols} from
     * the raw codes stored at Bind time. Handles the 0 / 1 / N shapes. Returns
     * null when every resolved code is 0 (caller treats null as "all text").
     */
    private static int[] resolveResultFormats(int[] raw, int numCols) {
        if (raw == null || raw.length == 0) return null;
        int[] out = new int[numCols];
        if (raw.length == 1) {
            int v = raw[0];
            if (v == 0) return null;
            for (int i = 0; i < numCols; i++) out[i] = v;
            return out;
        }
        boolean anyBinary = false;
        for (int i = 0; i < numCols; i++) {
            int v = i < raw.length ? raw[i] : 0;
            out[i] = v;
            if (v != 0) anyBinary = true;
        }
        return anyBinary ? out : null;
    }

    /** Returns the index of the first binary-formatted column, or -1 if all text. */
    private static int firstBinaryColumn(int[] formats) {
        if (formats == null) return -1;
        for (int i = 0; i < formats.length; i++) {
            if (formats[i] != 0) return i;
        }
        return -1;
    }

    /**
     * Return the index of the first column whose requested format isn't
     * supported by {@link PgBinaryCodec}, or -1 if every requested format
     * is either text (0) or a binary format we can encode for the given
     * OID. Used at Describe/Execute time to fail with 0A000 before we
     * commit to writing rows we can't actually produce.
     */
    private static int firstUnsupportedFormatColumn(int[] formats, int[] oids, Object[][] typedRows) {
        if (formats == null) return -1;
        for (int i = 0; i < formats.length; i++) {
            if (formats[i] == 0) continue;
            if (formats[i] != 1) return i;             // unknown format code
            if (!PgBinaryCodec.supportsBinaryOutput(oids[i])) return i;
            if (typedRows == null) return i;           // no typed values available
        }
        return -1;
    }

    /**
     * Build a one-line reason explaining why the given column's format
     * request can't be honoured. Returns a stable string for the
     * ErrorResponse message field.
     */
    private static String unsupportedFormatReason(int col, int[] formats, int[] oids,
                                                  String[] names, Object[][] typedRows) {
        int fmt = formats[col];
        int oid = oids[col];
        String name = names[col];
        if (fmt != 1) {
            return "Unknown wire format code " + fmt + " for column "
                + (col + 1) + " (" + name + ").";
        }
        if (!PgBinaryCodec.supportsBinaryOutput(oid)) {
            return "Binary result format not supported for column "
                + (col + 1) + " (" + name + ", OID=" + oid
                + "). Configure your client to request text format for this column.";
        }
        if (typedRows == null) {
            return "Binary result format requested for column "
                + (col + 1) + " (" + name + ", OID=" + oid
                + ") but the server only produced text values for this result. "
                + "This is an internal limitation — please report it.";
        }
        return "Binary result format unavailable for column " + (col + 1) + " (" + name + ").";
    }

    private void handleDescribe(byte[] body, DataOutputStream out,
                                String[] lastParsedSql, String[][] lastBoundParams,
                                QueryResult[] cachedResult,
                                int[][] resultFormatCodes,
                                QueryHandler handler) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(body);
        byte descType = buf.get(); // 'S' = statement, 'P' = portal
        String name = readCString(buf);

        String sql = lastParsedSql[0];
        if (sql == null || sql.isEmpty()) {
            out.writeByte('n'); // NoData
            out.writeInt(4);
            out.flush();
            return;
        }

        // DML must not execute at Describe time — only send ParameterDescription + NoData
        String sqlUpper = sql.trim().toUpperCase();
        boolean isDml = sqlUpper.startsWith("INSERT") || sqlUpper.startsWith("UPDATE")
                     || sqlUpper.startsWith("DELETE");

        if (descType == 'S') {
            // Count $N parameters for ParameterDescription (all types reported as unspecified=0)
            int numParams = 0;
            for (int i = 0; i < sql.length() - 1; i++) {
                if (sql.charAt(i) == '$' && Character.isDigit(sql.charAt(i + 1))) {
                    numParams++;
                    i++;
                    while (i + 1 < sql.length() && Character.isDigit(sql.charAt(i + 1))) i++;
                }
            }
            out.writeByte('t'); // ParameterDescription
            out.writeInt(4 + 2 + numParams * 4);
            out.writeShort(numParams);
            for (int i = 0; i < numParams; i++) out.writeInt(0); // unspecified type
            out.flush();
        }

        if (isDml) {
            out.writeByte('n'); // NoData
            out.writeInt(4);
            out.flush();
            return;
        }

        // SELECT — execute now to get column metadata; cache result so Execute doesn't re-run it
        String[] params = lastBoundParams[0];
        String execSql = (params != null && params.length > 0) ? substituteParams(sql, params) : sql;

        try {
            QueryResult result = handler.execute(execSql);
            cachedResult[0] = result;
            if (result.error != null || result.columnNames == null || result.columnNames.length == 0) {
                out.writeByte('n'); // NoData
                out.writeInt(4);
            } else {
                int[] formats = resolveResultFormats(resultFormatCodes[0], result.columnNames.length);
                int badCol = firstUnsupportedFormatColumn(formats, result.columnOids, result.typedRows);
                if (badCol >= 0) {
                    // Refuse early — better than advertising a format in
                    // RowDescription that we can't actually honour at Execute.
                    cachedResult[0] = null;
                    sendError(out, "ERROR", "0A000",
                        unsupportedFormatReason(badCol, formats, result.columnOids,
                                                result.columnNames, result.typedRows));
                } else {
                    sendRowDescription(out, result.columnNames, result.columnOids, formats);
                }
            }
        } catch (Exception e) {
            cachedResult[0] = null;
            out.writeByte('n'); // NoData
            out.writeInt(4);
        }
        out.flush();
    }

    private void handleExecuteMsg(byte[] body, DataOutputStream out,
                                  String[] lastParsedSql, String[][] lastBoundParams,
                                  QueryResult[] cachedResult,
                                  int[][] resultFormatCodes,
                                  char[] txStatus,
                                  QueryHandler handler) throws IOException {
        // If Describe already ran and cached the result, reuse it (avoid double-execution)
        QueryResult result = cachedResult[0];
        boolean describedAlready = (result != null);
        cachedResult[0] = null;

        if (result == null) {
            // Describe wasn't called — execute now
            String sql = lastParsedSql[0];
            String[] params = lastBoundParams[0];
            if (sql != null && !sql.isEmpty() && params != null && params.length > 0) {
                sql = substituteParams(sql, params);
            }
            if (sql != null && !sql.isEmpty()) {
                try {
                    result = handler.execute(sql);
                } catch (Exception e) {
                    sendError(out, "ERROR", "XX000",
                            e.getMessage() != null ? e.getMessage() : e.getClass().getName());
                    lastBoundParams[0] = null;
                    out.flush();
                    return;
                }
            }
        }

        if (result != null) {
            txStatus[0] = result.txStatus;

            if (result.error != null) {
                sendError(out, "ERROR", "42000", result.error);
            } else if (result.columnNames.length == 0) {
                sendCommandComplete(out, result.commandTag);
            } else {
                int[] formats = resolveResultFormats(resultFormatCodes[0], result.columnNames.length);
                int badCol = firstUnsupportedFormatColumn(formats, result.columnOids, result.typedRows);
                if (badCol >= 0) {
                    sendError(out, "ERROR", "0A000",
                        unsupportedFormatReason(badCol, formats, result.columnOids,
                                                result.columnNames, result.typedRows));
                } else {
                    // Describe already sent RowDescription — don't repeat it
                    if (!describedAlready) {
                        sendRowDescription(out, result.columnNames, result.columnOids, formats);
                    }
                    sendDataRows(out, result, formats);
                    sendCommandComplete(out, result.commandTag);
                }
            }
        } else {
            sendCommandComplete(out, "SELECT 0");
        }
        lastBoundParams[0] = null;
        resultFormatCodes[0] = null;
        out.flush();
    }

    /**
     * Stream all DataRow messages for a result, choosing text or binary
     * per column based on {@code formats}. When all formats are 0 this
     * is equivalent to the old text-only loop.
     */
    private void sendDataRows(DataOutputStream out, QueryResult result, int[] formats) throws IOException {
        if (formats == null || firstBinaryColumn(formats) < 0) {
            for (String[] row : result.rows) sendDataRow(out, row, null);
            return;
        }
        int nRows = result.rows.length;
        for (int r = 0; r < nRows; r++) {
            String[] textRow = result.rows[r];
            Object[] typedRow = result.typedRows[r];
            sendDataRowMixed(out, textRow, typedRow, result.columnOids, formats);
        }
    }

    private void handleSync(DataOutputStream out, char[] txStatus,
                            int[][] resultFormatCodes) throws IOException {
        // Sync ends the current Extended-Query cycle — clear any stray format
        // state so the next Bind starts from a clean slate.
        resultFormatCodes[0] = null;
        sendReadyForQuery(out, txStatus[0]);
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

    /**
     * Emit a RowDescription. {@code formats} may be null (all text) or a
     * fully resolved per-column array (length == names.length) — see
     * {@link #resolveResultFormats}. The caller is responsible for refusing
     * binary requests when no encoder is wired up.
     */
    private void sendRowDescription(DataOutputStream out, String[] names, int[] oids,
                                    int[] formats) throws IOException {
        int bodyLen = 2; // field count (int16)
        byte[][] nameBytes = new byte[names.length][];
        for (int i = 0; i < names.length; i++) {
            nameBytes[i] = names[i].getBytes(StandardCharsets.UTF_8);
            bodyLen += nameBytes[i].length + 1  // name + null terminator
                    + 4   // table OID
                    + 2   // column attr number
                    + 4   // type OID
                    + 2   // type size
                    + 4   // type modifier
                    + 2;  // format code
        }

        out.writeByte('T');
        out.writeInt(4 + bodyLen);
        out.writeShort(names.length);

        for (int i = 0; i < names.length; i++) {
            out.write(nameBytes[i]);
            out.writeByte(0);               // null terminator
            out.writeInt(0);                // table OID (none)
            out.writeShort(0);              // column attr number
            out.writeInt(oids[i]);          // type OID
            out.writeShort(typeSize(oids[i])); // type size
            out.writeInt(-1);               // type modifier
            int fmt = (formats == null) ? 0 : formats[i];
            out.writeShort(fmt);            // format code (0=text, 1=binary)
        }
        out.flush();
    }

    /**
     * Emit a DataRow that mixes text- and binary-formatted columns. The
     * caller must have validated the format requests via
     * {@link #firstUnsupportedFormatColumn} before calling.
     */
    private void sendDataRowMixed(DataOutputStream out,
                                  String[] textRow,
                                  Object[] typedRow,
                                  int[] oids,
                                  int[] formats) throws IOException {
        int n = formats.length;
        byte[][] payloads = new byte[n][];
        int bodyLen = 2; // column count int16
        for (int i = 0; i < n; i++) {
            if (formats[i] == 1) {
                Object v = typedRow == null ? null : typedRow[i];
                payloads[i] = (v == null) ? null : PgBinaryCodec.encode(oids[i], v);
            } else {
                String s = textRow[i];
                payloads[i] = (s == null) ? null : s.getBytes(StandardCharsets.UTF_8);
            }
            bodyLen += 4 + (payloads[i] == null ? 0 : payloads[i].length);
        }
        out.writeByte('D');
        out.writeInt(4 + bodyLen);
        out.writeShort(n);
        for (byte[] p : payloads) {
            if (p == null) {
                out.writeInt(-1);
            } else {
                out.writeInt(p.length);
                out.write(p);
            }
        }
        out.flush();
    }

    /**
     * Emit a DataRow. {@code formats} may be null (all text) or a fully
     * resolved per-column array. Step W1 only supports format 0 (text);
     * callers must refuse binary requests before reaching this method.
     */
    private void sendDataRow(DataOutputStream out, String[] values, int[] formats) throws IOException {
        int bodyLen = 2; // column count (int16)
        byte[][] valBytes = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                valBytes[i] = null;
                bodyLen += 4; // -1 sentinel for NULL
            } else {
                valBytes[i] = values[i].getBytes(StandardCharsets.UTF_8);
                bodyLen += 4 + valBytes[i].length;
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
        byte[] msgBytes = (message != null ? message : "Unknown error").getBytes(StandardCharsets.UTF_8);

        int bodyLen = 1 + sevBytes.length + 1
                    + 1 + codeBytes.length + 1
                    + 1 + msgBytes.length + 1
                    + 1; // terminator

        out.writeByte('E');
        out.writeInt(4 + bodyLen);
        out.writeByte('S'); out.write(sevBytes); out.writeByte(0);
        out.writeByte('C'); out.write(codeBytes); out.writeByte(0);
        out.writeByte('M'); out.write(msgBytes); out.writeByte(0);
        out.writeByte(0);   // terminator
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
            case OID_BOOL -> 1;
            case OID_INT2 -> 2;
            case OID_INT4, OID_OID -> 4;
            case OID_INT8 -> 8;
            case OID_FLOAT4 -> 4;
            case OID_FLOAT8 -> 8;
            case OID_DATE -> 4;
            case OID_TIMESTAMP, OID_TIMESTAMPTZ -> 8;
            case OID_UUID -> 16;
            default -> -1; // variable length
        };
    }

    private static String substituteParams(String sql, String[] params) {
        StringBuilder sb = new StringBuilder(sql.length() + params.length * 10);
        int i = 0;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            if (c == '\'') {
                sb.append(c); i++;
                while (i < sql.length()) {
                    char q = sql.charAt(i); sb.append(q); i++;
                    if (q == '\'') {
                        if (i < sql.length() && sql.charAt(i) == '\'') { sb.append('\''); i++; }
                        else break;
                    }
                }
            } else if (c == '$' && i + 1 < sql.length() && Character.isDigit(sql.charAt(i + 1))) {
                int start = i + 1, end = start;
                while (end < sql.length() && Character.isDigit(sql.charAt(end))) end++;
                int paramIdx = Integer.parseInt(sql.substring(start, end)) - 1;
                if (paramIdx >= 0 && paramIdx < params.length) {
                    String val = params[paramIdx];
                    if (val == null) { sb.append("NULL"); }
                    else if (isNumeric(val)) { sb.append(val); }
                    else {
                        sb.append('\'');
                        for (int j = 0; j < val.length(); j++) {
                            char vc = val.charAt(j);
                            if (vc == '\'') sb.append('\'');
                            sb.append(vc);
                        }
                        sb.append('\'');
                    }
                } else { sb.append(sql, i, end); }
                i = end;
            } else { sb.append(c); i++; }
        }
        return sb.toString();
    }

    private static boolean isNumeric(String s) {
        if (s.isEmpty()) return false;
        int start = 0;
        if (s.charAt(0) == '-' || s.charAt(0) == '+') start = 1;
        if (start >= s.length()) return false;
        boolean hasDot = false;
        for (int i = start; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '.') { if (hasDot) return false; hasDot = true; }
            else if (!Character.isDigit(c)) return false;
        }
        return true;
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
