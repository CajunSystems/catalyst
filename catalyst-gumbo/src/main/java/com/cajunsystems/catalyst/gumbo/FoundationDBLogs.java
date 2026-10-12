package com.cajunsystems.catalyst.gumbo;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.cajunsystems.catalyst.events.EventCodec;
import com.cajunsystems.gumbo.persistence.FoundationDBPersistenceAdapter;
import com.cajunsystems.gumbo.sequencer.FoundationDBSequencer;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Everything that names a FoundationDB type, kept out of {@link GumboEventLog} itself.
 *
 * <p>{@code fdb-java} is an optional dependency — it carries a native library that an app on the
 * file-backed log should not inherit — so the one class every app loads must not reference it. The
 * JVM resolves this class only when {@link GumboEventLog#foundationDB} is called.
 */
final class FoundationDBLogs {

    /** The API version Gumbo's FoundationDB adapter and sequencer select; one per JVM, so it must match. */
    private static final int FDB_API_VERSION = 730;

    private FoundationDBLogs() {}

    /**
     * Opens a multi-writer log on {@code root}: Gumbo's FoundationDB adapter for storage <em>and</em>
     * its FoundationDB sequencer for the global seqnum, over one shared connection. Both halves are
     * needed — Gumbo reports {@code multiWriter} only for that pairing, since the default sequencer is
     * a per-process counter that no storage fence can make safe.
     *
     * <p>The connection is opened here and owned by the returned log, which closes it after the
     * service. Gumbo's service never opens or closes the sequencer, and neither component closes a
     * {@code Database} it was handed, so nothing else would.
     */
    static GumboEventLog open(String clusterFile, String root, EventCodec codec) {
        Database db = clusterFile == null
                ? FDB.selectAPIVersion(FDB_API_VERSION).open()
                : FDB.selectAPIVersion(FDB_API_VERSION).open(clusterFile);
        try {
            FoundationDBSequencer sequencer = new FoundationDBSequencer(db, root);
            sequencer.open();
            return GumboEventLog.open(new FoundationDBPersistenceAdapter(db, root), sequencer, codec, db::close);
        } catch (IOException e) {
            db.close();
            throw new UncheckedIOException("Failed to open FoundationDB-backed Gumbo log at root '" + root + "'", e);
        } catch (RuntimeException | Error e) {
            db.close();
            throw e;
        }
    }
}
