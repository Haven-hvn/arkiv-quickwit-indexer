//! SQLite-backed bridge state: cursor, reorg buffer, dedup window, and
//! pending delete tasks. Everything needed to recover from a crash lives
//! here (§6.4 of the design).
//!
//! Hidden contract: all methods run on the caller's thread. The store is
//! wrapped in a `tokio::sync::Mutex`-free way by only being touched from
//! the single control loop task; there is exactly one writer by design.

use std::path::Path;

use rusqlite::{Connection, OptionalExtension, params};

use crate::error::Result;

/// A `(block_number, block_hash)` pair from the rolling reorg buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BufferedBlock {
    pub block_number: u64,
    /// 32-byte hash, `0x`-prefixed lowercase hex.
    pub block_hash: String,
}

/// A delete-task query persisted before submission so a crash between
/// "decide to delete" and "POST delete-task" cannot lose the retraction.
#[derive(Debug, Clone)]
pub struct PendingDelete {
    pub id: i64,
    pub query: String,
}

/// A record of one emitted document, kept for the dedup window and for
/// sampling audits.
#[derive(Debug, Clone)]
pub struct EmittedDoc {
    pub doc_id: String,
    pub entity_key: String,
    pub block_number: u64,
    pub code_hash: String,
    /// Byte length of the payload the hash covers — lets the audit scan
    /// fixed-length windows in O(n).
    pub payload_len: u64,
    pub emitted_count: u64,
}

pub struct StateStore {
    connection: Connection,
}

impl StateStore {
    /// Opens (creating if needed) the store at `path` and runs migrations.
    pub fn open(path: &Path) -> Result<Self> {
        let connection = Connection::open(path)?;
        Self::init(connection)
    }

    /// In-memory store for tests.
    #[cfg(test)]
    pub fn open_in_memory() -> Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(connection: Connection) -> Result<Self> {
        // WAL survives crashes without blocking readers; NORMAL sync is the
        // standard durability/throughput point for WAL.
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        connection.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS cursor (
              singleton     INTEGER PRIMARY KEY CHECK (singleton = 0),
              next_block    INTEGER NOT NULL,
              updated_at    INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS reorg_buffer (
              block_number  INTEGER PRIMARY KEY,
              block_hash    TEXT    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS emitted_dedup (
              doc_id        TEXT    PRIMARY KEY,
              entity_key    TEXT    NOT NULL,
              block_number  INTEGER NOT NULL,
              code_hash     TEXT    NOT NULL,
              payload_len   INTEGER NOT NULL DEFAULT 0,
              emitted_count INTEGER NOT NULL DEFAULT 1
            );
            CREATE INDEX IF NOT EXISTS emitted_dedup_by_block
              ON emitted_dedup(block_number);

            CREATE TABLE IF NOT EXISTS pending_deletes (
              id            INTEGER PRIMARY KEY AUTOINCREMENT,
              query         TEXT    NOT NULL,
              scheduled_at  INTEGER NOT NULL,
              submitted     INTEGER NOT NULL DEFAULT 0
            );
            "#,
        )?;
        Ok(Self { connection })
    }

    // ── Cursor ───────────────────────────────────────────────────────────

    /// Returns the next block to crawl, or `None` on first run.
    pub fn cursor(&self) -> Result<Option<u64>> {
        let next_block: Option<i64> = self
            .connection
            .query_row("SELECT next_block FROM cursor WHERE singleton = 0", [], |row| {
                row.get(0)
            })
            .optional()?;
        Ok(next_block.map(|block| block as u64))
    }

    pub fn set_cursor(&self, next_block: u64) -> Result<()> {
        self.connection.execute(
            "INSERT INTO cursor (singleton, next_block, updated_at)
             VALUES (0, ?1, unixepoch())
             ON CONFLICT(singleton) DO UPDATE SET next_block = ?1, updated_at = unixepoch()",
            params![next_block as i64],
        )?;
        Ok(())
    }

    // ── Reorg buffer ─────────────────────────────────────────────────────

    /// Records the observed hash for a block. Returns the previously stored
    /// hash if it differs (i.e. a reorg signal), else `None`.
    pub fn observe_block(&self, block_number: u64, block_hash: &str) -> Result<Option<String>> {
        let existing: Option<String> = self
            .connection
            .query_row(
                "SELECT block_hash FROM reorg_buffer WHERE block_number = ?1",
                params![block_number as i64],
                |row| row.get(0),
            )
            .optional()?;
        match existing {
            Some(stored_hash) if stored_hash != block_hash => Ok(Some(stored_hash)),
            Some(_) => Ok(None),
            None => {
                self.connection.execute(
                    "INSERT INTO reorg_buffer (block_number, block_hash) VALUES (?1, ?2)",
                    params![block_number as i64, block_hash],
                )?;
                Ok(None)
            }
        }
    }

    /// All buffered blocks, ascending.
    pub fn buffered_blocks(&self) -> Result<Vec<BufferedBlock>> {
        let mut statement = self
            .connection
            .prepare("SELECT block_number, block_hash FROM reorg_buffer ORDER BY block_number")?;
        let rows = statement.query_map([], |row| {
            Ok(BufferedBlock {
                block_number: row.get::<_, i64>(0)? as u64,
                block_hash: row.get(1)?,
            })
        })?;
        let mut blocks = Vec::new();
        for row in rows {
            blocks.push(row?);
        }
        Ok(blocks)
    }

    /// Keeps only the most recent `buffer_size` entries.
    pub fn prune_reorg_buffer(&self, buffer_size: u64) -> Result<()> {
        self.connection.execute(
            "DELETE FROM reorg_buffer WHERE block_number <= (
               SELECT MAX(block_number) FROM reorg_buffer
             ) - ?1",
            params![buffer_size as i64],
        )?;
        Ok(())
    }

    /// Drops buffered entries above the fork point (reorg rollback).
    pub fn truncate_reorg_buffer_after(&self, fork_point: u64) -> Result<()> {
        self.connection.execute(
            "DELETE FROM reorg_buffer WHERE block_number > ?1",
            params![fork_point as i64],
        )?;
        Ok(())
    }

    // ── Dedup window / emitted docs ──────────────────────────────────────

    /// Records an emitted doc. Returns the new emitted count (1 = first
    /// emission, >1 = re-emission that needs a dedup delete task).
    pub fn record_emitted_doc(
        &self,
        doc_id: &str,
        entity_key: &str,
        block_number: u64,
        code_hash: &str,
        payload_len: u64,
    ) -> Result<u64> {
        self.connection.execute(
            "INSERT INTO emitted_dedup (doc_id, entity_key, block_number, code_hash, payload_len)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(doc_id) DO UPDATE SET emitted_count = emitted_count + 1",
            params![doc_id, entity_key, block_number as i64, code_hash, payload_len as i64],
        )?;
        let count: i64 = self.connection.query_row(
            "SELECT emitted_count FROM emitted_dedup WHERE doc_id = ?1",
            params![doc_id],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    /// Entity keys emitted for blocks strictly above `fork_point` — the
    /// retraction set for a reorg.
    pub fn entity_keys_after(&self, fork_point: u64) -> Result<Vec<String>> {
        let mut statement = self.connection.prepare(
            "SELECT DISTINCT entity_key FROM emitted_dedup WHERE block_number > ?1",
        )?;
        let rows = statement.query_map(params![fork_point as i64], |row| row.get(0))?;
        let mut keys = Vec::new();
        for row in rows {
            keys.push(row?);
        }
        Ok(keys)
    }

    /// Drops dedup entries above the fork point (reorg rollback).
    pub fn prune_emitted_after(&self, fork_point: u64) -> Result<()> {
        self.connection.execute(
            "DELETE FROM emitted_dedup WHERE block_number > ?1",
            params![fork_point as i64],
        )?;
        Ok(())
    }

    /// Drops dedup entries older than the window.
    pub fn prune_emitted_before(&self, min_block: u64) -> Result<()> {
        self.connection.execute(
            "DELETE FROM emitted_dedup WHERE block_number < ?1",
            params![min_block as i64],
        )?;
        Ok(())
    }

    /// A pseudo-random sample of emitted docs for the audit pass. `seed`
    /// varies the sample between passes without needing an RNG in SQL.
    pub fn sample_emitted_docs(&self, sample_size: usize, seed: u64) -> Result<Vec<EmittedDoc>> {
        let mut statement = self.connection.prepare(
            "SELECT doc_id, entity_key, block_number, code_hash, payload_len, emitted_count
             FROM emitted_dedup
             ORDER BY ((rowid + ?2) * 2654435761) % 4294967296
             LIMIT ?1",
        )?;
        let rows = statement.query_map(params![sample_size as i64, seed as i64], |row| {
            Ok(EmittedDoc {
                doc_id: row.get(0)?,
                entity_key: row.get(1)?,
                block_number: row.get::<_, i64>(2)? as u64,
                code_hash: row.get(3)?,
                payload_len: row.get::<_, i64>(4)? as u64,
                emitted_count: row.get::<_, i64>(5)? as u64,
            })
        })?;
        let mut docs = Vec::new();
        for row in rows {
            docs.push(row?);
        }
        Ok(docs)
    }

    // ── Pending delete tasks ─────────────────────────────────────────────

    /// Persists a delete-task query for later (idempotent) submission.
    pub fn enqueue_delete(&self, query: &str) -> Result<()> {
        self.connection.execute(
            "INSERT INTO pending_deletes (query, scheduled_at) VALUES (?1, unixepoch())",
            params![query],
        )?;
        Ok(())
    }

    /// Unsubmitted delete tasks, oldest first.
    pub fn pending_deletes(&self) -> Result<Vec<PendingDelete>> {
        let mut statement = self.connection.prepare(
            "SELECT id, query FROM pending_deletes WHERE submitted = 0 ORDER BY id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(PendingDelete {
                id: row.get(0)?,
                query: row.get(1)?,
            })
        })?;
        let mut deletes = Vec::new();
        for row in rows {
            deletes.push(row?);
        }
        Ok(deletes)
    }

    pub fn mark_delete_submitted(&self, id: i64) -> Result<()> {
        self.connection.execute(
            "UPDATE pending_deletes SET submitted = 1 WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    /// Removes submitted rows older than the dedup window to bound growth.
    pub fn prune_submitted_deletes(&self) -> Result<()> {
        self.connection.execute(
            "DELETE FROM pending_deletes
             WHERE submitted = 1 AND scheduled_at < unixepoch() - 86400",
            [],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_roundtrip() {
        let store = StateStore::open_in_memory().unwrap();
        assert_eq!(store.cursor().unwrap(), None);
        store.set_cursor(42).unwrap();
        assert_eq!(store.cursor().unwrap(), Some(42));
        store.set_cursor(43).unwrap();
        assert_eq!(store.cursor().unwrap(), Some(43));
    }

    #[test]
    fn reorg_buffer_detects_hash_mismatch() {
        let store = StateStore::open_in_memory().unwrap();
        assert_eq!(store.observe_block(10, "0xaa").unwrap(), None);
        assert_eq!(store.observe_block(10, "0xaa").unwrap(), None);
        assert_eq!(store.observe_block(10, "0xbb").unwrap(), Some("0xaa".to_string()));
    }

    #[test]
    fn reorg_buffer_prune_keeps_recent() {
        let store = StateStore::open_in_memory().unwrap();
        for block in 1..=300u64 {
            store.observe_block(block, &format!("0x{block:x}")).unwrap();
        }
        store.prune_reorg_buffer(256).unwrap();
        let blocks = store.buffered_blocks().unwrap();
        assert_eq!(blocks.first().unwrap().block_number, 45);
        assert_eq!(blocks.last().unwrap().block_number, 300);
    }

    #[test]
    fn dedup_counts_re_emissions() {
        let store = StateStore::open_in_memory().unwrap();
        let first = store.record_emitted_doc("doc-1", "0xkey", 5, "0xhash", 11).unwrap();
        assert_eq!(first, 1);
        let second = store.record_emitted_doc("doc-1", "0xkey", 5, "0xhash", 11).unwrap();
        assert_eq!(second, 2);
    }

    #[test]
    fn retraction_set_and_rollback() {
        let store = StateStore::open_in_memory().unwrap();
        store.record_emitted_doc("doc-1", "0xkey1", 5, "0xh1", 4).unwrap();
        store.record_emitted_doc("doc-2", "0xkey2", 8, "0xh2", 4).unwrap();
        store.record_emitted_doc("doc-3", "0xkey2", 9, "0xh3", 4).unwrap();
        let keys = store.entity_keys_after(5).unwrap();
        assert_eq!(keys, vec!["0xkey2".to_string()]);
        store.prune_emitted_after(5).unwrap();
        assert!(store.entity_keys_after(0).unwrap().contains(&"0xkey1".to_string()));
        assert_eq!(store.entity_keys_after(5).unwrap().len(), 0);
    }

    #[test]
    fn pending_deletes_lifecycle() {
        let store = StateStore::open_in_memory().unwrap();
        store.enqueue_delete("entity_key:0xabc").unwrap();
        store.enqueue_delete("entity_key:0xdef").unwrap();
        let pending = store.pending_deletes().unwrap();
        assert_eq!(pending.len(), 2);
        store.mark_delete_submitted(pending[0].id).unwrap();
        assert_eq!(store.pending_deletes().unwrap().len(), 1);
    }

    #[test]
    fn audit_sample_is_bounded() {
        let store = StateStore::open_in_memory().unwrap();
        for doc_index in 0..100u64 {
            store
                .record_emitted_doc(
                    &format!("doc-{doc_index}"),
                    &format!("0xkey{doc_index}"),
                    doc_index,
                    "0xhash",
                    4,
                )
                .unwrap();
        }
        let sample = store.sample_emitted_docs(20, 7).unwrap();
        assert_eq!(sample.len(), 20);
        let other_sample = store.sample_emitted_docs(20, 8).unwrap();
        // Different seeds should (almost surely) give different orderings.
        assert_ne!(
            sample.iter().map(|doc| doc.doc_id.clone()).collect::<Vec<_>>(),
            other_sample.iter().map(|doc| doc.doc_id.clone()).collect::<Vec<_>>()
        );
    }
}
