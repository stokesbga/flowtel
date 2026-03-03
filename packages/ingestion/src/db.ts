import pg from "pg";

export async function initDb(pool: pg.Pool): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS ingested_events (
        id UUID PRIMARY KEY,
        session_id UUID,
        user_id UUID,
        type VARCHAR(50),
        name VARCHAR(100),
        properties JSONB,
        timestamp BIGINT NOT NULL,
        session_browser VARCHAR(50)
      );
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_events_timestamp ON ingested_events (timestamp);
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS ingestion_progress (
        segment_id INTEGER PRIMARY KEY,
        cursor_value TEXT NOT NULL,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    // Optimize PostgreSQL for bulk inserts
    await client.query(`ALTER DATABASE ingestion SET synchronous_commit = off`);

    console.log("Database initialized");
  } finally {
    client.release();
  }
}

function normalizeTimestamp(ts: any): number {
  if (typeof ts === "number") return ts;
  if (typeof ts === "string") return new Date(ts).getTime();
  return 0;
}

export async function insertEventsBatch(pool: pg.Pool, events: any[]): Promise<number> {
  if (events.length === 0) return 0;

  // Split into chunks of 500 to stay within PG parameter limits (500 * 8 = 4000 params)
  const CHUNK_SIZE = 500;
  let totalInserted = 0;

  for (let start = 0; start < events.length; start += CHUNK_SIZE) {
    const chunk = events.slice(start, start + CHUNK_SIZE);
    const inserted = await insertChunk(pool, chunk);
    totalInserted += inserted;
  }

  return totalInserted;
}

async function insertChunk(pool: pg.Pool, events: any[]): Promise<number> {
  const values: any[] = [];
  const placeholders: string[] = [];

  for (let i = 0; i < events.length; i++) {
    const e = events[i];
    const offset = i * 8;
    placeholders.push(
      `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8})`
    );
    values.push(
      e.id,
      e.sessionId || null,
      e.userId || null,
      e.type || null,
      e.name || null,
      JSON.stringify(e.properties || {}),
      normalizeTimestamp(e.timestamp),
      e.session?.browser || null,
    );
  }

  const query = `
    INSERT INTO ingested_events (id, session_id, user_id, type, name, properties, timestamp, session_browser)
    VALUES ${placeholders.join(", ")}
    ON CONFLICT (id) DO NOTHING
  `;

  const result = await pool.query(query, values);
  return result.rowCount || 0;
}

export interface ProgressState {
  completedSegments: Set<number>;
  segmentCursors: Map<number, string>;
}

export async function getProgress(pool: pg.Pool): Promise<ProgressState> {
  const client = await pool.connect();
  try {
    const result = await client.query("SELECT segment_id, cursor_value FROM ingestion_progress");
    const completedSegments = new Set<number>();
    const segmentCursors = new Map<number, string>();

    for (const row of result.rows) {
      if (row.cursor_value === "COMPLETED") {
        completedSegments.add(row.segment_id);
      } else {
        segmentCursors.set(row.segment_id, row.cursor_value);
      }
    }

    return { completedSegments, segmentCursors };
  } finally {
    client.release();
  }
}

export async function saveProgress(pool: pg.Pool, segmentId: number, cursor: string): Promise<void> {
  await pool.query(
    `INSERT INTO ingestion_progress (segment_id, cursor_value, updated_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (segment_id) DO UPDATE SET cursor_value = $2, updated_at = NOW()`,
    [segmentId, cursor]
  );
}

export async function markSegmentComplete(pool: pg.Pool, segmentId: number): Promise<void> {
  await saveProgress(pool, segmentId, "COMPLETED");
}

export async function getEventCount(pool: pg.Pool): Promise<number> {
  const result = await pool.query("SELECT COUNT(*) as count FROM ingested_events");
  return parseInt(result.rows[0].count);
}
