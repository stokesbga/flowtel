import pg from "pg";
import { fetchEventsPage, createSyntheticCursor, RateLimitError, CursorExpiredError } from "./api.js";
import { initDb, insertEventsBatch, getProgress, saveProgress, getEventCount, markSegmentComplete } from "./db.js";

const API_BASE_URL = process.env.API_BASE_URL || "http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com";
const API_KEY = process.env.TARGET_API_KEY || "";
const PAGE_SIZE = 5000;
const TOTAL_EVENTS = 3_000_000;

// Timestamp range discovered via API exploration
const TS_NEWEST = 1769542000000;
const TS_OLDEST = 1766900000000;

const RATE_LIMIT = 10;
const RATE_WINDOW_MS = 61_000;

function normalizeTimestamp(ts: any): number {
  if (typeof ts === "number") return ts;
  if (typeof ts === "string") return new Date(ts).getTime();
  return 0;
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

interface Segment {
  id: number;
  tsHigh: number;
  tsLow: number;
  cursor: string | null;
  done: boolean;
  pagesProcessed: number;
}

function createSegments(count: number): Segment[] {
  const range = TS_NEWEST - TS_OLDEST;
  const segmentSize = Math.ceil(range / count);
  const segments: Segment[] = [];

  for (let i = 0; i < count; i++) {
    const tsHigh = TS_NEWEST - i * segmentSize;
    const tsLow = Math.max(TS_OLDEST, TS_NEWEST - (i + 1) * segmentSize);
    segments.push({
      id: i,
      tsHigh,
      tsLow,
      cursor: createSyntheticCursor(tsHigh),
      done: false,
      pagesProcessed: 0,
    });
  }

  return segments;
}

async function fetchAndInsert(
  pool: pg.Pool,
  segment: Segment,
): Promise<{ inserted: number; rateLimit?: { remaining: number; reset: number } }> {
  const result = await fetchEventsPage(API_BASE_URL, API_KEY, segment.cursor, PAGE_SIZE);

  if (!result.data || result.data.length === 0) {
    segment.done = true;
    return { inserted: 0, rateLimit: result.rateLimit };
  }

  const inserted = await insertEventsBatch(pool, result.data);
  segment.pagesProcessed++;

  const lastEvent = result.data[result.data.length - 1];
  const lastTs = normalizeTimestamp(lastEvent.timestamp);

  if (lastTs <= segment.tsLow || !result.pagination.hasMore) {
    segment.done = true;
  } else {
    segment.cursor = result.pagination.nextCursor;
  }

  return { inserted, rateLimit: result.rateLimit };
}

async function main() {
  console.log("============================================");
  console.log("DataSync Ingestion - Starting");
  console.log("============================================");

  if (!API_KEY) {
    console.error("ERROR: TARGET_API_KEY environment variable is required");
    process.exit(1);
  }

  const dbUrl = process.env.DATABASE_URL || "postgresql://postgres:postgres@postgres:5432/ingestion";
  const pool = new pg.Pool({ connectionString: dbUrl, max: 20 });

  await initDb(pool);

  const existingCount = await getEventCount(pool);
  console.log(`Existing events in database: ${existingCount}`);

  if (existingCount >= TOTAL_EVENTS) {
    console.log("ingestion complete");
    await pool.end();
    return;
  }

  const progress = await getProgress(pool);

  // 10 segments = 10 concurrent cursors, one per rate limit slot
  const NUM_SEGMENTS = 10;
  const segments = createSegments(NUM_SEGMENTS);

  for (const seg of segments) {
    if (progress.completedSegments.has(seg.id)) {
      seg.done = true;
    } else if (progress.segmentCursors.has(seg.id)) {
      seg.cursor = progress.segmentCursors.get(seg.id)!;
    }
  }

  const activeCount = segments.filter((s) => !s.done).length;
  console.log(`Segments: ${NUM_SEGMENTS} total, ${activeCount} active`);
  console.log(`Rate: ${RATE_LIMIT} req/min × ${PAGE_SIZE} events = ~${(RATE_LIMIT * PAGE_SIZE).toLocaleString()}/min`);

  const startTime = Date.now();
  let totalFetched = existingCount;
  let windowNum = 0;

  while (true) {
    const pending = segments.filter((s) => !s.done);
    if (pending.length === 0) break;

    windowNum++;
    const windowStart = Date.now();

    // Allocate requests across pending segments (round-robin)
    const tasks: Array<{ seg: Segment; promise: Promise<{ inserted: number; rateLimit?: { remaining: number; reset: number } }> }> = [];

    for (let r = 0; r < RATE_LIMIT; r++) {
      const seg = pending[r % pending.length];
      if (!seg.done) {
        tasks.push({ seg, promise: fetchAndInsert(pool, seg) });
      }
    }

    // Execute all in parallel
    let batchTotal = 0;
    let maxResetSec = 0;

    const results = await Promise.allSettled(tasks.map((t) => t.promise));

    for (let i = 0; i < results.length; i++) {
      const res = results[i];
      const seg = tasks[i].seg;

      if (res.status === "fulfilled") {
        batchTotal += res.value.inserted;
        if (res.value.rateLimit) {
          maxResetSec = Math.max(maxResetSec, res.value.rateLimit.reset);
        }
        // Persist progress
        if (seg.done) {
          await markSegmentComplete(pool, seg.id);
        } else if (seg.cursor) {
          await saveProgress(pool, seg.id, seg.cursor);
        }
      } else {
        const err = res.reason;
        if (err instanceof RateLimitError) {
          maxResetSec = Math.max(maxResetSec, err.retryAfter);
        } else if (err instanceof CursorExpiredError) {
          // Regenerate cursor from last known position
          if (seg.cursor) {
            try {
              const decoded = JSON.parse(Buffer.from(seg.cursor, "base64").toString());
              seg.cursor = createSyntheticCursor(decoded.ts);
            } catch {
              seg.cursor = createSyntheticCursor(seg.tsHigh);
            }
          }
        } else {
          console.error(`Segment ${seg.id} error:`, err.message || err);
        }
      }
    }

    totalFetched += batchTotal;

    const elapsed = (Date.now() - startTime) / 1000;
    const rate = elapsed > 0 ? totalFetched / (elapsed / 60) : 0;
    const eta = rate > 0 ? (TOTAL_EVENTS - totalFetched) / rate : 0;

    console.log(
      `[${windowNum}] +${batchTotal} | ${totalFetched.toLocaleString()}/${TOTAL_EVENTS.toLocaleString()} ` +
      `(${((totalFetched / TOTAL_EVENTS) * 100).toFixed(1)}%) | ` +
      `${Math.round(rate)}/min | ETA ${eta.toFixed(1)}m | ` +
      `${segments.filter((s) => !s.done).length} segs`
    );

    if (totalFetched >= TOTAL_EVENTS) break;

    // Wait for rate limit reset
    const elapsed_ms = Date.now() - windowStart;
    const waitMs = Math.max(maxResetSec * 1000, RATE_WINDOW_MS) - elapsed_ms + 500;
    if (waitMs > 0 && segments.some((s) => !s.done)) {
      await sleep(waitMs);
    }
  }

  const finalCount = await getEventCount(pool);

  if (finalCount < TOTAL_EVENTS) {
    console.log(`\n${finalCount} events in DB, need ${TOTAL_EVENTS}. Restarting...`);
    await pool.query("DELETE FROM ingestion_progress");
    await pool.end();
    process.exit(1);
  }

  const totalTime = (Date.now() - startTime) / 1000;
  console.log("\n============================================");
  console.log(`DONE: ${finalCount.toLocaleString()} events`);
  console.log(`Time: ${(totalTime / 60).toFixed(1)} min | Rate: ${Math.round(finalCount / (totalTime / 60))}/min`);
  console.log("ingestion complete");
  console.log("============================================");

  await pool.end();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
