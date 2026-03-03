import http from "http";

interface ApiResponse {
  data: any[];
  pagination: {
    limit: number;
    hasMore: boolean;
    nextCursor: string | null;
    cursorExpiresIn: number | null;
  };
  meta: {
    total: number;
    returned: number;
    requestId: string;
  };
  rateLimit?: {
    limit: number;
    remaining: number;
    reset: number;
  };
}

export class RateLimitError extends Error {
  status: number;
  retryAfter: number;

  constructor(retryAfter: number) {
    super(`Rate limit exceeded. Retry after ${retryAfter}s`);
    this.status = 429;
    this.retryAfter = retryAfter;
  }
}

export class CursorExpiredError extends Error {
  code: string;

  constructor(message: string) {
    super(message);
    this.code = "CURSOR_EXPIRED";
  }
}

/**
 * Create a synthetic cursor for a given timestamp.
 * Cursor format: base64 encoded JSON with {id, ts, v, exp}
 */
export function createSyntheticCursor(timestamp: number): string {
  // Use a far-future expiration
  const cursorData = {
    id: "00000000-0000-0000-0000-000000000000",
    ts: timestamp,
    v: 2,
    exp: Date.now() + 3600_000, // 1 hour from now
  };
  return Buffer.from(JSON.stringify(cursorData)).toString("base64");
}

function httpGet(url: string, headers: Record<string, string>): Promise<{ statusCode: number; headers: http.IncomingHttpHeaders; body: string }> {
  return new Promise((resolve, reject) => {
    const req = http.get(url, { headers }, (res) => {
      let body = "";
      res.on("data", (chunk) => (body += chunk));
      res.on("end", () => {
        resolve({
          statusCode: res.statusCode || 0,
          headers: res.headers,
          body,
        });
      });
    });
    req.on("error", reject);
    req.setTimeout(30000, () => {
      req.destroy(new Error("Request timeout"));
    });
  });
}

export async function fetchEventsPage(
  baseUrl: string,
  apiKey: string,
  cursor: string | null,
  limit: number,
  maxRetries: number = 3,
): Promise<ApiResponse> {
  let url = `${baseUrl}/api/v1/events?limit=${limit}`;
  if (cursor) {
    url += `&cursor=${encodeURIComponent(cursor)}`;
  }

  const headers = {
    "X-API-Key": apiKey,
    "Accept": "application/json",
  };

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const response = await httpGet(url, headers);

      // Parse rate limit headers
      const rateLimit = {
        limit: parseInt(response.headers["x-ratelimit-limit"] as string) || 10,
        remaining: parseInt(response.headers["x-ratelimit-remaining"] as string) || 0,
        reset: parseInt(response.headers["x-ratelimit-reset"] as string) || 60,
      };

      if (response.statusCode === 429) {
        const retryAfter = parseInt(response.headers["retry-after"] as string) || 60;
        throw new RateLimitError(retryAfter);
      }

      if (response.statusCode === 400) {
        const body = JSON.parse(response.body);
        if (body.code === "CURSOR_EXPIRED" || (body.message && body.message.includes("Cursor expired"))) {
          throw new CursorExpiredError(body.message);
        }
        throw new Error(`Bad Request: ${body.message}`);
      }

      if (response.statusCode !== 200) {
        throw new Error(`HTTP ${response.statusCode}: ${response.body.substring(0, 200)}`);
      }

      const data = JSON.parse(response.body);
      return { ...data, rateLimit };
    } catch (error: any) {
      if (error instanceof RateLimitError || error instanceof CursorExpiredError) {
        throw error;
      }

      if (attempt < maxRetries - 1) {
        console.warn(`Request failed (attempt ${attempt + 1}/${maxRetries}): ${error.message}`);
        await new Promise((resolve) => setTimeout(resolve, 2000 * (attempt + 1)));
        continue;
      }
      throw error;
    }
  }

  throw new Error("Max retries exceeded");
}
