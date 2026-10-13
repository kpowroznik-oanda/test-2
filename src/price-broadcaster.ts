import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import { randomUUID } from 'node:crypto';

export interface Price {
  instrument: string;
  bid: string;
  ask: string;
  time: string;
}

const HEARTBEAT_INTERVAL_MS = 15_000;
const IDLE_TIMEOUT_MS = 60_000;
const SWEEP_INTERVAL_MS = 30_000;

interface Session {
  id: string;
  instruments: Set<string>;
  stream: PassThrough;
  lastActivity: number;
  heartbeat: NodeJS.Timeout;
}

/**
 * Fans out prices from a single upstream feed to many SSE clients (web + mobile,
 * thousands of concurrent connections, frequent reconnects on flaky mobile networks).
 *
 * The HTTP layer is intentionally out of scope: callers pass in the already-parsed
 * instrument list and a way to register a disconnect callback (e.g. `res.on('close', cb)`),
 * and get back a stream to pipe to the response.
 */
export class PriceBroadcaster {
  private readonly sessions = new Map<string, Session>();

  constructor(private readonly feed: EventEmitter<{ price: [Price] }>) {
    this.feed.setMaxListeners(0); // one listener per connected client
    setInterval(() => this.sweepIdleSessions(), SWEEP_INTERVAL_MS);
  }

  connect(instruments: Set<string>, onDisconnect: (cb: () => void) => void): PassThrough {
    let session!: Session;
    session = {
      id: randomUUID(),
      instruments,
      stream: new PassThrough(),
      lastActivity: Date.now(),
      heartbeat: setInterval(() => this.heartbeat(session), HEARTBEAT_INTERVAL_MS),
    };
    session.stream.on('error', () => {
      // swallow write/socket errors from a single misbehaving client; don't take down the process
    });
    this.sessions.set(session.id, session);

    const onPrice = (price: Price) => {
      if (session.instruments.has(price.instrument)) this.send(session, 'price', price);
    };
    this.feed.on('price', onPrice);

    onDisconnect(() => {
      this.feed.off('price', onPrice);
      session.stream.end();
      // the session itself is reaped by sweepIdleSessions()
    });

    return session.stream;
  }

  private heartbeat(session: Session): void {
    session.lastActivity = Date.now();
    this.write(session, `: heartbeat ${session.lastActivity}\n\n`);
  }

  private send(session: Session, event: string, data: unknown): void {
    session.lastActivity = Date.now();
    this.write(session, `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  }

  private write(session: Session, chunk: string): void {
    if (session.stream.writableEnded || session.stream.destroyed) return; // client already gone
    session.stream.write(chunk);
  }

  private sweepIdleSessions(): void {
    const now = Date.now();
    for (const [id, session] of this.sessions) {
      if (now - session.lastActivity > IDLE_TIMEOUT_MS) {
        clearInterval(session.heartbeat);
        session.stream.end();
        this.sessions.delete(id);
      }
    }
  }
}
