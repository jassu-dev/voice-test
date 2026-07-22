import "dotenv-flow/config";
import express from "express";
import ExpressWs from "express-ws";
import * as crypto from "crypto";
import bot from "./bot";

const { app } = ExpressWs(express());
app.use(express.urlencoded({ extended: true })).use(express.json());

const XAI_API_KEY = process.env.XAI_API_KEY || "";
const API_URL = process.env.API_URL || "wss://api.x.ai/v1/realtime";
const ENABLE_TOOLS = process.env.ENABLE_TOOLS !== "false";

// ── VAD config — tune via env vars without touching code ──────────────────
// Issue 1 Fix A: Raise threshold to 0.75 for real phone lines (noisy carriers)
// Test: set VAD_THRESHOLD=0.7 first, call from real phone, check turn durations
// If turns still stay open >2x actual speaking time, try VAD_THRESHOLD=0.75
const VAD_CONFIG = {
  threshold: parseFloat(process.env.VAD_THRESHOLD || "0.75"),
  silence_duration_ms: parseInt(process.env.VAD_SILENCE_MS || "800"),
  prefix_padding_ms: parseInt(process.env.VAD_PREFIX_MS || "400"),
};

// Issue 2 Fix A: Debounce rapid-fire response.cancel sends
const CANCEL_DEBOUNCE_MS = parseInt(process.env.CANCEL_DEBOUNCE_MS || "250");

// Issue 3 Fix B: Filler injection threshold
const FILLER_THRESHOLD_MS = parseInt(process.env.FILLER_THRESHOLD_MS || "700");
const FILLER_PHRASES = ["Mm-hmm,", "Let me see,", "Sure,"];
let fillerIndex = 0;
const nextFiller = () => { const f = FILLER_PHRASES[fillerIndex % FILLER_PHRASES.length]; fillerIndex++; return f; };

function generateSecureId(prefix: string): string {
  return `${prefix}_${crypto.randomBytes(8).toString("hex")}`;
}
function ts(): string { return new Date().toISOString().slice(11, 23); }


// ── Pre-warm xAI WebSocket pool ───────────────────────────────────────────
interface WarmConnection { ws: any; ready: boolean; createdAt: number; earlyMessages: string[]; }
let warmPool: WarmConnection | null = null;
const MAX_WARM_AGE_MS = 55000;

function createWarmConnection(): WarmConnection {
  const WebSocket = require("ws");
  const conn: WarmConnection = {
    ws: new WebSocket(API_URL, { headers: { Authorization: `Bearer ${XAI_API_KEY}` } }),
    ready: false,
    createdAt: Date.now(),
    earlyMessages: [],
  };
  conn.ws.on("open", () => { conn.ready = true; });
  // Buffer any server messages that arrive while connection sits idle in pool
  // (session.created, conversation.created fire immediately on open)
  conn.ws.on("message", (data: Buffer, isBinary: boolean) => {
    if (!isBinary) conn.earlyMessages.push(data.toString());
  });
  conn.ws.on("error", () => { if (warmPool === conn) warmPool = null; });
  conn.ws.on("close", () => { if (warmPool === conn) warmPool = null; });
  return conn;
}

function getOrCreateWarmWs(): Promise<any> {
  const WebSocket = require("ws");
  const now = Date.now();
  if (warmPool && warmPool.ready && (now - warmPool.createdAt) < MAX_WARM_AGE_MS) {
    const conn = warmPool;
    warmPool = null;
    setTimeout(() => { warmPool = createWarmConnection(); }, 0);
    if (conn.earlyMessages.length > 0) {
      const types = conn.earlyMessages.map(m => { try { return JSON.parse(m).type; } catch { return "?"; } }).join(", ");
      console.log(`${ts()} [pool] claimed warm connection had ${conn.earlyMessages.length} early message(s): ${types}`);
    }
    return Promise.resolve(conn.ws);
  }
  console.log(`${ts()} [pool] no warm connection — connecting now`);
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(API_URL, { headers: { Authorization: `Bearer ${XAI_API_KEY}` } });
    const t = setTimeout(() => { ws.close(); reject(new Error("xAI connect timeout")); }, 10000);
    ws.on("open", () => { clearTimeout(t); resolve(ws); });
    ws.on("error", (e: any) => { clearTimeout(t); reject(e); });
  });
}
warmPool = createWarmConnection();

const tools = [
  { type: "function", name: "generate_random_number", description: "Generate a random number between min and max values",
    parameters: { type: "object", properties: { min: { type: "number", description: "Minimum value (inclusive)" }, max: { type: "number", description: "Maximum value (inclusive)" } }, required: ["min", "max"] } },
];

async function handleToolCall(name: string, args: Record<string, any>): Promise<string> {
  if (name === "generate_random_number") {
    const min = Math.ceil(args.min); const max = Math.floor(args.max);
    return JSON.stringify({ result: Math.floor(Math.random() * (max - min + 1)) + min });
  }
  return JSON.stringify({ error: `Unknown tool: ${name}` });
}


// ── Audio Playback Manager (real-time paced) ──────────────────────────────
// Twilio expects µ-law 8kHz audio at real-time pace.
// Standard frame: 160 bytes = 20ms of audio at 8kHz.
// Previous implementation flushed all queued chunks in a tight loop,
// sending 13s of audio in ~2.6s — activeResponseId cleared way before
// the caller finished hearing the response, breaking barge-in detection.
// This version paces outbound frames at exactly 20ms/frame via setInterval.
class AudioPlayer {
  private frameQueue: Buffer[] = [];
  private activeResponseId: string | null = null;
  private playedMs: number = 0;
  private outputItemAdded: boolean = false;
  private paceTimer: ReturnType<typeof setInterval> | null = null;
  private onDrained: (() => void) | null = null;
  private sendFn: (payload: string) => void;
  private callId: string;
  private readonly FRAME_MS = 20;
  private readonly FRAME_BYTES = 160; // 8kHz mulaw, 20ms per frame

  constructor(callId: string, sendFn: (payload: string) => void) {
    this.callId = callId;
    this.sendFn = sendFn;
  }

  setResponse(responseId: string) {
    this.activeResponseId = responseId;
    this.playedMs = 0;
    this.outputItemAdded = false;
    this.frameQueue = [];
    this.startPacing();
  }

  markOutputItem() { this.outputItemAdded = true; }
  hadOutputItem() { return this.outputItemAdded; }
  getQueuedFrameCount() { return this.frameQueue.length; }

  // Chop incoming delta into 160-byte frames and enqueue for paced delivery
  enqueue(base64Payload: string, responseId: string) {
    if (responseId !== this.activeResponseId) return;
    const bytes = Buffer.from(base64Payload, "base64");
    for (let i = 0; i < bytes.length; i += this.FRAME_BYTES) {
      this.frameQueue.push(bytes.subarray(i, i + this.FRAME_BYTES));
    }
  }

  private startPacing() {
    this.stopPacing();
    this.paceTimer = setInterval(() => {
      const frame = this.frameQueue.shift();
      if (frame) {
        this.sendFn(frame.toString("base64"));
        this.playedMs += this.FRAME_MS;
      } else if (this.onDrained) {
        // Queue empty AND generation is done — playback genuinely finished
        const cb = this.onDrained;
        this.onDrained = null;
        this.stopPacing();
        cb();
      }
    }, this.FRAME_MS);
  }

  private stopPacing() {
    if (this.paceTimer) { clearInterval(this.paceTimer); this.paceTimer = null; }
  }

  // Call on response.done — do NOT clear state immediately.
  // If queue still has frames, defer cleanup until they drain (real playback done).
  markGenerationDone(onActuallyDone: () => void) {
    if (this.frameQueue.length === 0) {
      this.stopPacing();
      onActuallyDone();
    } else {
      this.onDrained = onActuallyDone;
    }
  }

  // Hard interrupt: stop pacing immediately, drop all remaining frames
  interrupt(): number {
    const played = this.playedMs;
    this.frameQueue = [];
    this.onDrained = null;
    this.stopPacing();
    this.activeResponseId = null;
    this.outputItemAdded = false;
    return played;
  }

  clear() {
    this.frameQueue = [];
    this.onDrained = null;
    this.stopPacing();
    this.activeResponseId = null;
    this.playedMs = 0;
    this.outputItemAdded = false;
  }

  getActiveResponseId(): string | null { return this.activeResponseId; }
  getPlayedMs(): number { return this.playedMs; }
}

app.get("/health", (_req, res) => res.json({ status: "ok", vad: VAD_CONFIG, cancelDebounce: CANCEL_DEBOUNCE_MS, fillerThreshold: FILLER_THRESHOLD_MS }));

app.post("/twiml", (_req, res) => {
  if (!process.env.HOSTNAME) { res.status(500).send("HOSTNAME not set"); return; }
  const callId = generateSecureId("call");
  const hostname = process.env.HOSTNAME.replace(/^https?:\/\//, "").replace(/\/$/, "");
  console.log(`${ts()} [${callId}] incoming call`);
  res.status(200).type("text/xml").end(
    `<Response><Connect><Stream url="wss://${hostname}/media-stream/${callId}" /></Connect></Response>`
  );
});

app.post("/call-status", (_req, res) => res.status(200).send());


// ── Inbound Media Stream ───────────────────────────────────────────────────
app.ws("/media-stream/:callId", async (ws: any, req) => {
  const callId = req.params.callId as string;
  const callStartTime = Date.now();
  console.log(`${ts()} [${callId}] CALL STARTED`);

  let streamSid = "";
  let sessionReady = false;
  let currentItemId: string | null = null;
  const pendingAudio: string[] = [];
  let micChunkCount = 0;

  // Barge-in state
  let lastCancelSentAt = 0;
  let lastCancelledResponseId: string | null = null;
  let lastCancelTimestamp = 0;

  // Turn timing (Issue 1 Fix B)
  let speechStartedAt = 0;
  let lastTranscript = "";

  // Watchdog
  let commitWatchdog: ReturnType<typeof setTimeout> | null = null;
  const clearWatchdog = () => { if (commitWatchdog) { clearTimeout(commitWatchdog); commitWatchdog = null; } };
  const startWatchdog = () => {
    clearWatchdog();
    commitWatchdog = setTimeout(() => { console.log(`${ts()} [${callId}] [WATCHDOG] no response.created within 2500ms of commit`); }, 2500);
  };

  // Filler injection (Issue 3 Fix B)
  let fillerTimer: ReturnType<typeof setTimeout> | null = null;
  const clearFillerTimer = () => { if (fillerTimer) { clearTimeout(fillerTimer); fillerTimer = null; } };

  const sendToTwilio = (payload: string) => {
    if (streamSid) {
      ws.send(JSON.stringify({ event: "media", streamSid, media: { payload } }), (err: any) => {
        if (err) console.log(`${ts()} [${callId}] send error: ${err.message}`);
      });
    } else {
      pendingAudio.push(payload);
    }
  };

  const player = new AudioPlayer(callId, sendToTwilio);

  // ── Twilio handler first ─────────────────────────────────────────────────
  ws.on("message", (raw: string) => {
    let msg: any;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.event === "start") {
      streamSid = msg.start.streamSid;
      console.log(`${ts()} [${callId}] twilio ready sid=${streamSid} (+${Date.now() - callStartTime}ms)`);
      if (pendingAudio.length > 0) {
        for (const p of pendingAudio) ws.send(JSON.stringify({ event: "media", streamSid, media: { payload: p } }));
        pendingAudio.length = 0;
      }
    } else if (msg.event === "media") {
      if (msg.media?.track !== "inbound") return;
      if (!sessionReady || xaiWs.readyState !== 1) return;
      if (micChunkCount === 0) console.log(`${ts()} [${callId}] [LATENCY] first mic chunk → xAI`);
      micChunkCount++;
      xaiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio: msg.media.payload }));
    } else if (msg.event === "stop") {
      console.log(`${ts()} [${callId}] call ended`);
      clearWatchdog(); clearFillerTimer(); clearTimeout(sessionReadyWatchdog);
      xaiWs.close();
    }
  });

  ws.on("close", () => { clearWatchdog(); clearFillerTimer(); clearTimeout(sessionReadyWatchdog); player.clear(); try { xaiWs.close(); } catch {} });

  console.log(`${ts()} [${callId}] [CONNECT-ATTEMPT-START]`);
  const connectStart = Date.now();
  const xaiWs = await getOrCreateWarmWs().catch(e => {
    console.log(`${ts()} [${callId}] xAI connect failed: ${e.message}`);
    ws.close(); throw e;
  });
  console.log(`${ts()} [${callId}] xAI connected (+${Date.now() - connectStart}ms from attempt, +${Date.now() - callStartTime}ms total)`);

  // ── FIX: Send session.update immediately — do NOT wait for conversation.created
  // session.created and conversation.created fire immediately on socket open and
  // may already be missed if this is a warm (pre-opened) connection.
  // The socket is guaranteed open here (promise resolved on open event), so send now.
  console.log(`${ts()} [${callId}] [SEND] session.update dispatched`);
  xaiWs.send(JSON.stringify({
    type: "session.update",
    session: {
      instructions: bot.instructions,
      voice: "celeste",
      reasoning: { effort: "none" },
      turn_detection: { type: "server_vad", ...VAD_CONFIG },
      audio: {
        input: { format: { type: "audio/pcmu" }, transcription: { model: "grok-transcribe" } },
        output: { format: { type: "audio/pcmu" } },
      },
      ...(ENABLE_TOOLS ? { tools } : {}),
    },
  }));

  // ── Session-ready watchdog — fires if session never becomes ready ────────
  const sessionReadyWatchdog = setTimeout(() => {
    if (!sessionReady) {
      console.log(`${ts()} [${callId}] [CRITICAL] session never became ready — call is deaf/mute`);
    }
  }, 3000);


  // ── xAI message handler ───────────────────────────────────────────────────
  xaiWs.on("message", (data: Buffer, isBinary: boolean) => {
    if (isBinary) {
      const payload = data.toString("base64");
      const rid = player.getActiveResponseId() || "";
      if (player.getQueuedFrameCount() === 0 && player.getPlayedMs() === 0) {
        console.log(`${ts()} [${callId}] [LATENCY] first binary audio frame → paced queue`);
        clearFillerTimer();
      }
      player.enqueue(payload, rid);
      return;
    }

    let msg: any;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    const quiet = ["response.output_audio.delta", "response.output_audio_transcript.delta"];
    if (!quiet.includes(msg.type)) console.log(`${ts()} [${callId}] ${msg.type}`);

    switch (msg.type) {

      // ── Issue 2 Fix B: downgrade known-race error, log others fully ──────
      case "error": {
        const errMsg: string = msg.error?.message || "";
        if (errMsg.includes("Cancellation failed")) {
          console.log(`${ts()} [${callId}] [EXPECTED-RACE] ${errMsg}`);
        } else {
          console.log(`${ts()} [${callId}] [SERVER-ERROR] ${JSON.stringify(msg)}`);
        }
        break;
      }

      case "response.output_audio.delta":
        if (msg.delta && msg.response_id) {
          // First frame enqueued — log latency marker
          if (player.getQueuedFrameCount() === 0 && player.getPlayedMs() === 0) {
            console.log(`${ts()} [${callId}] [LATENCY] first audio delta → paced queue`);
          }
          clearFillerTimer();
          player.enqueue(msg.delta, msg.response_id);
        }
        break;

      case "response.output_audio_transcript.delta":
        process.stdout.write(msg.delta || "");
        break;

      case "response.created": {
        clearWatchdog();
        const rid = msg.response?.id || "";
        const now = Date.now();
        if (lastCancelledResponseId && (now - lastCancelTimestamp) < 400) {
          console.log(`${ts()} [${callId}] [FALSE-START] response_id=${rid} created ${now - lastCancelTimestamp}ms after cancel of ${lastCancelledResponseId}`);
        }
        console.log(`${ts()} [${callId}] [LATENCY] response.created id=${rid} (+${Date.now() - callStartTime}ms total)`);
        player.setResponse(rid);

        // Issue 3 Fix B: filler injection if no audio within FILLER_THRESHOLD_MS
        clearFillerTimer();
        fillerTimer = setTimeout(() => {
          fillerTimer = null;
          if (player.getActiveResponseId() === rid) {
            const phrase = nextFiller();
            console.log(`${ts()} [${callId}] [FILLER-INJECTED] response_id=${rid} phrase="${phrase}"`);
            xaiWs.send(JSON.stringify({
              type: "conversation.item.create",
              item: { type: "force_message", role: "assistant", interruptible: true, content: [{ type: "output_text", text: phrase }] },
            }));
          }
        }, FILLER_THRESHOLD_MS);
        break;
      }

      case "response.output_item.added":
        if (msg.item?.type === "message" && msg.item?.role === "assistant") {
          currentItemId = msg.item.id;
          player.markOutputItem();
        }
        break;

      case "response.done":
      case "response.cancelled": {
        process.stdout.write("\n");
        clearFillerTimer();
        const rid = player.getActiveResponseId();
        const queuedFrames = player.getQueuedFrameCount();
        console.log(`${ts()} [${callId}] [PACING] generation done, ${queuedFrames} frames (${queuedFrames * 20}ms) still queued for real-time playback`);
        // markGenerationDone defers cleanup until paced queue drains
        // so activeResponseId stays non-null until caller finishes hearing audio
        player.markGenerationDone(() => {
          if (rid && !player.hadOutputItem()) {
            console.log(`${ts()} [${callId}] [FALSE-START] response_id=${rid} ended with no output`);
          }
          console.log(`${ts()} [${callId}] [PACING] playback complete response_id=${rid}`);
        });
        break;
      }

      case "conversation.item.input_audio_transcription.completed": {
        lastTranscript = msg.transcript || "";
        const turnDuration = speechStartedAt > 0 ? Date.now() - speechStartedAt : 0;
        console.log(`${ts()} [${callId}] [TRANSCRIPTION] "${lastTranscript}" turn_duration=${turnDuration}ms transcript_len=${lastTranscript.length}`);
        // Issue 1 Fix B: flag suspiciously long turns
        if (turnDuration > 0 && lastTranscript.length > 0) {
          const msPerChar = turnDuration / lastTranscript.length;
          if (msPerChar > 150) { // >150ms per char = far longer than actual speech
            console.log(`${ts()} [${callId}] [LONG-TURN] duration=${turnDuration}ms transcript_len=${lastTranscript.length} ms_per_char=${Math.round(msPerChar)} — possible VAD noise issue`);
          }
        }
        break;
      }

      case "conversation.item.input_audio_transcription.updated":
        console.log(`${ts()} [${callId}] [TRANSCRIPTION-UPDATE] "${msg.transcript}"`);
        break;

      // ── Barge-in — guarded + debounced ───────────────────────────────────
      case "input_audio_buffer.speech_started": {
        speechStartedAt = Date.now();
        const activeRid = player.getActiveResponseId();
        if (activeRid !== null) {
          const playedMs = player.interrupt();
          if (streamSid) ws.send(JSON.stringify({ event: "clear", streamSid }));

          const now = Date.now();
          // Issue 2 Fix A: debounce cancel sends — local state always updated
          if (now - lastCancelSentAt < CANCEL_DEBOUNCE_MS) {
            console.log(`${ts()} [${callId}] [BARGE-IN-SUPPRESSED] within ${CANCEL_DEBOUNCE_MS}ms debounce, skipping cancel send (played=${playedMs}ms)`);
          } else {
            lastCancelSentAt = now;
            lastCancelledResponseId = activeRid;
            lastCancelTimestamp = now;
            console.log(`${ts()} [${callId}] [BARGE-IN] cancelling response_id=${activeRid} played=${playedMs}ms`);
            xaiWs.send(JSON.stringify({ type: "response.cancel" }));
            if (currentItemId && playedMs > 0) {
              xaiWs.send(JSON.stringify({ type: "conversation.item.truncate", item_id: currentItemId, content_index: 0, audio_end_ms: playedMs }));
            }
          }
        } else {
          console.log(`${ts()} [${callId}] [speech_started] no active response — normal turn`);
        }
        break;
      }

      case "input_audio_buffer.speech_stopped":
        console.log(`${ts()} [${callId}] [LATENCY] speech_stopped (+${Date.now() - speechStartedAt}ms from speech_started)`);
        break;

      case "input_audio_buffer.committed":
        console.log(`${ts()} [${callId}] [LATENCY] committed — requesting response`);
        startWatchdog();
        xaiWs.send(JSON.stringify({ type: "response.create" }));
        break;

      case "conversation.item.truncated":
        console.log(`${ts()} [${callId}] item truncated at ${msg.audio_end_ms}ms`);
        break;

      case "session.updated":
        sessionReady = true;
        clearTimeout(sessionReadyWatchdog);
        console.log(`${ts()} [${callId}] session ready (+${Date.now() - callStartTime}ms total)`);
        xaiWs.send(JSON.stringify({ type: "conversation.item.create", item: { type: "force_message", role: "assistant", interruptible: true, content: [{ type: "output_text", text: "Hey! How can I help you today?" }] } }));
        console.log(`${ts()} [${callId}] [SEND] force_message greeting dispatched`);
        break;

      // conversation.created: log only — session.update already sent above
      case "conversation.created":
        console.log(`${ts()} [${callId}] conversation.created (session.update already dispatched)`);
        break;

      case "response.output_item.done":
        if (msg.item?.type === "function_call") {
          (async () => {
            let args: Record<string, any> = {};
            try { args = JSON.parse(msg.item.arguments || "{}"); } catch {}
            const result = await handleToolCall(msg.item.name, args);
            xaiWs.send(JSON.stringify({ type: "conversation.item.create", item: { type: "function_call_output", call_id: msg.item.call_id, output: result } }));
            const waitForPlayback = () => new Promise<void>(res => { const check = () => { if (!player.getActiveResponseId()) { res(); return; } setTimeout(check, 50); }; check(); });
            await waitForPlayback();
            xaiWs.send(JSON.stringify({ type: "response.create" }));
          })();
        }
        break;
    }
  });

  xaiWs.on("error", (e: any) => console.log(`${ts()} [${callId}] ws error: ${e?.message}`));
  xaiWs.on("close", (code: number) => { player.clear(); console.log(`${ts()} [${callId}] ws closed: ${code}`); });
});


// ── Outbound ───────────────────────────────────────────────────────────────
const OUTBOUND_INSTRUCTIONS = `You are an outbound AI phone agent. YOU speak first. Greet warmly, keep replies short and conversational.`;

app.post("/outbound-twiml", (_req, res) => {
  const hostname = (process.env.HOSTNAME || "").replace(/^https?:\/\//, "").replace(/\/$/, "");
  res.type("text/xml").send(`<Response><Connect><Stream url="wss://${hostname}/outbound-stream" /></Connect></Response>`);
});

app.ws("/outbound-stream", (ws: any) => {
  const WebSocket = require("ws");
  const MAX_TURNS = 10;
  let streamSid = "", callSid = "";
  let xaiWs: any = null;
  let sessionReady = false;
  let turnCount = 0;
  let currentResponseId: string | null = null;
  let currentItemId: string | null = null;
  let playedMs = 0;
  let outputItemAdded = false;
  let lastCancelSentAt = 0;
  let lastCancelledResponseId: string | null = null;
  let lastCancelTimestamp = 0;
  let commitWatchdog: ReturnType<typeof setTimeout> | null = null;
  let speechStartedAt = 0;

  ws.on("message", (raw: string) => {
    let msg: any;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.event === "start") {
      callSid = msg.start.callSid; streamSid = msg.start.streamSid;
      console.log(`${ts()} [OUTBOUND][${callSid}] started`);

      xaiWs = new WebSocket(API_URL, { headers: { Authorization: `Bearer ${XAI_API_KEY}` } });

      // Outbound paced audio state
      const FRAME_MS = 20, FRAME_BYTES = 160;
      let outFrameQueue: Buffer[] = [];
      let outPaceTimer: ReturnType<typeof setInterval> | null = null;
      let outOnDrained: (() => void) | null = null;

      const startOutPacing = () => {
        if (outPaceTimer) return;
        outPaceTimer = setInterval(() => {
          const frame = outFrameQueue.shift();
          if (frame && streamSid) {
            ws.send(JSON.stringify({ event: "media", streamSid, media: { payload: frame.toString("base64") } }));
            playedMs += FRAME_MS;
          } else if (!frame && outOnDrained) {
            const cb = outOnDrained; outOnDrained = null;
            clearInterval(outPaceTimer!); outPaceTimer = null;
            cb();
          }
        }, FRAME_MS);
      };

      const enqueueOutAudio = (base64Payload: string, responseId: string) => {
        if (responseId !== currentResponseId) return;
        const bytes = Buffer.from(base64Payload, "base64");
        for (let i = 0; i < bytes.length; i += FRAME_BYTES) outFrameQueue.push(bytes.subarray(i, i + FRAME_BYTES));
        startOutPacing();
      };

      const interruptOutAudio = () => {
        outFrameQueue = []; outOnDrained = null;
        if (outPaceTimer) { clearInterval(outPaceTimer); outPaceTimer = null; }
      };
      xaiWs.on("open", () => {
        console.log(`${ts()} [OUTBOUND][${callSid}] [SEND] session.update dispatched`);
        xaiWs.send(JSON.stringify({ type: "session.update", session: {
          instructions: OUTBOUND_INSTRUCTIONS, voice: "atlas", reasoning: { effort: "none" },
          turn_detection: { type: "server_vad", ...VAD_CONFIG },
          audio: { input: { format: { type: "audio/pcmu" }, transcription: { model: "grok-transcribe" } }, output: { format: { type: "audio/pcmu" } } },
        }}));
      });

      xaiWs.on("message", (d: Buffer, isBinary: boolean) => {
        if (isBinary) {
          const payload = d.toString("base64");
          if (streamSid && currentResponseId) enqueueOutAudio(payload, currentResponseId);
          return;
        }
        let m: any;
        try { m = JSON.parse(d.toString()); } catch { return; }
        if (m.type !== "response.output_audio.delta") console.log(`${ts()} [OUTBOUND][${callSid}] ${m.type}`);

        switch (m.type) {
          case "error": {
            const errMsg: string = m.error?.message || "";
            if (errMsg.includes("Cancellation failed")) { console.log(`${ts()} [OUTBOUND] [EXPECTED-RACE] ${errMsg}`); }
            else { console.log(`${ts()} [OUTBOUND] [SERVER-ERROR] ${JSON.stringify(m)}`); }
            break;
          }
          case "session.updated": sessionReady = true; xaiWs.send(JSON.stringify({ type: "response.create" })); break;
          case "response.created": {
            if (commitWatchdog) { clearTimeout(commitWatchdog); commitWatchdog = null; }
            const now = Date.now();
            if (lastCancelledResponseId && (now - lastCancelTimestamp) < 400) console.log(`${ts()} [OUTBOUND][${callSid}] [FALSE-START] response_id=${m.response?.id} created ${now - lastCancelTimestamp}ms after cancel`);
            turnCount++; currentResponseId = m.response?.id || null; playedMs = 0; outputItemAdded = false;
            break;
          }
          case "response.output_item.added":
            if (m.item?.type === "message" && m.item?.role === "assistant") { currentItemId = m.item.id; outputItemAdded = true; }
            break;
          case "response.output_audio.delta":
            if (m.delta && m.response_id === currentResponseId && streamSid) {
              enqueueOutAudio(m.delta, m.response_id);
            }
            break;
          case "response.done": case "response.cancelled": {
            const qf = outFrameQueue.length;
            console.log(`${ts()} [OUTBOUND][${callSid}] [PACING] generation done, ${qf} frames (${qf * 20}ms) queued`);
            const doneRid = currentResponseId;
            outOnDrained = () => {
              if (doneRid && !outputItemAdded) console.log(`${ts()} [OUTBOUND][${callSid}] [FALSE-START] response_id=${doneRid} ended with no output`);
              currentResponseId = null;
              console.log(`${ts()} [OUTBOUND][${callSid}] [PACING] playback complete`);
              if (turnCount >= MAX_TURNS) setTimeout(() => { xaiWs?.close(); ws.close(); }, 3000);
            };
            if (outFrameQueue.length === 0) { const cb = outOnDrained; outOnDrained = null; if (outPaceTimer) { clearInterval(outPaceTimer); outPaceTimer = null; } cb(); }
            break;
          }
          case "input_audio_buffer.speech_started": {
            speechStartedAt = Date.now();
            const rid = currentResponseId;
            if (rid !== null) {
              interruptOutAudio(); // stop paced playback immediately
              currentResponseId = null;
              if (streamSid) ws.send(JSON.stringify({ event: "clear", streamSid }));
              const now = Date.now();
              if (now - lastCancelSentAt < CANCEL_DEBOUNCE_MS) {
                console.log(`${ts()} [OUTBOUND][${callSid}] [BARGE-IN-SUPPRESSED] within debounce`);
              } else {
                lastCancelSentAt = now; lastCancelledResponseId = rid; lastCancelTimestamp = now;
                console.log(`${ts()} [OUTBOUND][${callSid}] [BARGE-IN] cancelling response_id=${rid} played=${playedMs}ms`);
                xaiWs.send(JSON.stringify({ type: "response.cancel" }));
                if (currentItemId && playedMs > 0) xaiWs.send(JSON.stringify({ type: "conversation.item.truncate", item_id: currentItemId, content_index: 0, audio_end_ms: playedMs }));
              }
            } else {
              console.log(`${ts()} [OUTBOUND][${callSid}] [speech_started] no active response — normal turn`);
            }
            break;
          }
          case "input_audio_buffer.speech_stopped":
            console.log(`${ts()} [OUTBOUND][${callSid}] speech_stopped (+${Date.now() - speechStartedAt}ms from speech_started)`);
            break;
          case "input_audio_buffer.committed":
            commitWatchdog = setTimeout(() => { console.log(`${ts()} [OUTBOUND][${callSid}] [WATCHDOG] no response.created within 2500ms`); }, 2500);
            xaiWs.send(JSON.stringify({ type: "response.create" }));
            break;
          case "conversation.item.input_audio_transcription.completed": {
            const transcript = m.transcript || "";
            const turnDuration = speechStartedAt > 0 ? Date.now() - speechStartedAt : 0;
            console.log(`${ts()} [OUTBOUND][${callSid}] [TRANSCRIPTION] "${transcript}" turn_duration=${turnDuration}ms`);
            if (turnDuration > 0 && transcript.length > 0 && (turnDuration / transcript.length) > 150)
              console.log(`${ts()} [OUTBOUND][${callSid}] [LONG-TURN] duration=${turnDuration}ms len=${transcript.length} — possible VAD noise`);
            break;
          }
          case "response.output_audio_transcript.delta": process.stdout.write(m.delta || ""); break;
        }
      });

      xaiWs.on("close", () => { interruptOutAudio(); console.log(`${ts()} [OUTBOUND][${callSid}] xAI closed`); });

    } else if (msg.event === "media" && msg.media?.track === "inbound") {
      if (xaiWs && sessionReady && xaiWs.readyState === 1)
        xaiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio: msg.media.payload }));
    } else if (msg.event === "stop") {
      if (commitWatchdog) clearTimeout(commitWatchdog);
      xaiWs?.close();
    }
  });

  ws.on("close", () => { try { xaiWs?.close(); } catch {} });
});

const port = process.env.PORT || "3000";
app.listen(port, () => {
  console.log(`Server on http://localhost:${port}`);
  console.log(`VAD: threshold=${VAD_CONFIG.threshold} silence=${VAD_CONFIG.silence_duration_ms}ms prefix=${VAD_CONFIG.prefix_padding_ms}ms`);
  console.log(`Cancel debounce: ${CANCEL_DEBOUNCE_MS}ms | Filler threshold: ${FILLER_THRESHOLD_MS}ms`);
  console.log(`Tools: ${ENABLE_TOOLS ? "enabled" : "disabled"} | xAI warm pool: seeded`);
});
