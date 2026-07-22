import "dotenv-flow/config";
import express from "express";
import ExpressWs from "express-ws";
import * as crypto from "crypto";
import bot from "./bot";
import { notifyCallStart, notifyCallEnd, logTranscript } from "./db-client";

const { app } = ExpressWs(express());
app.use(express.urlencoded({ extended: true })).use(express.json());

const XAI_API_KEY = process.env.XAI_API_KEY || "";
const API_URL = process.env.API_URL || "wss://api.x.ai/v1/realtime";
const ENABLE_TOOLS = process.env.ENABLE_TOOLS !== "false";

const VAD_CONFIG = {
  threshold: parseFloat(process.env.VAD_THRESHOLD || "0.75"),
  silence_duration_ms: parseInt(process.env.VAD_SILENCE_MS || "650"),
  prefix_padding_ms: parseInt(process.env.VAD_PREFIX_MS || "400"),
};
const CANCEL_DEBOUNCE_MS = parseInt(process.env.CANCEL_DEBOUNCE_MS || "250");
const FILLER_THRESHOLD_MS = parseInt(process.env.FILLER_THRESHOLD_MS || "800");
const FILLER_PHRASES = ["Mm-hmm,", "Let me see,", "Sure,"];
const VOICE_ID = process.env.VOICE_ID || "rigel";
const OUTPUT_SPEED = parseFloat(process.env.OUTPUT_SPEED || "1.0");
const REPLACE_MAP: Record<string, string> = {};
const KEYTERMS: string[] = process.env.KEYTERMS ? process.env.KEYTERMS.split(",").map(k => k.trim()) : [];

let fillerIndex = 0;
const nextFiller = () => { const f = FILLER_PHRASES[fillerIndex % FILLER_PHRASES.length]; fillerIndex++; return f; };
function generateSecureId(p: string) { return `${p}_${crypto.randomBytes(8).toString("hex")}`; }
function ts() { return new Date().toISOString().slice(11, 23); }

// ── Warm pool ─────────────────────────────────────────────────────────────
interface WarmConnection { ws: any; ready: boolean; createdAt: number; earlyMessages: string[]; }
let warmPool: WarmConnection | null = null;
const MAX_WARM_AGE_MS = 55000;

function createWarmConnection(): WarmConnection {
  const WebSocket = require("ws");
  const conn: WarmConnection = {
    ws: new WebSocket(API_URL, { headers: { Authorization: `Bearer ${XAI_API_KEY}` } }),
    ready: false, createdAt: Date.now(), earlyMessages: [],
  };
  conn.ws.on("open", () => { conn.ready = true; });
  conn.ws.on("message", (data: Buffer, isBinary: boolean) => { if (!isBinary) conn.earlyMessages.push(data.toString()); });
  conn.ws.on("error", () => { if (warmPool === conn) warmPool = null; });
  conn.ws.on("close", () => { if (warmPool === conn) warmPool = null; });
  return conn;
}

function getOrCreateWarmWs(): Promise<any> {
  const WebSocket = require("ws");
  const now = Date.now();
  if (warmPool && warmPool.ready && (now - warmPool.createdAt) < MAX_WARM_AGE_MS) {
    const conn = warmPool; warmPool = null;
    setTimeout(() => { warmPool = createWarmConnection(); }, 0);
    if (conn.earlyMessages.length > 0) {
      const types = conn.earlyMessages.map(m => { try { return JSON.parse(m).type; } catch { return "?"; } }).join(", ");
      console.log(`${ts()} [pool] warm connection had ${conn.earlyMessages.length} early messages: ${types}`);
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
    parameters: { type: "object", properties: { min: { type: "number" }, max: { type: "number" } }, required: ["min", "max"] } },
];

async function handleToolCall(name: string, args: Record<string, any>): Promise<string> {
  if (name === "generate_random_number") {
    const min = Math.ceil(args.min); const max = Math.floor(args.max);
    return JSON.stringify({ result: Math.floor(Math.random() * (max - min + 1)) + min });
  }
  return JSON.stringify({ error: `Unknown tool: ${name}` });
}

// ── AudioPlayer ───────────────────────────────────────────────────────────
class AudioPlayer {
  private activeResponseId: string | null = null;
  private totalQueuedBytes = 0;
  private playbackStartedAt = 0;
  private outputItemAdded = false;
  private onDrained: (() => void) | null = null;
  private drainTimer: ReturnType<typeof setTimeout> | null = null;
  private firstChunkLogged = false;
  private callId: string;
  private sendFn: (p: string) => void;

  constructor(callId: string, sendFn: (p: string) => void) { this.callId = callId; this.sendFn = sendFn; }

  setResponse(id: string) {
    this.stopDrain(); this.activeResponseId = id; this.totalQueuedBytes = 0;
    this.playbackStartedAt = 0; this.outputItemAdded = false; this.onDrained = null; this.firstChunkLogged = false;
  }
  markOutputItem() { this.outputItemAdded = true; }
  hadOutputItem() { return this.outputItemAdded; }
  getQueuedFrameCount() { return Math.ceil(this.totalQueuedBytes / 160); }

  enqueue(b64: string, responseId: string) {
    if (responseId !== this.activeResponseId) return;
    const bytes = Buffer.from(b64, "base64");
    if (this.playbackStartedAt === 0) {
      this.playbackStartedAt = Date.now();
      if (!this.firstChunkLogged) { console.log(`${ts()} [${this.callId}] [LATENCY] first audio chunk → Twilio`); this.firstChunkLogged = true; }
    }
    this.totalQueuedBytes += bytes.length;
    this.sendFn(b64);
  }

  getPlayedMs() {
    if (this.playbackStartedAt === 0) return 0;
    return Math.min(Date.now() - this.playbackStartedAt, Math.round(this.totalQueuedBytes / 8));
  }

  getActiveResponseId() { return this.activeResponseId; }

  markGenerationDone(cb: () => void) {
    const totalMs = Math.round(this.totalQueuedBytes / 8);
    const elapsed = this.playbackStartedAt > 0 ? Date.now() - this.playbackStartedAt : 0;
    const remaining = Math.max(0, totalMs - elapsed);
    console.log(`${ts()} [${this.callId}] [PACING] generation done, ~${remaining}ms audio still playing`);
    if (remaining <= 0) { cb(); return; }
    this.onDrained = cb;
    this.drainTimer = setTimeout(() => { this.drainTimer = null; this.onDrained = null; cb(); }, remaining + 100);
  }

  interrupt() {
    const played = this.getPlayedMs(); this.stopDrain(); this.activeResponseId = null; this.outputItemAdded = false; this.onDrained = null;
    return played;
  }

  clear() {
    this.stopDrain(); this.activeResponseId = null; this.totalQueuedBytes = 0; this.playbackStartedAt = 0;
    this.outputItemAdded = false; this.onDrained = null; this.firstChunkLogged = false;
  }

  private stopDrain() { if (this.drainTimer) { clearTimeout(this.drainTimer); this.drainTimer = null; } }
}

// ── Routes ────────────────────────────────────────────────────────────────
app.get("/health", (_req, res) => res.json({ status: "ok", vad: VAD_CONFIG }));

app.post("/twiml", (_req, res) => {
  if (!process.env.HOSTNAME) { res.status(500).send("HOSTNAME not set"); return; }
  const callId = generateSecureId("call");
  const hostname = process.env.HOSTNAME.replace(/^https?:\/\//, "").replace(/\/$/, "");
  const callerNumber = _req.body?.From || "";
  const phoneNumberDialed = _req.body?.To || "";
  console.log(`${ts()} [${callId}] incoming call to=${phoneNumberDialed} from=${callerNumber}`);
  // Pass To/From via <Parameter> tags — available in msg.start.customParameters
  res.status(200).type("text/xml").end(
    `<Response><Connect><Stream url="wss://${hostname}/media-stream/${callId}"><Parameter name="To" value="${phoneNumberDialed}" /><Parameter name="From" value="${callerNumber}" /></Stream></Connect></Response>`
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
  let lastCancelSentAt = 0;
  let lastCancelledResponseId: string | null = null;
  let lastCancelTimestamp = 0;
  let speechStartedAt = 0;
  let currentAssistantTranscript = "";
  let callUserId = "";
  let callInstructions = bot.instructions;

  let commitWatchdog: ReturnType<typeof setTimeout> | null = null;
  const clearWatchdog = () => { if (commitWatchdog) { clearTimeout(commitWatchdog); commitWatchdog = null; } };
  const startWatchdog = () => {
    clearWatchdog();
    commitWatchdog = setTimeout(() => { console.log(`${ts()} [${callId}] [WATCHDOG] no response.created within 2500ms`); }, 2500);
  };

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

  // Register Twilio handler FIRST — before any await
  ws.on("message", (raw: string) => {
    let msg: any;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.event === "start") {
      streamSid = msg.start.streamSid;
      const cp = msg.start.customParameters || {};
      const phoneNumberDialed = cp.To || cp.phoneNumberDialed || "";
      const callerNumber = cp.From || cp.callerNumber || "";
      console.log(`${ts()} [${callId}] twilio ready sid=${streamSid} to=${phoneNumberDialed} from=${callerNumber} (+${Date.now() - callStartTime}ms)`);
      console.log(`${ts()} [${callId}] start payload: ${JSON.stringify(msg.start)}`);
      notifyCallStart(callId, phoneNumberDialed, callerNumber).then(data => {
        if (data) {
          callUserId = data.user_id || "";
          callInstructions = data.instructions || bot.instructions;
          console.log(`${ts()} [${callId}] account loaded user_id=${callUserId}`);
        } else {
          console.log(`${ts()} [${callId}] account lookup returned null — using default instructions`);
        }
      }).catch((e) => { console.log(`${ts()} [${callId}] account lookup error: ${e.message}`); });
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
      notifyCallEnd(callId, Math.round((Date.now() - callStartTime) / 1000));
      xaiWs.close();
    }
  });

  ws.on("close", () => {
    clearWatchdog(); clearFillerTimer(); clearTimeout(sessionReadyWatchdog); player.clear();
    try { xaiWs.close(); } catch {}
  });

  // Connect xAI
  console.log(`${ts()} [${callId}] [CONNECT-ATTEMPT-START]`);
  const connectStart = Date.now();
  let xaiWs: any;
  try {
    xaiWs = await getOrCreateWarmWs();
  } catch (e: any) {
    console.error(`${ts()} [${callId}] xAI connect failed: ${e.message}`);
    ws.close(); return;
  }
  console.log(`${ts()} [${callId}] xAI connected (+${Date.now() - connectStart}ms, +${Date.now() - callStartTime}ms total)`);

  // Send session.update immediately — warm connections may have missed conversation.created
  console.log(`${ts()} [${callId}] [SEND] session.update dispatched`);
  xaiWs.send(JSON.stringify({
    type: "session.update",
    session: {
      instructions: callInstructions,
      voice: VOICE_ID,
      reasoning: { effort: "none" },
      turn_detection: { type: "server_vad", ...VAD_CONFIG },
      audio: {
        input: {
          format: { type: "audio/pcmu" },
          transcription: { model: "grok-transcribe", ...(KEYTERMS.length > 0 ? { keyterms: KEYTERMS } : {}) },
        },
        output: {
          format: { type: "audio/pcmu" },
          ...(OUTPUT_SPEED !== 1.0 ? { speed: OUTPUT_SPEED } : {}),
        },
      },
      ...(Object.keys(REPLACE_MAP).length > 0 ? { replace: REPLACE_MAP } : {}),
      ...(ENABLE_TOOLS ? { tools } : {}),
    },
  }));

  const sessionReadyWatchdog = setTimeout(() => {
    if (!sessionReady) console.log(`${ts()} [${callId}] [CRITICAL] session never became ready`);
  }, 3000);

  xaiWs.on("message", (data: Buffer, isBinary: boolean) => {
    if (isBinary) { clearFillerTimer(); player.enqueue(data.toString("base64"), player.getActiveResponseId() || ""); return; }
    let msg: any;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    const quiet = ["response.output_audio.delta", "response.output_audio_transcript.delta"];
    if (!quiet.includes(msg.type)) console.log(`${ts()} [${callId}] ${msg.type}`);

    switch (msg.type) {
      case "error": {
        const em: string = msg.error?.message || "";
        if (em.includes("Cancellation failed")) console.log(`${ts()} [${callId}] [EXPECTED-RACE] ${em}`);
        else console.log(`${ts()} [${callId}] [SERVER-ERROR] ${JSON.stringify(msg)}`);
        break;
      }
      case "response.output_audio.delta":
        if (msg.delta && msg.response_id) { clearFillerTimer(); player.enqueue(msg.delta, msg.response_id); }
        break;
      case "response.output_audio_transcript.delta":
        process.stdout.write(msg.delta || "");
        currentAssistantTranscript += (msg.delta || "");
        break;
      case "response.created": {
        clearWatchdog();
        const rid = msg.response?.id || "";
        const now = Date.now();
        if (lastCancelledResponseId && (now - lastCancelTimestamp) < 400)
          console.log(`${ts()} [${callId}] [FALSE-START] response_id=${rid} created ${now - lastCancelTimestamp}ms after cancel`);
        console.log(`${ts()} [${callId}] [LATENCY] response.created id=${rid} (+${Date.now() - callStartTime}ms total)`);
        player.setResponse(rid);
        clearFillerTimer();
        const fillerRid = rid;
        fillerTimer = setTimeout(() => {
          fillerTimer = null;
          if (player.getActiveResponseId() === fillerRid && player.getPlayedMs() === 0) {
            const phrase = nextFiller();
            console.log(`${ts()} [${callId}] [FILLER-INJECTED] phrase="${phrase}"`);
            xaiWs.send(JSON.stringify({ type: "conversation.item.create", item: { type: "force_message", role: "assistant", interruptible: true, content: [{ type: "output_text", text: phrase }] } }));
          }
        }, FILLER_THRESHOLD_MS);
        break;
      }
      case "response.output_item.added":
        if (msg.item?.type === "message" && msg.item?.role === "assistant") { currentItemId = msg.item.id; player.markOutputItem(); }
        break;
      case "response.done":
      case "response.cancelled": {
        process.stdout.write("\n"); clearFillerTimer();
        if (currentAssistantTranscript && callUserId) logTranscript(callId, callUserId, "assistant", currentAssistantTranscript);
        currentAssistantTranscript = "";
        // Guard against double-firing (response.done + response.cancelled both arrive sometimes)
        if (!player.getActiveResponseId()) break;
        const rid = player.getActiveResponseId();
        const qf = player.getQueuedFrameCount();
        console.log(`${ts()} [${callId}] [PACING] generation done, ~${qf * 20}ms audio still playing`);
        player.markGenerationDone(() => {
          if (rid && !player.hadOutputItem() && msg.type !== "response.cancelled")
            console.log(`${ts()} [${callId}] [FALSE-START] response_id=${rid} no output`);
          console.log(`${ts()} [${callId}] [PACING] playback complete response_id=${rid}`);
        });
        break;
      }
      case "conversation.item.input_audio_transcription.completed": {
        const transcript = msg.transcript || "";
        const turnDuration = speechStartedAt > 0 ? Date.now() - speechStartedAt : 0;
        console.log(`${ts()} [${callId}] [TRANSCRIPTION] "${transcript}" turn_duration=${turnDuration}ms`);
        // Pass item_id to deduplicate — completed can fire multiple times as corrections
        if (callUserId) logTranscript(callId, callUserId, "user", transcript, msg.item_id);
        if (turnDuration > 0 && transcript.length > 0 && (turnDuration / transcript.length) > 150)
          console.log(`${ts()} [${callId}] [LONG-TURN] ${turnDuration}ms — possible VAD noise`);
        break;
      }
      case "conversation.item.input_audio_transcription.updated":
        console.log(`${ts()} [${callId}] [TRANSCRIPTION-UPDATE] "${msg.transcript}"`);
        break;
      case "input_audio_buffer.speech_started": {
        speechStartedAt = Date.now();
        const activeRid = player.getActiveResponseId();
        if (activeRid !== null) {
          const playedMs = player.interrupt();
          if (streamSid) ws.send(JSON.stringify({ event: "clear", streamSid }));
          const now = Date.now();
          if (now - lastCancelSentAt < CANCEL_DEBOUNCE_MS) {
            console.log(`${ts()} [${callId}] [BARGE-IN-SUPPRESSED] debounce played=${playedMs}ms`);
          } else {
            lastCancelSentAt = now; lastCancelledResponseId = activeRid; lastCancelTimestamp = now;
            console.log(`${ts()} [${callId}] [BARGE-IN] cancelling response_id=${activeRid} played=${playedMs}ms`);
            xaiWs.send(JSON.stringify({ type: "response.cancel" }));
            if (currentItemId && playedMs > 0)
              xaiWs.send(JSON.stringify({ type: "conversation.item.truncate", item_id: currentItemId, content_index: 0, audio_end_ms: playedMs }));
          }
        } else {
          console.log(`${ts()} [${callId}] [speech_started] no active response — normal turn`);
        }
        break;
      }
      case "input_audio_buffer.speech_stopped":
        console.log(`${ts()} [${callId}] [LATENCY] speech_stopped (+${Date.now() - speechStartedAt}ms)`);
        break;
      case "input_audio_buffer.committed":
        console.log(`${ts()} [${callId}] [LATENCY] committed — requesting response`);
        // Guard: don't send response.create if one is already in progress
        if (!player.getActiveResponseId()) {
          startWatchdog(); xaiWs.send(JSON.stringify({ type: "response.create" }));
        } else {
          console.log(`${ts()} [${callId}] committed but response already active — skipping`);
        }
        break;
      case "conversation.item.truncated":
        console.log(`${ts()} [${callId}] item truncated at ${msg.audio_end_ms}ms`);
        break;
      case "session.updated":
        sessionReady = true; clearTimeout(sessionReadyWatchdog);
        console.log(`${ts()} [${callId}] session ready (+${Date.now() - callStartTime}ms total)`);
        xaiWs.send(JSON.stringify({ type: "conversation.item.create", item: { type: "force_message", role: "assistant", interruptible: true, content: [{ type: "output_text", text: "Hey! How can I help you today?" }] } }));
        console.log(`${ts()} [${callId}] [SEND] force_message greeting dispatched`);
        break;
      case "conversation.created":
        console.log(`${ts()} [${callId}] conversation.created`);
        break;
      case "response.output_item.done":
        if (msg.item?.type === "function_call") {
          (async () => {
            let args: Record<string, any> = {};
            try { args = JSON.parse(msg.item.arguments || "{}"); } catch {}
            console.log(`${ts()} [${callId}] fn: ${msg.item.name}(${JSON.stringify(args)})`);
            const result = await handleToolCall(msg.item.name, args);
            xaiWs.send(JSON.stringify({ type: "conversation.item.create", item: { type: "function_call_output", call_id: msg.item.call_id, output: result } }));
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

      const BYTES_PER_MS = 8;
      let outTotalBytes = 0, outPlaybackStartedAt = 0;
      let outDrainTimer: ReturnType<typeof setTimeout> | null = null;

      const enqueueOutAudio = (b64: string, responseId: string) => {
        if (responseId !== currentResponseId) return;
        const bytes = Buffer.from(b64, "base64");
        if (outPlaybackStartedAt === 0) outPlaybackStartedAt = Date.now();
        outTotalBytes += bytes.length;
        if (streamSid) ws.send(JSON.stringify({ event: "media", streamSid, media: { payload: b64 } }));
        playedMs = Math.min(Date.now() - outPlaybackStartedAt, Math.round(outTotalBytes / BYTES_PER_MS));
      };

      const interruptOutAudio = () => {
        if (outDrainTimer) { clearTimeout(outDrainTimer); outDrainTimer = null; }
        outTotalBytes = 0; outPlaybackStartedAt = 0;
      };

      const markOutDone = (cb: () => void) => {
        const totalMs = Math.round(outTotalBytes / BYTES_PER_MS);
        const elapsed = outPlaybackStartedAt > 0 ? Date.now() - outPlaybackStartedAt : 0;
        const remaining = Math.max(0, totalMs - elapsed);
        console.log(`${ts()} [OUTBOUND][${callSid}] [PACING] ~${remaining}ms audio still playing`);
        if (remaining <= 0) { cb(); return; }
        outDrainTimer = setTimeout(() => { outDrainTimer = null; cb(); }, remaining + 100);
      };

      xaiWs.on("open", () => {
        console.log(`${ts()} [OUTBOUND][${callSid}] [SEND] session.update dispatched`);
        xaiWs.send(JSON.stringify({
          type: "session.update",
          session: {
            instructions: OUTBOUND_INSTRUCTIONS, voice: VOICE_ID, reasoning: { effort: "none" },
            turn_detection: { type: "server_vad", ...VAD_CONFIG },
            audio: {
              input: { format: { type: "audio/pcmu" }, transcription: { model: "grok-transcribe" } },
              output: { format: { type: "audio/pcmu" }, ...(OUTPUT_SPEED !== 1.0 ? { speed: OUTPUT_SPEED } : {}) },
            },
          },
        }));
      });

      xaiWs.on("message", (d: Buffer, isBinary: boolean) => {
        if (isBinary) { if (streamSid && currentResponseId) enqueueOutAudio(d.toString("base64"), currentResponseId); return; }
        let m: any;
        try { m = JSON.parse(d.toString()); } catch { return; }
        if (m.type !== "response.output_audio.delta") console.log(`${ts()} [OUTBOUND][${callSid}] ${m.type}`);

        switch (m.type) {
          case "error": {
            const em: string = m.error?.message || "";
            if (em.includes("Cancellation failed")) console.log(`${ts()} [OUTBOUND] [EXPECTED-RACE] ${em}`);
            else console.log(`${ts()} [OUTBOUND] [SERVER-ERROR] ${JSON.stringify(m)}`);
            break;
          }
          case "session.updated": sessionReady = true; xaiWs.send(JSON.stringify({ type: "response.create" })); break;
          case "response.created": {
            if (commitWatchdog) { clearTimeout(commitWatchdog); commitWatchdog = null; }
            const now = Date.now();
            if (lastCancelledResponseId && (now - lastCancelTimestamp) < 400)
              console.log(`${ts()} [OUTBOUND][${callSid}] [FALSE-START] response_id=${m.response?.id}`);
            turnCount++; currentResponseId = m.response?.id || null; playedMs = 0; outputItemAdded = false;
            break;
          }
          case "response.output_item.added":
            if (m.item?.type === "message" && m.item?.role === "assistant") { currentItemId = m.item.id; outputItemAdded = true; }
            break;
          case "response.output_audio.delta":
            if (m.delta && m.response_id === currentResponseId) enqueueOutAudio(m.delta, m.response_id);
            break;
          case "response.done":
          case "response.cancelled": {
            const doneRid = currentResponseId;
            markOutDone(() => {
              if (doneRid && !outputItemAdded) console.log(`${ts()} [OUTBOUND][${callSid}] [FALSE-START] ${doneRid} no output`);
              currentResponseId = null;
              console.log(`${ts()} [OUTBOUND][${callSid}] [PACING] playback complete`);
              if (turnCount >= MAX_TURNS) setTimeout(() => { xaiWs?.close(); ws.close(); }, 3000);
            });
            break;
          }
          case "input_audio_buffer.speech_started": {
            speechStartedAt = Date.now();
            const rid = currentResponseId;
            if (rid !== null) {
              interruptOutAudio(); currentResponseId = null;
              if (streamSid) ws.send(JSON.stringify({ event: "clear", streamSid }));
              const now = Date.now();
              if (now - lastCancelSentAt < CANCEL_DEBOUNCE_MS) {
                console.log(`${ts()} [OUTBOUND][${callSid}] [BARGE-IN-SUPPRESSED]`);
              } else {
                lastCancelSentAt = now; lastCancelledResponseId = rid; lastCancelTimestamp = now;
                console.log(`${ts()} [OUTBOUND][${callSid}] [BARGE-IN] cancelling ${rid} played=${playedMs}ms`);
                xaiWs.send(JSON.stringify({ type: "response.cancel" }));
                if (currentItemId && playedMs > 0)
                  xaiWs.send(JSON.stringify({ type: "conversation.item.truncate", item_id: currentItemId, content_index: 0, audio_end_ms: playedMs }));
              }
            } else {
              console.log(`${ts()} [OUTBOUND][${callSid}] [speech_started] normal turn`);
            }
            break;
          }
          case "input_audio_buffer.speech_stopped":
            console.log(`${ts()} [OUTBOUND][${callSid}] speech_stopped (+${Date.now() - speechStartedAt}ms)`);
            break;
          case "input_audio_buffer.committed":
            commitWatchdog = setTimeout(() => { console.log(`${ts()} [OUTBOUND][${callSid}] [WATCHDOG] no response.created within 2500ms`); }, 2500);
            xaiWs.send(JSON.stringify({ type: "response.create" }));
            break;
          case "conversation.item.input_audio_transcription.completed":
            console.log(`${ts()} [OUTBOUND][${callSid}] [TRANSCRIPTION] "${m.transcript}"`);
            break;
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
  console.log(`Voice: ${VOICE_ID} | Speed: ${OUTPUT_SPEED} | Tools: ${ENABLE_TOOLS ? "on" : "off"}`);
  console.log(`VAD: threshold=${VAD_CONFIG.threshold} silence=${VAD_CONFIG.silence_duration_ms}ms prefix=${VAD_CONFIG.prefix_padding_ms}ms`);
  console.log(`Cancel debounce: ${CANCEL_DEBOUNCE_MS}ms | Filler: ${FILLER_THRESHOLD_MS}ms | Keyterms: ${KEYTERMS.length > 0 ? KEYTERMS.join(",") : "none"}`);
  console.log(`xAI warm pool: seeded`);
});
