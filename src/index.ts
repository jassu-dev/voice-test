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

// ── VAD config — tune these per deployment without touching core logic ─────
const VAD_CONFIG = {
  threshold: parseFloat(process.env.VAD_THRESHOLD || "0.6"),
  silence_duration_ms: parseInt(process.env.VAD_SILENCE_MS || "500"),
  prefix_padding_ms: parseInt(process.env.VAD_PREFIX_MS || "300"),
};

function generateSecureId(prefix: string): string {
  return `${prefix}_${crypto.randomBytes(8).toString("hex")}`;
}

function ts(): string {
  return new Date().toISOString().slice(11, 23); // HH:MM:SS.mmm
}

const tools = [
  {
    type: "function",
    name: "generate_random_number",
    description: "Generate a random number between min and max values",
    parameters: {
      type: "object",
      properties: {
        min: { type: "number", description: "Minimum value (inclusive)" },
        max: { type: "number", description: "Maximum value (inclusive)" },
      },
      required: ["min", "max"],
    },
  },
];

async function handleToolCall(name: string, args: Record<string, any>): Promise<string> {
  switch (name) {
    case "generate_random_number": {
      const min = Math.ceil(args.min);
      const max = Math.floor(args.max);
      return JSON.stringify({ result: Math.floor(Math.random() * (max - min + 1)) + min });
    }
    default:
      return JSON.stringify({ error: `Unknown tool: ${name}` });
  }
}

// ── Audio Playback Manager ─────────────────────────────────────────────────
// Tracks per-response audio queue, playback ms, and handles barge-in.
// mulaw 8kHz: 8000 bytes/sec = 8 bytes/ms
class AudioPlayer {
  private queue: Array<{ payload: string; bytes: number }> = [];
  private activeResponseId: string | null = null;
  private playedMs: number = 0;
  private firstChunkPlayed: boolean = false;
  private sendFn: (payload: string) => void;
  private callId: string;

  constructor(callId: string, sendFn: (payload: string) => void) {
    this.callId = callId;
    this.sendFn = sendFn;
  }

  setResponse(responseId: string) {
    this.activeResponseId = responseId;
    this.playedMs = 0;
    this.firstChunkPlayed = false;
    this.queue = [];
  }

  // Play delta — discard if stale response_id (guards race conditions)
  play(payload: string, responseId: string) {
    if (responseId !== this.activeResponseId) return;
    const bytes = Buffer.from(payload, "base64").length;
    this.queue.push({ payload, bytes });
    this.flush();
  }

  // Hard interrupt: flush queue, return playedMs for truncation
  interrupt(): number {
    const played = this.playedMs;
    this.queue = [];
    this.activeResponseId = null;
    this.firstChunkPlayed = false;
    return played;
  }

  clear() {
    this.queue = [];
    this.activeResponseId = null;
    this.playedMs = 0;
    this.firstChunkPlayed = false;
  }

  getActiveResponseId() { return this.activeResponseId; }
  getPlayedMs() { return this.playedMs; }

  private flush() {
    while (this.queue.length > 0) {
      const chunk = this.queue.shift()!;
      if (!this.firstChunkPlayed) {
        console.log(`${ts()} [${this.callId}] [LATENCY] first audio chunk playing`);
        this.firstChunkPlayed = true;
      }
      this.sendFn(chunk.payload);
      this.playedMs += Math.round(chunk.bytes / 8);
    }
  }
}

app.get("/health", (_req, res) => res.json({ status: "ok", vad: VAD_CONFIG }));

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
  console.log(`${ts()} [${callId}] CALL STARTED`);

  const WebSocket = require("ws");
  let streamSid = "";
  let sessionReady = false;
  let currentItemId: string | null = null;
  const pendingAudio: string[] = [];
  let micChunkCount = 0;

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

  // ── Twilio handler registered FIRST — never misses start event ───────────
  ws.on("message", (raw: string) => {
    let msg: any;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.event === "start") {
      streamSid = msg.start.streamSid;
      console.log(`${ts()} [${callId}] twilio ready sid=${streamSid}`);
      if (pendingAudio.length > 0) {
        console.log(`${ts()} [${callId}] flushing ${pendingAudio.length} buffered chunks`);
        for (const p of pendingAudio) {
          ws.send(JSON.stringify({ event: "media", streamSid, media: { payload: p } }));
        }
        pendingAudio.length = 0;
      }

    } else if (msg.event === "media") {
      if (msg.media?.track !== "inbound") return;
      if (!sessionReady || xaiWs.readyState !== 1) return;

      // Stream mic audio CONTINUOUSLY — no gating, no client-side VAD
      // Let server_vad decide; gating here causes word loss
      if (micChunkCount === 0) {
        console.log(`${ts()} [${callId}] [LATENCY] first mic chunk sent to xAI`);
      }
      micChunkCount++;
      // NOTE: Twilio sends audio/x-mulaw 8kHz — matches session.update audio.input format
      // No resampling needed. Pass payload directly.
      xaiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio: msg.media.payload }));

    } else if (msg.event === "stop") {
      console.log(`${ts()} [${callId}] call ended`);
      xaiWs.close();
    }
  });

  ws.on("close", () => { try { xaiWs.close(); } catch {} });

  // ── Connect xAI ──────────────────────────────────────────────────────────
  const xaiWs = new WebSocket(API_URL, {
    headers: { Authorization: `Bearer ${XAI_API_KEY}` },
  });

  await new Promise<void>((resolve, reject) => {
    const t = setTimeout(() => { xaiWs.close(); reject(new Error("xAI timeout")); }, 10000);
    xaiWs.on("open", () => {
      clearTimeout(t);
      console.log(`${ts()} [${callId}] xAI connected`);
      resolve();
    });
    xaiWs.on("error", (e: any) => { clearTimeout(t); reject(e); });
  });

  // ── xAI message handler ───────────────────────────────────────────────────
  xaiWs.on("message", (data: Buffer, isBinary: boolean) => {
    // Binary frame = raw audio from xAI (binary transport)
    if (isBinary) {
      const payload = data.toString("base64");
      const rid = player.getActiveResponseId() || "";
      player.play(payload, rid);
      return;
    }

    let msg: any;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    const skip = ["response.output_audio.delta", "response.output_audio_transcript.delta"];
    if (!skip.includes(msg.type)) {
      console.log(`${ts()} [${callId}] ${msg.type}`);
    }

    switch (msg.type) {

      case "response.output_audio.delta":
        // Stream immediately on first delta — don't buffer
        if (msg.delta && msg.response_id) {
          player.play(msg.delta, msg.response_id);
        }
        break;

      case "response.output_audio_transcript.delta":
        process.stdout.write(msg.delta || "");
        break;

      case "response.created":
        console.log(`${ts()} [${callId}] [LATENCY] response.created id=${msg.response?.id}`);
        player.setResponse(msg.response?.id || "");
        break;

      case "response.output_item.added":
        if (msg.item?.type === "message" && msg.item?.role === "assistant") {
          currentItemId = msg.item.id;
        }
        break;

      case "response.done":
      case "response.cancelled":
        process.stdout.write("\n");
        player.clear();
        break;

      // ── Transcription — primary debug tool for speech quality ───────────
      case "conversation.item.input_audio_transcription.completed":
        console.log(`${ts()} [${callId}] [TRANSCRIPTION] "${msg.transcript}" (item=${msg.item_id})`);
        break;

      // xAI uses "updated" (cumulative) not "delta" for grok-transcribe
      case "conversation.item.input_audio_transcription.updated":
        console.log(`${ts()} [${callId}] [TRANSCRIPTION-UPDATE] "${msg.transcript}"`);
        break;

      // ── BARGE-IN ─────────────────────────────────────────────────────────
      case "input_audio_buffer.speech_started": {
        const activeRid = player.getActiveResponseId();
        const playedMs = player.interrupt(); // hard stop + flush — no output_audio.clear sent here

        console.log(`${ts()} [${callId}] [BARGE-IN] response_id=${activeRid} played=${playedMs}ms`);

        // 1. Clear Twilio jitter buffer — stops audio on caller's end instantly
        if (streamSid) ws.send(JSON.stringify({ event: "clear", streamSid }));

        // 2. Cancel server response generation
        // NOTE: we do NOT send input_audio_buffer.clear here — that would destroy
        // the user's speech that triggered this barge-in
        if (activeRid) {
          xaiWs.send(JSON.stringify({ type: "response.cancel" }));
        }

        // 3. Truncate conversation history to what was actually heard by caller
        if (currentItemId && playedMs > 0) {
          xaiWs.send(JSON.stringify({
            type: "conversation.item.truncate",
            item_id: currentItemId,
            content_index: 0,
            audio_end_ms: playedMs,
          }));
        }
        break;
      }

      case "input_audio_buffer.speech_stopped":
        console.log(`${ts()} [${callId}] [LATENCY] speech_stopped`);
        break;

      case "input_audio_buffer.committed":
        console.log(`${ts()} [${callId}] [LATENCY] buffer committed — requesting response`);
        // No artificial delay — request response immediately
        xaiWs.send(JSON.stringify({ type: "response.create" }));
        break;

      case "conversation.item.truncated":
        console.log(`${ts()} [${callId}] item truncated at ${msg.audio_end_ms}ms`);
        break;

      // ── Session ready: greeting ──────────────────────────────────────────
      case "session.updated":
        sessionReady = true;
        console.log(`${ts()} [${callId}] session ready — VAD threshold=${VAD_CONFIG.threshold} silence=${VAD_CONFIG.silence_duration_ms}ms`);
        // force_message = pure TTS, no LLM round-trip = fastest first audio
        xaiWs.send(JSON.stringify({
          type: "conversation.item.create",
          item: {
            type: "force_message",
            role: "assistant",
            interruptible: true,
            content: [{ type: "output_text", text: "Hey! How can I help you today?" }],
          },
        }));
        break;

      // ── Configure session ────────────────────────────────────────────────
      case "conversation.created":
        xaiWs.send(JSON.stringify({
          type: "session.update",
          session: {
            instructions: bot.instructions,
            voice: "celeste",
            // reasoning none = fastest responses for conversational use
            reasoning: { effort: "none" },
            turn_detection: {
              type: "server_vad",
              ...VAD_CONFIG, // threshold, silence_duration_ms, prefix_padding_ms
            },
            audio: {
              // audio/pcmu = Twilio native mulaw 8kHz — no transcoding needed
              // IMPORTANT: actual Twilio audio IS mulaw 8kHz, matching this declaration
              // A mismatch here would cause garbled ASR — confirm with Twilio mediaFormat
              input: {
                format: { type: "audio/pcmu" },
                // grok-transcribe enables transcription events for debugging speech quality
                transcription: {
                  model: "grok-transcribe",
                  // Add business-specific terms here to improve recognition accuracy
                  // keyterms: ["your-product-name", "specific-term"],
                },
              },
              output: { format: { type: "audio/pcmu" } },
            },
            ...(ENABLE_TOOLS ? { tools } : {}),
          },
        }));
        break;

      // ── Function tool calls ──────────────────────────────────────────────
      case "response.output_item.done":
        if (msg.item?.type === "function_call") {
          (async () => {
            let args: Record<string, any> = {};
            try { args = JSON.parse(msg.item.arguments || "{}"); } catch {}
            const result = await handleToolCall(msg.item.name, args);
            xaiWs.send(JSON.stringify({
              type: "conversation.item.create",
              item: { type: "function_call_output", call_id: msg.item.call_id, output: result },
            }));
            // Wait for current turn's audio to finish before triggering next response
            // Prevents audio overlap on tool calls
            const waitForPlayback = () => new Promise<void>(res => {
              const check = () => {
                if (!player.getActiveResponseId()) { res(); return; }
                setTimeout(check, 50);
              };
              check();
            });
            await waitForPlayback();
            xaiWs.send(JSON.stringify({ type: "response.create" }));
          })();
        }
        break;

      case "error":
        console.log(`${ts()} [${callId}] ERROR: ${msg.error?.message}`);
        break;
    }
  });

  xaiWs.on("error", (e: any) => console.log(`${ts()} [${callId}] ws error: ${e?.message}`));
  xaiWs.on("close", (code: number) => console.log(`${ts()} [${callId}] ws closed: ${code}`));
});

// ── Outbound ───────────────────────────────────────────────────────────────
const OUTBOUND_INSTRUCTIONS = `You are an outbound AI phone agent. YOU speak first. Greet warmly, keep replies short and conversational.`;

app.post("/outbound-twiml", (_req, res) => {
  const hostname = (process.env.HOSTNAME || "").replace(/^https?:\/\//, "").replace(/\/$/, "");
  res.type("text/xml").send(
    `<Response><Connect><Stream url="wss://${hostname}/outbound-stream" /></Connect></Response>`
  );
});

app.ws("/outbound-stream", (ws: any) => {
  const WebSocket = require("ws");
  const MAX_TURNS = 10;
  let streamSid = "";
  let callSid = "";
  let xaiWs: any = null;
  let sessionReady = false;
  let turnCount = 0;
  let currentResponseId: string | null = null;
  let currentItemId: string | null = null;
  let playedMs = 0;

  ws.on("message", (raw: string) => {
    let msg: any;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.event === "start") {
      callSid = msg.start.callSid;
      streamSid = msg.start.streamSid;
      console.log(`${ts()} [OUTBOUND][${callSid}] started`);

      xaiWs = new WebSocket(API_URL, { headers: { Authorization: `Bearer ${XAI_API_KEY}` } });

      xaiWs.on("open", () => {
        xaiWs.send(JSON.stringify({
          type: "session.update",
          session: {
            instructions: OUTBOUND_INSTRUCTIONS,
            voice: "atlas",
            reasoning: { effort: "none" },
            turn_detection: { type: "server_vad", ...VAD_CONFIG },
            audio: {
              input: {
                format: { type: "audio/pcmu" },
                transcription: { model: "grok-transcribe" },
              },
              output: { format: { type: "audio/pcmu" } },
            },
          },
        }));
      });

      xaiWs.on("message", (d: Buffer, isBinary: boolean) => {
        if (isBinary) {
          const payload = d.toString("base64");
          if (streamSid && currentResponseId) {
            ws.send(JSON.stringify({ event: "media", streamSid, media: { payload } }));
            playedMs += Math.round(d.length / 8);
          }
          return;
        }
        let m: any;
        try { m = JSON.parse(d.toString()); } catch { return; }
        if (m.type !== "response.output_audio.delta") {
          console.log(`${ts()} [OUTBOUND][${callSid}] ${m.type}`);
        }

        switch (m.type) {
          case "session.updated":
            sessionReady = true;
            xaiWs.send(JSON.stringify({ type: "response.create" }));
            break;
          case "response.created":
            turnCount++;
            currentResponseId = m.response?.id || null;
            playedMs = 0;
            break;
          case "response.output_item.added":
            if (m.item?.type === "message" && m.item?.role === "assistant") currentItemId = m.item.id;
            break;
          case "response.output_audio.delta":
            if (m.delta && m.response_id === currentResponseId && streamSid) {
              ws.send(JSON.stringify({ event: "media", streamSid, media: { payload: m.delta } }));
              playedMs += Math.round(Buffer.from(m.delta, "base64").length / 8);
            }
            break;
          case "response.done":
          case "response.cancelled":
            currentResponseId = null;
            if (turnCount >= MAX_TURNS) setTimeout(() => { xaiWs?.close(); ws.close(); }, 3000);
            break;
          case "input_audio_buffer.speech_started": {
            const rid = currentResponseId;
            const pms = playedMs;
            console.log(`${ts()} [OUTBOUND][${callSid}] [BARGE-IN] response_id=${rid} played=${pms}ms`);
            currentResponseId = null;
            if (streamSid) ws.send(JSON.stringify({ event: "clear", streamSid }));
            if (rid) xaiWs.send(JSON.stringify({ type: "response.cancel" }));
            if (currentItemId && pms > 0) {
              xaiWs.send(JSON.stringify({
                type: "conversation.item.truncate",
                item_id: currentItemId,
                content_index: 0,
                audio_end_ms: pms,
              }));
            }
            break;
          }
          case "input_audio_buffer.committed":
            xaiWs.send(JSON.stringify({ type: "response.create" }));
            break;
          case "conversation.item.input_audio_transcription.completed":
            console.log(`${ts()} [OUTBOUND][${callSid}] [TRANSCRIPTION] "${m.transcript}"`);
            break;
          case "response.output_audio_transcript.delta":
            process.stdout.write(m.delta || "");
            break;
          case "error":
            console.log(`${ts()} [OUTBOUND] ERROR: ${m.error?.message}`);
            break;
        }
      });

      xaiWs.on("close", () => console.log(`${ts()} [OUTBOUND][${callSid}] xAI closed`));

    } else if (msg.event === "media" && msg.media?.track === "inbound") {
      if (xaiWs && sessionReady && xaiWs.readyState === 1)
        xaiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio: msg.media.payload }));
    } else if (msg.event === "stop") {
      xaiWs?.close();
    }
  });

  ws.on("close", () => { try { xaiWs?.close(); } catch {} });
});

const port = process.env.PORT || "3000";
app.listen(port, () => {
  console.log(`Server on http://localhost:${port}`);
  console.log(`VAD config: threshold=${VAD_CONFIG.threshold} silence=${VAD_CONFIG.silence_duration_ms}ms prefix=${VAD_CONFIG.prefix_padding_ms}ms`);
});
