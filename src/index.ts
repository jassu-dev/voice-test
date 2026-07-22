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

function generateSecureId(prefix: string): string {
  return `${prefix}_${crypto.randomBytes(8).toString("hex")}`;
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
// Tracks per-response audio queue, playback position, and handles barge-in.
// For telephony (Twilio), "playback" = sending media frames to Twilio.
// Each mulaw chunk at 8kHz = (bytes / 1) ms (1 byte per ms at 8kHz mulaw).
class AudioPlayer {
  private queue: Array<{ payload: string; bytes: number }> = [];
  private currentResponseId: string | null = null;
  private playedMs = 0;
  private callId: string;
  private sendFn: (payload: string) => void;

  constructor(callId: string, sendFn: (payload: string) => void) {
    this.callId = callId;
    this.sendFn = sendFn;
  }

  // Called for every response.output_audio.delta — only play if responseId matches current
  play(payload: string, responseId: string) {
    if (responseId !== this.currentResponseId) return; // stale delta — discard
    const bytes = Buffer.from(payload, "base64").length;
    this.queue.push({ payload, bytes });
    // Flush immediately — stream deltas to Twilio as they arrive
    this.flush();
  }

  // Set active response — call on response.created
  setResponse(responseId: string) {
    this.currentResponseId = responseId;
    this.playedMs = 0;
    this.queue = [];
  }

  // Hard stop + flush — call on barge-in
  interrupt(): number {
    const playedMs = this.playedMs;
    this.queue = []; // flush unplayed queue
    this.currentResponseId = null;
    return playedMs;
  }

  // Clear current response on normal end
  clear() {
    this.queue = [];
    this.currentResponseId = null;
    this.playedMs = 0;
  }

  getCurrentResponseId() { return this.currentResponseId; }
  getPlayedMs() { return this.playedMs; }

  private flush() {
    while (this.queue.length > 0) {
      const chunk = this.queue.shift()!;
      this.sendFn(chunk.payload);
      // mulaw 8kHz: 8000 bytes/sec = 8 bytes/ms
      this.playedMs += Math.round(chunk.bytes / 8);
    }
  }
}

app.get("/health", (_req, res) => res.json({ status: "ok" }));

app.post("/twiml", (_req, res) => {
  if (!process.env.HOSTNAME) { res.status(500).send("HOSTNAME not set"); return; }
  const callId = generateSecureId("call");
  const hostname = process.env.HOSTNAME.replace(/^https?:\/\//, "").replace(/\/$/, "");
  console.log(`[${callId}] incoming call`);
  res.status(200).type("text/xml").end(
    `<Response><Connect><Stream url="wss://${hostname}/media-stream/${callId}" /></Connect></Response>`
  );
});

app.post("/call-status", (_req, res) => res.status(200).send());

// ── Inbound Media Stream ───────────────────────────────────────────────────
app.ws("/media-stream/:callId", async (ws: any, req) => {
  const callId = req.params.callId as string;
  console.log(`[${callId}] CALL STARTED`);

  const WebSocket = require("ws");
  let streamSid = "";
  let sessionReady = false;
  let currentItemId: string | null = null; // conversation item id for truncation
  const pendingAudio: string[] = [];       // audio buffered before streamSid is set

  // ── Send audio to Twilio ─────────────────────────────────────────────────
  const sendToTwilio = (payload: string) => {
    if (streamSid) {
      ws.send(JSON.stringify({ event: "media", streamSid, media: { payload } }), (err: any) => {
        if (err) console.log(`[${callId}] send error: ${err.message}`);
      });
    } else {
      pendingAudio.push(payload);
    }
  };

  const player = new AudioPlayer(callId, sendToTwilio);

  // ── Register Twilio message handler BEFORE any await ────────────────────
  ws.on("message", (raw: string) => {
    let msg: any;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.event === "start") {
      streamSid = msg.start.streamSid;
      console.log(`[${callId}] twilio ready sid=${streamSid}`);
      if (pendingAudio.length > 0) {
        console.log(`[${callId}] flushing ${pendingAudio.length} buffered chunks`);
        for (const p of pendingAudio) {
          ws.send(JSON.stringify({ event: "media", streamSid, media: { payload: p } }));
        }
        pendingAudio.length = 0;
      }
    } else if (msg.event === "media") {
      if (msg.media?.track !== "inbound") return;
      if (!sessionReady || xaiWs.readyState !== 1) return;
      xaiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio: msg.media.payload }));
    } else if (msg.event === "stop") {
      console.log(`[${callId}] call ended`);
      xaiWs.close();
    }
  });

  ws.on("close", () => { try { xaiWs.close(); } catch {} });

  // ── Connect to xAI ───────────────────────────────────────────────────────
  const xaiWs = new WebSocket(API_URL, {
    headers: { Authorization: `Bearer ${XAI_API_KEY}` },
  });

  await new Promise<void>((resolve, reject) => {
    const t = setTimeout(() => { xaiWs.close(); reject(new Error("xAI timeout")); }, 10000);
    xaiWs.on("open", () => { clearTimeout(t); console.log(`[${callId}] xAI connected`); resolve(); });
    xaiWs.on("error", (e: any) => { clearTimeout(t); reject(e); });
  });

  // ── Handle xAI messages ──────────────────────────────────────────────────
  xaiWs.on("message", (data: Buffer, isBinary: boolean) => {
    // Binary = raw audio frame
    if (isBinary) {
      const payload = data.toString("base64");
      const responseId = player.getCurrentResponseId() || "";
      player.play(payload, responseId);
      return;
    }

    let msg: any;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    if (msg.type !== "response.output_audio.delta") {
      console.log(`[${callId}] ${msg.type}`);
    }

    switch (msg.type) {

      // ── Audio delta: stream immediately via player ───────────────────────
      case "response.output_audio.delta":
        if (msg.delta && msg.response_id) {
          player.play(msg.delta, msg.response_id);
        }
        break;

      // ── New response starting ────────────────────────────────────────────
      case "response.created":
        console.log(`[${callId}] response started id=${msg.response?.id}`);
        player.setResponse(msg.response?.id || "");
        break;

      // ── Track conversation item id for truncation ────────────────────────
      case "response.output_item.added":
        if (msg.item?.type === "message" && msg.item?.role === "assistant") {
          currentItemId = msg.item.id;
        }
        break;

      case "response.done":
      case "response.cancelled":
        player.clear();
        break;

      case "response.output_audio_transcript.delta":
        process.stdout.write(msg.delta || "");
        break;

      case "conversation.item.input_audio_transcription.completed":
        if (msg.transcript) console.log(`\n[${callId}] User: "${msg.transcript}"`);
        break;

      // ── BARGE-IN ─────────────────────────────────────────────────────────
      // speech_started fires THE INSTANT VAD detects speech — act immediately
      case "input_audio_buffer.speech_started": {
        const activeResponseId = player.getCurrentResponseId();
        const playedMs = player.interrupt(); // hard stop + flush queue

        console.log(`[${callId}] [BARGE-IN] cancelling response_id=${activeResponseId} at ${playedMs}ms`);

        // 1. Clear Twilio's jitter buffer so audio stops instantly on caller's end
        if (streamSid) ws.send(JSON.stringify({ event: "clear", streamSid }));

        // 2. Cancel server-side response generation
        if (activeResponseId) {
          xaiWs.send(JSON.stringify({ type: "response.cancel" }));
        }

        // 3. Truncate conversation item to only what was actually played
        // This keeps conversation history accurate for future turns
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
        console.log(`[${callId}] speech_stopped`);
        break;

      // ── After commit: request response ──────────────────────────────────
      case "input_audio_buffer.committed":
        console.log(`[${callId}] committed`);
        xaiWs.send(JSON.stringify({ type: "response.create" }));
        break;

      // ── Session configured: send greeting ───────────────────────────────
      case "session.updated":
        sessionReady = true;
        console.log(`[${callId}] session ready`);
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
            reasoning: { effort: "none" },
            turn_detection: {
              type: "server_vad",
              threshold: 0.6,           // lower = catches speech on noisy phone lines
              silence_duration_ms: 500, // natural conversational pause
              prefix_padding_ms: 300,   // don't clip first syllable
            },
            audio: {
              input:  { format: { type: "audio/pcmu" } }, // Twilio native
              output: { format: { type: "audio/pcmu" } }, // Twilio native
            },
            ...(ENABLE_TOOLS ? { tools } : {}),
          },
        }));
        break;

      // ── Function tool call ───────────────────────────────────────────────
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
            // Wait for current playback to finish before requesting next response
            // (avoids audio overlap on tool calls)
            const waitForPlayback = () => new Promise<void>(res => {
              const check = () => {
                if (!player.getCurrentResponseId()) { res(); return; }
                setTimeout(check, 50);
              };
              check();
            });
            await waitForPlayback();
            xaiWs.send(JSON.stringify({ type: "response.create" }));
          })();
        }
        break;

      case "conversation.item.truncated":
        console.log(`[${callId}] item truncated at ${msg.audio_end_ms}ms`);
        break;

      case "error":
        console.log(`[${callId}] ERROR: ${msg.error?.message}`);
        break;
    }
  });

  xaiWs.on("error", (e: any) => console.log(`[${callId}] ws error: ${e?.message}`));
  xaiWs.on("close", (code: number) => console.log(`[${callId}] ws closed: ${code}`));
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
      console.log(`[OUTBOUND][${callSid}] started`);

      xaiWs = new WebSocket(API_URL, { headers: { Authorization: `Bearer ${XAI_API_KEY}` } });

      xaiWs.on("open", () => {
        xaiWs.send(JSON.stringify({
          type: "session.update",
          session: {
            instructions: OUTBOUND_INSTRUCTIONS,
            voice: "atlas",
            reasoning: { effort: "none" },
            turn_detection: {
              type: "server_vad",
              threshold: 0.6,
              silence_duration_ms: 500,
              prefix_padding_ms: 300,
            },
            audio: {
              input:  { format: { type: "audio/pcmu" } },
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
        if (m.type !== "response.output_audio.delta") console.log(`[OUTBOUND][${callSid}] ${m.type}`);

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
            console.log(`[OUTBOUND][${callSid}] [BARGE-IN] cancelling response_id=${rid} at ${pms}ms`);
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
          case "response.output_audio_transcript.delta":
            process.stdout.write(m.delta || "");
            break;
          case "error":
            console.log(`[OUTBOUND] ERROR: ${m.error?.message}`);
            break;
        }
      });

      xaiWs.on("close", () => console.log(`[OUTBOUND][${callSid}] xAI closed`));

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
app.listen(port, () => console.log(`Server on http://localhost:${port}`));
