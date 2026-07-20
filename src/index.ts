import "dotenv-flow/config";
import express from "express";
import ExpressWs from "express-ws";
import * as crypto from "crypto";
import bot from "./bot";
import { TwilioMediaStreamWebsocket } from "./twilio";

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

app.get("/health", (_req, res) => res.json({ status: "ok" }));

app.post("/twiml", async (_req, res) => {
  if (!process.env.HOSTNAME) {
    console.error("HOSTNAME env var is not set!");
    res.status(500).send("Server misconfigured: HOSTNAME not set");
    return;
  }
  const callId = generateSecureId("call");
  const hostname = process.env.HOSTNAME.replace(/^https?:\/\//, "").replace(/\/$/, "");
  console.log(`[${callId}] TwiML request, stream URL: wss://${hostname}/media-stream/${callId}`);
  res.status(200).type("text/xml").end(
    `<Response><Connect><Stream url="wss://${hostname}/media-stream/${callId}" /></Connect></Response>`
  );
});

app.post("/call-status", (_req, res) => res.status(200).send());

// ── Inbound Media Stream ───────────────────────────────────────────────────
app.ws("/media-stream/:callId", async (ws, req) => {
  const callId = req.params.callId as string;
  console.log(`\n[${callId}] CALL STARTED`);

  const WebSocket = require("ws");
  const tw = new TwilioMediaStreamWebsocket(ws);
  // Buffer audio that arrives before streamSid is set
  const pendingAudio: string[] = [];

  tw.on("start", (msg) => {
    tw.streamSid = msg.start.streamSid;
    console.log(`[${callId}] twilio.start sid=${tw.streamSid}`);
    // Flush any audio that arrived before streamSid was ready
    if (pendingAudio.length > 0) {
      console.log(`[${callId}] flushing ${pendingAudio.length} buffered audio chunks`);
      for (const payload of pendingAudio) {
        tw.send({ event: "media", streamSid: tw.streamSid!, media: { payload } });
      }
      pendingAudio.length = 0;
    }
  });

  const xaiWs = new WebSocket(API_URL, {
    headers: { Authorization: `Bearer ${XAI_API_KEY}` },
  });

  await new Promise<void>((resolve, reject) => {
    const t = setTimeout(() => { xaiWs.close(); reject(new Error("xAI timeout")); }, 10000);
    xaiWs.on("open", () => { clearTimeout(t); console.log(`[${callId}] xAI connected`); resolve(); });
    xaiWs.on("error", (e: any) => { clearTimeout(t); reject(e); });
  });

  let sessionReady = false;
  let turnCount = 0;
  let turnActive = false;

  xaiWs.on("message", (data: Buffer, isBinary: boolean) => {
    // xAI sends audio as binary WebSocket frames
    if (isBinary) {
      const payload = data.toString("base64");
      if (tw.streamSid) {
        tw.send({ event: "media", streamSid: tw.streamSid, media: { payload } });
      } else {
        pendingAudio.push(payload);
      }
      return;
    }

    try {
      const msg = JSON.parse(data.toString());

      if (msg.type !== "response.output_audio.delta") {
        console.log(`[${callId}] ${msg.type}`);
      }

      switch (msg.type) {

        case "response.output_audio.delta":
          if (msg.delta) {
            if (tw.streamSid) {
              tw.send({ event: "media", streamSid: tw.streamSid, media: { payload: msg.delta } });
            } else {
              pendingAudio.push(msg.delta);
            }
          } else {
            console.log(`[${callId}] audio delta with no payload`);
          }
          break;

        case "response.created":
          if (turnActive) console.log(`[${callId}] turn ${turnCount} interrupted`);
          turnCount++;
          turnActive = true;
          console.log(`[${callId}] turn ${turnCount} started`);
          break;

        case "response.done":
        case "response.cancelled":
          turnActive = false;
          console.log(`[${callId}] turn ${turnCount} ended`);
          break;

        case "response.output_audio_transcript.delta":
          process.stdout.write(msg.delta || "");
          break;

        case "conversation.item.input_audio_transcription.completed":
          if (msg.transcript) console.log(`\n[${callId}] User: "${msg.transcript}"`);
          break;

        case "input_audio_buffer.speech_started":
          console.log(`[${callId}] barge-in`);
          turnActive = false;
          if (tw.streamSid) tw.send({ event: "clear", streamSid: tw.streamSid });
          break;

        case "input_audio_buffer.speech_stopped":
          console.log(`[${callId}] speech stopped`);
          break;

        case "input_audio_buffer.committed":
          console.log(`[${callId}] buffer committed`);
          break;

        case "session.updated":
          sessionReady = true;
          console.log(`[${callId}] session ready — triggering greeting`);
          xaiWs.send(JSON.stringify({
            type: "conversation.item.create",
            item: {
              type: "message",
              role: "user",
              content: [{ type: "input_text", text: "Say hello and introduce yourself briefly" }],
            },
          }));
          xaiWs.send(JSON.stringify({ type: "response.create" }));
          break;

        case "conversation.created":
          xaiWs.send(JSON.stringify({
            type: "session.update",
            session: {
              instructions: bot.instructions,
              voice: "ara",
              reasoning: { effort: "none" },
              turn_detection: {
                type: "server_vad",
                threshold: 0.85,
                silence_duration_ms: 400,
                prefix_padding_ms: 200,
              },
              audio: {
                input:  { format: { type: "audio/pcmu" } },
                output: { format: { type: "audio/pcmu" } },
              },
              ...(ENABLE_TOOLS ? { tools } : {}),
            },
          }));
          break;

        case "response.output_item.done":
          if (msg.item?.type === "function_call") {
            (async () => {
              const fnName = msg.item.name;
              const fnCallId = msg.item.call_id;
              let args: Record<string, any> = {};
              try { args = JSON.parse(msg.item.arguments || "{}"); } catch {}
              const result = await handleToolCall(fnName, args);
              xaiWs.send(JSON.stringify({
                type: "conversation.item.create",
                item: { type: "function_call_output", call_id: fnCallId, output: result },
              }));
              xaiWs.send(JSON.stringify({ type: "response.create" }));
            })();
          }
          break;

        case "error":
          console.log(`[${callId}] ERROR: ${msg.error?.message}`);
          break;
      }
    } catch (e) {
      console.error(`[${callId}] parse error:`, e);
    }
  });

  tw.on("media", (msg) => {
    if (msg.media.track !== "inbound") return;
    if (!sessionReady || xaiWs.readyState !== 1) return;
    xaiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio: msg.media.payload }));
  });

  xaiWs.on("error", (e: any) => console.log(`[${callId}] ws error: ${e?.message}`));
  xaiWs.on("close", (code: number) => console.log(`[${callId}] ws closed: ${code}`));
  ws.on("close", () => { console.log(`[${callId}] call ended`); xaiWs.close(); });
});

// ── Outbound ───────────────────────────────────────────────────────────────
const OUTBOUND_INSTRUCTIONS = `You are an outbound AI phone agent. YOU speak first. Greet warmly, keep replies short.`;

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
  let turnActive = false;

  ws.on("message", async (data: Buffer) => {
    try {
      const msg = JSON.parse(data.toString());

      if (msg.event === "start") {
        callSid = msg.start.callSid;
        streamSid = msg.start.streamSid;
        console.log(`\n[OUTBOUND][${callSid}] started`);

        xaiWs = new WebSocket(API_URL, { headers: { Authorization: `Bearer ${XAI_API_KEY}` } });

        xaiWs.on("open", () => {
          xaiWs.send(JSON.stringify({
            type: "session.update",
            session: {
              instructions: OUTBOUND_INSTRUCTIONS,
              voice: "rex",
              reasoning: { effort: "none" },
              turn_detection: { type: "server_vad", threshold: 0.85, silence_duration_ms: 400, prefix_padding_ms: 200 },
              audio: {
                input:  { format: { type: "audio/pcmu" } },
                output: { format: { type: "audio/pcmu" } },
              },
            },
          }));
        });

        xaiWs.on("message", (d: Buffer) => {
          try {
            const m = JSON.parse(d.toString());
            if (m.type !== "response.output_audio.delta") console.log(`[OUTBOUND][${callSid}] ${m.type}`);
            switch (m.type) {
              case "session.updated":
                sessionReady = true;
                xaiWs.send(JSON.stringify({ type: "response.create" }));
                break;
              case "response.created":
                turnCount++; turnActive = true;
                break;
              case "response.output_audio.delta":
                if (m.delta && streamSid)
                  ws.send(JSON.stringify({ event: "media", streamSid, media: { payload: m.delta } }));
                break;
              case "response.done":
                turnActive = false;
                if (turnCount >= MAX_TURNS) setTimeout(() => { xaiWs?.close(); ws.close(); }, 3000);
                break;
              case "input_audio_buffer.speech_started":
                if (turnActive && streamSid) {
                  turnActive = false;
                  ws.send(JSON.stringify({ event: "clear", streamSid }));
                }
                break;
              case "input_audio_buffer.speech_stopped":
                xaiWs.send(JSON.stringify({ type: "input_audio_buffer.commit" }));
                xaiWs.send(JSON.stringify({ type: "response.create" }));
                break;
              case "response.output_audio_transcript.delta":
                process.stdout.write(m.delta || "");
                break;
              case "error":
                console.log(`[OUTBOUND] ERROR: ${m.error?.message}`);
                break;
            }
          } catch {}
        });

        xaiWs.on("close", () => console.log(`[OUTBOUND][${callSid}] xAI closed`));

      } else if (msg.event === "media" && msg.media?.track === "inbound") {
        if (xaiWs && sessionReady && xaiWs.readyState === 1)
          xaiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio: msg.media.payload }));
      } else if (msg.event === "stop") {
        xaiWs?.close();
      }
    } catch {}
  });

  ws.on("close", () => xaiWs?.close());
});

const port = process.env.PORT || "3000";
app.listen(port, () => console.log(`Server on http://localhost:${port}`));
