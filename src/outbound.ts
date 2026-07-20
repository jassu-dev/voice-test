import "dotenv-flow/config";
import Twilio from "twilio";
import log from "./logger";

const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID || "";
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN || "";
const TWILIO_PHONE_NUMBER = process.env.TWILIO_PHONE_NUMBER || "";
const TARGET_PHONE_NUMBER = process.env.TARGET_PHONE_NUMBER || "";
const HOSTNAME = process.env.HOSTNAME || "";

if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) {
  console.error("Missing TWILIO_ACCOUNT_SID or TWILIO_AUTH_TOKEN");
  process.exit(1);
}
if (!TWILIO_PHONE_NUMBER) { console.error("Missing TWILIO_PHONE_NUMBER"); process.exit(1); }
if (!TARGET_PHONE_NUMBER) { console.error("Missing TARGET_PHONE_NUMBER"); process.exit(1); }
if (!HOSTNAME) { console.error("Missing HOSTNAME"); process.exit(1); }

const twilioClient = Twilio(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);

async function makeOutboundCall(): Promise<string> {
  const twimlUrl = `${HOSTNAME}/outbound-twiml`;

  log.app.info("[OUTBOUND] Making outbound call");
  log.app.info(`[OUTBOUND] To: ${TARGET_PHONE_NUMBER}`);
  log.app.info(`[OUTBOUND] From: ${TWILIO_PHONE_NUMBER}`);
  log.app.info(`[OUTBOUND] TwiML URL: ${twimlUrl}`);

  const call = await twilioClient.calls.create({
    to: TARGET_PHONE_NUMBER,
    from: TWILIO_PHONE_NUMBER,
    url: twimlUrl,
  });

  log.app.info(`[OUTBOUND] Call initiated - SID: ${call.sid}`);
  return call.sid;
}

makeOutboundCall().catch(console.error);
