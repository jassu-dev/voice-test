// Thin client for the dashboard's internal API
// Handles call logging and per-account instruction lookup

const DASHBOARD_URL = process.env.DASHBOARD_URL || "";
const INTERNAL_API_KEY = process.env.INTERNAL_API_KEY || "";

async function callApi(body: object): Promise<any> {
  if (!DASHBOARD_URL) return null;
  try {
    const res = await fetch(`${DASHBOARD_URL}/api/internal/call-event`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-internal-key": INTERNAL_API_KEY },
      body: JSON.stringify(body),
    });
    if (!res.ok) return null;
    return res.json();
  } catch {
    return null;
  }
}

// Called on call start — returns { user_id, instructions } or null
export async function notifyCallStart(callId: string, phoneNumberDialed: string, callerNumber: string) {
  return callApi({ event: "call.start", call_id: callId, phone_number_dialed: phoneNumberDialed, caller_number: callerNumber });
}

export async function notifyCallEnd(callId: string, durationSeconds: number) {
  return callApi({ event: "call.end", call_id: callId, duration_seconds: durationSeconds });
}

export async function logTranscript(callId: string, userId: string, role: "user" | "assistant", text: string, itemId?: string) {
  return callApi({ event: "transcript", call_id: callId, user_id: userId, role, text, item_id: itemId || null });
}
