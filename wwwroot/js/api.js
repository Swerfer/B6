/**********************************************************************
 api.js — thin REST client for mission snapshots + kick triggers
  - Single source of truth is the backend API.
  - Always lowercase addresses (API groups & params are lowercase).
  - Light client-side de-duplication for /events/* kicks (~2s).
**********************************************************************/

// V4

const API_ROOT = "/api";
const j = (resp) => {
  if (!resp.ok) throw new Error(`${resp.url} failed (${resp.status})`);
  return resp.json();
};

// Keep same-origin credentials for all API calls
// Always send credentials so cookies/session are included.
const apiFetch = (path, init) => fetch(`${API_ROOT}${path}`, { credentials: "include", ...init });

const toLc = (s) => (s ? String(s).toLowerCase() : "");

// ------------------------ GET: snapshots ----------------------------

/** GET /missions/all/{n} — latest N missions (DB) */
export async function getMissionsAll        (limit = 100)                       {
  // hard-cap to 100 on the client as requested
  const n = Math.min(Math.max(Number(limit) || 0, 1), 100);
  const r = await apiFetch(`/missions/all/${n}`);
  return j(r);
}

/** GET /missions/not-ended — list of active/upcoming missions */
export async function   getMissionsNotEnded ()                                  {
  const r = await apiFetch("/missions/not-ended");
  return j(r);
}

/** GET /missions/joinable — missions currently in enrollment */
export async function   getMissionsJoinable ()                                  {
  const r = await apiFetch("/missions/joinable");
  return j(r);
}

/** GET /missions/mission/{address} — mission detail (snapshot) */
export async function   getMission          (addressLc)                         {
  const addr = toLc(addressLc);
  const r = await apiFetch(`/missions/mission/${addr}`, { cache: "no-store" });
  return j(r);
}

/** GET /missions/player/{address} — missions a player is in */
export async function   getPlayerMissions   (playerAddressLc)                   {
  const addr = toLc(playerAddressLc);
  const r = await apiFetch(`/missions/player/${addr}`);
  return j(r);
}

// ------------------------ POST: kick triggers ------------------------
// De-dup kicks locally so we don’t spam the backend (which also throttles).
// Keyed by `${type}:${mission}`; suppress repeats within KICK_TTL_MS.
const __kickGuard = new Map();
const KICK_TTL_MS = 2000;

function                shouldSendKick      (type, missionLc)                   {
  const key = `${type}:${missionLc}`;
  const now = Date.now();
  const last = __kickGuard.get(key) || 0;
  if (now - last < KICK_TTL_MS) return false;
  __kickGuard.set(key, now);
  return true;
}

async function          postJson            (path, body)                        {
  const r = await apiFetch(path, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  // kicks usually 204/200 with no payload; return ok boolean
  if (!r.ok) throw new Error(`${path} failed (${r.status})`);
  return true;
}

/** POST /events/created → { mission, txHash? } */
export async function   postKickCreated     ({ mission, txHash }          = {}) {
  const missionLc = toLc(mission);
  if (!missionLc) return false;
  if (!shouldSendKick("created", missionLc)) return false;
  return postJson("/events/created", { mission: missionLc, txHash });
}

/** POST /events/enrolled → { mission, player, txHash? } */
export async function   postKickEnrolled    ({ mission, player, txHash }  = {}) {
  const missionLc = toLc(mission);
  const playerLc  = toLc(player);
  if (!missionLc || !playerLc) return false;
  if (!shouldSendKick("enrolled", missionLc)) return false;
  return postJson("/events/enrolled", { mission: missionLc, player: playerLc, txHash });
}

/** POST /events/banked → { mission, player, txHash? } */
export async function   postKickBanked      ({ mission, player, txHash }  = {}) {
  const missionLc = toLc(mission);
  const playerLc  = toLc(player);
  if (!missionLc || !playerLc) return false;
  if (!shouldSendKick("banked", missionLc)) return false;
  return postJson("/events/banked", { mission: missionLc, player: playerLc, txHash });
}

/** POST /events/finalized → { mission, txHash? } */
export async function   postKickFinalized   ({ mission, txHash }          = {}) {
  const missionLc = toLc(mission);
  if (!missionLc) return false;
  if (!shouldSendKick("finalized", missionLc)) return false;
  return postJson("/events/finalized", { mission: missionLc, txHash });
}

/** GET /players/{address}/eligibility — memorized ~10s per address */
const __eligCache = new Map();  // addrLc -> { ts, p }
const ELIG_TTL_MS = 10_000;

export async function getPlayerEligibility  (addressLc)                         {
  const addr = toLc(addressLc);
  if (!addr) return { error: true, message: "No address" };

  const now = Date.now();
  const hit = __eligCache.get(addr);
  if (hit && (now - hit.ts) < ELIG_TTL_MS) return hit.p;

  const p = apiFetch(`/players/${addr}/eligibility`).then(j).catch(e => ({ error: true, message: e?.message || String(e) }));
  __eligCache.set(addr, { ts: now, p });
  return p;
}

