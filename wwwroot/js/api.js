/**********************************************************************
 api.js — thin REST client for mission snapshots + kick triggers
  - Single source of truth is the backend API.
  - Always lowercase addresses (API groups & params are lowercase).
  - Light client-side de-duplication for /events/* kicks (~2s).
**********************************************************************/

const API_ROOT = "/api";
const j = (resp) => {
  if (!resp.ok) throw new Error(`${resp.url} failed (${resp.status})`);
  return resp.json();
};

// Keep same-origin credentials like push.js
// (push.js uses: fetch(`/api${p}`, { credentials: "include", ... }) )
const apiFetch = (path, init) => fetch(`${API_ROOT}${path}`, { credentials: "include", ...init });

const toLc = (s) => (s ? String(s).toLowerCase() : "");

// ------------------------ GET: snapshots ----------------------------

/** GET /missions/not-ended — list of active/upcoming missions */
export async function   getMissionsNotEnded() {
  const r = await apiFetch("/missions/not-ended");
  return j(r);
}

/** GET /missions/joinable — missions currently in enrollment */
export async function   getMissionsJoinable() {
  const r = await apiFetch("/missions/joinable");
  return j(r);
}

/** GET /missions/mission/{address} — mission detail (snapshot) */
export async function   getMission(addressLc) {
  const addr = toLc(addressLc);
  const r = await apiFetch(`/missions/mission/${addr}`, { cache: "no-store" });
  return j(r);
}

/** GET /missions/player/{address} — missions a player is in */
export async function   getPlayerMissions(playerAddressLc) {
  const addr = toLc(playerAddressLc);
  const r = await apiFetch(`/missions/player/${addr}`);
  return j(r);
}

// ------------------------ POST: kick triggers ------------------------
// De-dup kicks locally so we don’t spam the backend (which also throttles).
// Keyed by `${type}:${mission}`; suppress repeats within KICK_TTL_MS.
const __kickGuard = new Map();
const KICK_TTL_MS = 2000;

function                shouldSendKick(type, missionLc) {
  const key = `${type}:${missionLc}`;
  const now = Date.now();
  const last = __kickGuard.get(key) || 0;
  if (now - last < KICK_TTL_MS) return false;
  __kickGuard.set(key, now);
  return true;
}

async function          postJson(path, body) {
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
export async function   postKickCreated({ mission, txHash } = {}) {
  const missionLc = toLc(mission);
  if (!missionLc) return false;
  if (!shouldSendKick("created", missionLc)) return false;
  return postJson("/events/created", { mission: missionLc, txHash });
}

/** POST /events/enrolled → { mission, player, txHash? } */
export async function   postKickEnrolled({ mission, player, txHash } = {}) {
  const missionLc = toLc(mission);
  const playerLc  = toLc(player);
  if (!missionLc || !playerLc) return false;
  if (!shouldSendKick("enrolled", missionLc)) return false;
  return postJson("/events/enrolled", { mission: missionLc, player: playerLc, txHash });
}

/** POST /events/banked → { mission, player, txHash? } */
export async function   postKickBanked({ mission, player, txHash } = {}) {
  const missionLc = toLc(mission);
  const playerLc  = toLc(player);
  if (!missionLc || !playerLc) return false;
  if (!shouldSendKick("banked", missionLc)) return false;
  return postJson("/events/banked", { mission: missionLc, player: playerLc, txHash });
}

// ------------------------ Helper: batch refetch (optional) ----------
// If you debounce refetches after hub pushes in game.js, you can import this.
// It collapses multiple getMission(addr) calls within ~250ms.
const MICRO_TTL_MS = 250;
const __detailInflight = new Map();

export async function   getMissionDebounced(addressLc) {
  const addr = toLc(addressLc);
  const now  = Date.now();
  const hit  = __detailInflight.get(addr);
  if (hit && (now - hit.ts) < MICRO_TTL_MS) return hit.p;

  const p = getMission(addr).finally(() => {
    // clear after TTL window; keeps a short-lived “burst” cache
    setTimeout(() => __detailInflight.delete(addr), MICRO_TTL_MS);
  });

  __detailInflight.set(addr, { ts: now, p });
  return p;
}
