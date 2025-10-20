/**********************************************************************
 game.js – home page bootstrap, re-uses core.js & walletConnect.js
**********************************************************************/

// V4

// #region Imports
import { 
  connectWallet, 
  disconnectWallet, 
  walletAddress, 
  getSigner 
} from "./walletConnect.js";

import { 
  showAlert, 
  showConfirm, 
  copyableAddr, 
  formatLocalDateTime, 
  formatCountdown,
  statusText,
  formatDurationShort,
  statusColorClass,
  txLinkIcon,
  addrLinkIcon,
  missionTypeName,
  weiToCro, 
  FACTORY_ABI, 
  FACTORY_ADDRESS,
  MISSION_ABI,
  setBtnLoading,
  decodeError,
  shorten,
} from "./core.js";

import { 
  enableGamePush 
} from "./push.js";

import { 
  startHub as startGameHub, 
  setHandlers, 
  joinMissionGroup, 
  leaveMissionGroup,
  getConnection as getHubConnection,
} from "./hub.js";

import {
  getMission,              
  getMissionDebounced,
  getMissionsAll,
  getMissionsNotEnded,      
  getMissionsJoinable,   
  getPlayerMissions,      
  postKickEnrolled,
  postKickBanked,
  getPlayerEligibility,
} from "./api.js";


// #endregion





// #region Config, int.face&DOM
const MISSION_ERROR_ABI   = [
  "error EnrollmentNotStarted(uint256 nowTs, uint256 startTs)",
  "error EnrollmentClosed(uint256 nowTs, uint256 endTs)",
  "error MaxPlayers(uint8 maxPlayers)",
  "error WrongEntryFee(uint256 expected, uint256 sent)",
  "error AlreadyJoined()",
  "error WeeklyLimit(uint256 secondsLeft)",
  "error MonthlyLimit(uint256 secondsLeft)",
  "error Cooldown(uint256 secondsLeft)",
  "error NotActive(uint256 nowTs, uint256 missionStart)",
  "error MissionEnded()",
  "error AlreadyWon()",
  "error NotJoined()",
  "error AllRoundsDone()",
  "error PayoutFailed(address winner, uint256 amount, bytes data)",
  "error ContractsNotAllowed()",
];
const __missionErrIface   = new ethers.utils.Interface(MISSION_ERROR_ABI);
const connectBtn          = document.getElementById("connectWalletBtn");
const sectionBoxes        = document.querySelectorAll(".section-box");
// #endregion





// #region Runtime state&caches

// List & view:
let   lastListShownId       = "joinableSection";
// Hub:
let   hubConnection         = null;
let   hubStartPromise       = null;   // prevent concurrent starts
let   currentMissionAddr    = null;
let   subscribedAddr        = null;
let   subscribedGroups      = new Set();
// Detail refresh:
let   detailRefreshTimer    = null;
let   detailBackoffMs       = 15000;
let   detailFailures        = 0;
// Stage:
let   ringTimer             = null;
let   stageTicker           = null;
let   stageCurrentStatus    = null;
let   stageReturnTo         = null;   // "stage" when we navigated from stage → detail
let   stageRefreshTimer     = null;
let   stageRefreshBusy      = false;
let   stageRefreshPending   = false;
const __endPopupShown       = new Set();
const __endPopupDeferred    = new Set(); // missions whose end popup is queued after video
let   __bankingInFlight     = null;
const __viewerWonOnce       = new Set(); // missions the current viewer has already won in this session
const __viewerWins          = new Set();
const VAULT_IMG_CLOSED      = 'assets/images/Vault_bg_squared.png';
const VAULT_IMG_OPEN        = 'assets/images/Vault_bg_squared_opened.png';
// All missions cache & filters:
let   __allMissionsCache    = [];     // last fetched list (raw objects)
let   __allFilterOpen       = false;
let   __allSelected         = null;   // null  → all statuses; otherwise Set<number> of allowed statuses
let __allMissionsRenderVersion = 0;
// My missions cache & filters:
let   __myMissionsCache     = [];     // last fetched list (raw objects)
let   __mySelected          = null;   // null → all; otherwise Set<number> of statuses
// Realtime:
let   __lastPushTs          = 0;      // updated on any hub push we care about
// Optimistic:
let   optimisticGuard       = { untilMs: 0, players: 0, croNow: "0" };
// Cache:
const __missionCreatedCache = new Map();
const JOIN_CACHE_KEY        = "_b6joined";
const MISSION_CACHE_TTL_MS  = 8000;
let __missionSnapCache      = { addr:"", ts:0, data:null };
const __missionInflight     = new Map();
const __missionMicroCache   = new Map();           // addr -> { ts, payload }
const MICRO_TTL_MS          = 900;   

// --- add near other runtime flags (top of file) ---
let __allListLastFetchAt    = 0;
const LIST_FETCH_COOLDOWN_MS= 2000;   // soft guard vs stampede
const DETAIL_BATCH_SIZE     = 3;      // small, polite batches
const DETAIL_BATCH_DELAY_MS = 600;    // gap between batches
const PRIMARY_FAST_COUNT    = 12;     // "first 10 fairly quick"
let __allListInflight       = null;
let __allListLastDone       = 0;
const ALL_LIST_COOLDOWN_MS  = 5000;

// miscellaneous 
let   staleWarningShown     = false;
let   ctaBusy               = false;

let __pillsHydrateBusy      = false;
let __pillsHydrateLast      = 0;
let __pauseUntilSec         = 0;
let __postCooldownUntilSec  = 0; 
let __lastPauseTs           = 0;
let __lastViewerAddr        = "";
// --- runtime flags / caches ---
let __lastChainPlayers = 0;  // defensive default; detail view reads this

let __maxRoundSeen          = Object.create(null); // keyed by mission address LC
// #endregion





// #region Degug helpers

window.__B6_DEBUG = true;
function dbg(...args){ if (window.__B6_DEBUG) console.debug("[B6]", ...args); }

window.debugPing = (addr) => fetch(`/api/debug/push/${(addr||window.currentMissionAddr)||""}`)
  .then(r=>r.text()).then(t=>console.log("debug/push →", t)).catch(console.error);

// #endregion





// #region Pure helpers

// Bucketing/sort/filter:

function        bucketOfStatus(s){ // Group order: Active bucket (2/3/4), then Enrolling(1), Pending(0), Ended(5/6/7)
  s = Number(s);
  if (s === 1)  return 1; // Enrolling
  if (s === 0)  return 2; // Pending
  if (s >= 5)   return 3; // Ended
                return 0; // Active bucket: 2 (Arming), 3 (Active), 4 (Paused)
}

function        sortAllMissions(list){
  return list.slice().sort((a, b) => {
    const sa = Number(a.status), sb = Number(b.status);
    const ba = bucketOfStatus(sa), bb = bucketOfStatus(sb);
    if (ba !== bb) return ba - bb;

    // same bucket → per-bucket sort
    if (ba === 0){ // Active bucket
      // prefer “oldest mission end” first; Arming (2) has no end yet → use mission_start
      const ea = sa === 2 ? Number(a.mission_start||0) : Number(a.mission_end||0);
      const eb = sb === 2 ? Number(b.mission_start||0) : Number(b.mission_end||0);
      return (ea||0) - (eb||0); // ascending (oldest first)
    }
    if (ba === 1){ // Enrolling
      const ea = Number(a.enrollment_end||0), eb = Number(b.enrollment_end||0);
      return (ea||0) - (eb||0); // ascending (oldest first)
    }
    if (ba === 2){ // Pending
      const sa_ = Number(a.enrollment_start||0), sb_ = Number(b.enrollment_start||0);
      return (sa_||0) - (sb_||0); // ascending (oldest first)
    }
    // Ended → latest first by mission_end
    const ea = Number(a.mission_end||0), eb = Number(b.mission_end||0);
    return (eb||0) - (ea||0); // descending (latest first)
  });
}

function        applyAllMissionFiltersAndRender(){
  let list = __allMissionsCache || [];
  // selection → Set of explicit statuses; Active includes 2/3/4
  if (__allSelected instanceof Set){
    list = list.filter(m => {
      const s = Number(m.status);
      // If “Active” was selected we store 2,3,4 in the set
      return __allSelected.has(s);
    });
  }
  list = sortAllMissions(list);
  renderAllMissions(list);              // uses the list as-is
  startJoinableTicker();
}

function        buildAllFiltersUI(){
  const host = document.getElementById("allFilters");
  const btn  = document.getElementById("filterAllBtn");
  if (!host || !btn) return;

  const pop = host.querySelector(".filter-pop");

  // Toggle popover
  btn.addEventListener("click", (e) => {
    e.stopPropagation();
    __allFilterOpen = !__allFilterOpen;
    pop.style.display = __allFilterOpen ? "block" : "none";
  });

  // Close only when clicking *outside* the button + popover
  const closeFilters = () => { __allFilterOpen = false; pop.style.display = "none"; };

  document.addEventListener("click", (e) => {
    if (!__allFilterOpen) return;
    const target = e.target;
    // Keep open if the click is on the funnel button or anywhere inside the popover container
    if (target === btn || host.contains(target)) return;
    closeFilters();
  });

  // Also prevent clicks inside the pop from bubbling to document
  pop.addEventListener("click", (e) => e.stopPropagation());

  // Reset = all statuses (including Partly Success implicitly)
  host.querySelector("#fltReset").addEventListener("click", () => {
    __allSelected = null;  // all
    // uncheck boxes in UI
    pop.querySelectorAll("input[type=checkbox]").forEach(i => i.checked = false);
    __allFilterOpen = false;
    pop.style.display = "none";
    applyAllMissionFiltersAndRender();
  });

  // Apply → gather checks into a Set of statuses
  host.querySelector("#fltApply").addEventListener("click", () => {
    const picked = new Set();
    pop.querySelectorAll("input[type=checkbox]:checked").forEach(i => {
      const vals = String(i.dataset.status || "").split(",").map(s => Number(s.trim())).filter(n => !isNaN(n));
      vals.forEach(v => picked.add(v));
    });
    __allSelected = picked.size ? picked : null; // none checked → all
    __allFilterOpen = false;
    pop.style.display = "none";
    applyAllMissionFiltersAndRender();
  });
}

function        applyMyMissionFiltersAndRender(){
  let list = __myMissionsCache || [];
  if (__mySelected instanceof Set){
    list = list.filter(m => __mySelected.has(Number(m.status)));
  }
  list = sortAllMissions(list);     // reuse same sorter
  renderMyMissions(list);           // existing renderer
  startJoinableTicker();            // keep countdowns ticking
}

function        buildMyFiltersUI(){
  const host = document.getElementById("myFilters");     // <div id="myFilters">…</div>
  const btn  = document.getElementById("filterMyBtn");   // <button id="filterMyBtn">…</button>
  if (!host || !btn) return;

  const pop = host.querySelector(".filter-pop");

  // Toggle popover
  btn.addEventListener("click", (e) => {
    e.stopPropagation();
    host.__open = !host.__open;
    pop.style.display = host.__open ? "block" : "none";
  });

  // Close when clicking outside button+popover
  const close = () => { host.__open = false; pop.style.display = "none"; };
  document.addEventListener("click", (e) => {
    if (!host.__open) return;
    const t = e.target;
    if (t === btn || host.contains(t)) return;
    close();
  });
  pop.addEventListener("click", (e) => e.stopPropagation());

  // Reset
  host.querySelector("#fltMyReset")?.addEventListener("click", () => {
    __mySelected = null;
    pop.querySelectorAll("input[type=checkbox]").forEach(i => i.checked = false);
    close();
    applyMyMissionFiltersAndRender();
  });

  // Apply
  host.querySelector("#fltMyApply")?.addEventListener("click", () => {
    const picked = new Set();
    pop.querySelectorAll("input[type=checkbox]:checked").forEach(i => {
      String(i.dataset.status || "")
        .split(",")
        .map(s => Number(s.trim()))
        .filter(n => !isNaN(n))
        .forEach(v => picked.add(v));
    });
    __mySelected = picked.size ? picked : null;
    close();
    applyMyMissionFiltersAndRender();
  });
}

// Promises/local storage:

function        withTimeout(promise, ms = 12000){
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error("timeout")), ms);
    promise.then(
      v => { clearTimeout(t); resolve(v); },
      e => { clearTimeout(t); reject(e); }
    );
  });
}

function        joinedCacheHas(addr, me){
  try {
    const k = (addr||"").toLowerCase();
    const meL = (me||"").toLowerCase();
    const all = JSON.parse(localStorage.getItem(JOIN_CACHE_KEY) || "{}");
    const set = Array.isArray(all[k]) ? all[k] : [];
    return !!(meL && set.includes(meL));
  } catch { return false; }
}

function        joinedCacheAdd(addr, me){
  try {
    const k = (addr||"").toLowerCase();
    const meL = (me||"").toLowerCase();
    if (!k || !meL) return;
    const all = JSON.parse(localStorage.getItem(JOIN_CACHE_KEY) || "{}");
    const set = new Set(Array.isArray(all[k]) ? all[k] : []);
    set.add(meL);
    all[k] = Array.from(set);
    localStorage.setItem(JOIN_CACHE_KEY, JSON.stringify(all));
  } catch {}
}

// Enrichment:

function        enrichMissionFromApi(data){
  const m = data?.mission || data || {};
  if (data && Array.isArray(data.enrollments)) m.enrollments = data.enrollments;
  if (data && Array.isArray(data.rounds))      m.rounds      = data.rounds;

  if (Number(m.status) === 1 &&
      m.cro_initial_wei != null &&
      m.enrollment_amount_wei != null) {
    const initialWei = BigInt(String(m.cro_initial_wei || "0"));
    const feeWei     = BigInt(String(m.enrollment_amount_wei || "0"));

    const enrolledFromApi = (m.enrolled_players != null)
      ? Number(m.enrolled_players)
      : Array.isArray(m.enrollments) ? m.enrollments.length : 0;

    const joined = BigInt(Math.max(
      enrolledFromApi,
      Number(optimisticGuard?.players || 0)
    ));

    m.cro_current_wei = (initialWei + feeWei * joined).toString();
  }
  return m;
}

// Mission error decoding:

function        isCooldownError(err){
  const hex =
    err?.error?.data ||
    err?.data?.originalError?.data ||
    err?.data ||
    null;

  if (!hex || typeof hex !== "string" || !hex.startsWith("0x")) return false;
  if (hex.startsWith("0x08c379a0")) return false; // Error(string) → not custom

  try {
    const parsed = __missionErrIface.parseError(hex);
    return parsed?.name === "Cooldown";
  } catch { return false; }
}

function        missionErrorToText(name, args = []) {
  switch (name) {
    case "EnrollmentNotStarted": {
      const [nowTs, startTs] = args.map(a => Number(a));
      const secs = Math.max(0, startTs - nowTs);
      return `Enrollment hasn’t started yet (${formatDurationShort(secs)} to go).`;
    }
    case "EnrollmentClosed":
      return "Enrollment is closed.";
    case "MaxPlayers": {
      const [max] = args;
      return `Maximum number of players reached (${max}).`;
    }
    case "WrongEntryFee": {
      const [expected, sent] = args;
      return `Incorrect entry fee. Expected ${weiToCro(String(expected), 2)} CRO, sent ${weiToCro(String(sent), 2)} CRO.`;
    }
    case "AlreadyJoined":
      return "You already joined this mission.";
    case "WeeklyLimit": {
      const [secsLeft] = args.map(a => Number(a));
      return `Weekly limit reached. Try again in ${formatDurationShort(secsLeft)}.`;
    }
    case "MonthlyLimit": {
      const [secsLeft] = args.map(a => Number(a));
      return `Monthly limit reached. Try again in ${formatDurationShort(secsLeft)}.`;
    }
    case "Cooldown": {
      const [secsLeft] = args.map(a => Number(a));
      return `Cooldown active. Try again in ${formatDurationShort(secsLeft)}.`;
    }
    case "NotActive":
      return "Mission is not active yet.";
    case "MissionEnded":
      return "Mission has already ended.";
    case "AlreadyWon":
      return "You already won in a previous round.";
    case "NotJoined":
      return "You haven’t joined this mission.";
    case "AllRoundsDone":
      return "All rounds have been completed.";
    case "PayoutFailed": {
      const [winner, amount] = args;
      return `Payout failed (${weiToCro(String(amount), 2)} CRO to ${winner}).`;
    }
    case "ContractsNotAllowed":
      return "Contracts are not allowed to join this mission.";
    default:
      return null;
  }
}

function        __revertHex(err) { // extract revert data from common provider error shapes
  return (
    err?.error?.data ||
    err?.data?.originalError?.data ||
    err?.data ||
    null
  );
}

function        missionCustomErrorMessage(err) { // decode a Mission custom error if present; otherwise return null
  const hex = __revertHex(err);
  if (!hex || typeof hex !== "string" || !hex.startsWith("0x")) return null;

  // let generic decoder handle Error(string)
  if (hex.startsWith("0x08c379a0")) return null;

  try {
    const parsed = __missionErrIface.parseError(hex);
    const name   = parsed?.name;
    const args   = parsed?.args ? Array.from(parsed.args) : [];
    return missionErrorToText(name, args) || `Contract error: ${name}`;
  } catch {
    return null;
  }
}

// Section switcher:

function        showOnlySection(sectionId) {
  sectionBoxes.forEach(sec => {
    sec.style.display = (sec.id === sectionId) ? "" : "none";
  });

  document.getElementById('gameMain').classList.toggle('stage-mode', sectionId === 'gameStage');

  if (["joinableSection","myMissionsSection","allMissionsSection"].includes(sectionId)) {
    lastListShownId = sectionId;                     // <-- keep runtime value updated
    try { localStorage.setItem("b6:lastList", sectionId); } catch {}
  }

  const REFRESH_THROTTLE_MS = 5000;
  if (sectionId === "allMissionsSection") {
    disableTemporarily(els.refreshAllBtn, REFRESH_THROTTLE_MS);
  } else if (sectionId === "joinableSection") {
    disableTemporarily(els.refreshJoinableBtn, REFRESH_THROTTLE_MS);
  } else if (sectionId === "myMissionsSection") {
    disableTemporarily(els.refreshMyBtn, REFRESH_THROTTLE_MS);
  }

}

// Cleanup:

async function  cleanupMissionDetail(){
  stopCountdown();
  stopStageTimer();
  unbindRing();
  clearDetailRefresh();
  setVaultOpen(false);
  staleWarningShown = false;

  if (subscribedAddr) {
    try { await leaveMissionGroup(subscribedAddr); dbg("Unsubscribed (cleanup):", subscribedAddr); } catch {}
  }
  subscribedGroups.clear();
  subscribedAddr = null;
  currentMissionAddr = null;
  // ...
}

// Status→slug:

function        statusSlug(s){
  switch (Number(s)) {
    case 0: return "pending";
    case 1: return "enrolling";
    case 2: return "arming";
    case 3: return "active";
    case 4: return "active";          // Paused uses the same "MISSION TIME" banner
    case 5: return "ended";           // unified banner for all ended variants
    case 6: return "ended";           // unified banner for all ended variants
    case 7: return "ended";           // unified banner for all ended variants
  }
}

// Round helpers:

function        getLastBankTs(mission, rounds){
  const t0 = Number(mission?.mission_start || 0);
  let last = t0;

  // include any recorded round times
  if (Array.isArray(rounds)) {
    for (const r of rounds) {
      const t = Number(r?.created_at || r?.played_at || 0);
      if (t > last) last = t;
    }
  }

  // ALSO include latest pause (a bank just happened)
  const pz = Number(mission?.pause_timestamp || 0);
  if (pz > last) last = pz;

  return last;
}

function        computeBankNowWei(mission, lastBankTs, now = Math.floor(Date.now() / 1000)) {
  const st = Number(mission?.status);
  if (st !== 3 && st !== 4) return "0"; // Only accrues while Active or Paused

  const ms = Number(mission?.mission_start || 0);
  const me = Number(mission?.mission_end || 0);
  const D = Math.max(0, me - ms); // Total mission duration in seconds
  const base = BigInt(mission?.cro_start_wei || "0"); // Total CRO in wei

  if (D === 0 || base === 0n) return "0";

  const tNow = Math.min(now, me);
  const tLast = Math.max(ms, Number(lastBankTs || ms));
  const dt = Math.max(0, tNow - tLast); // Time since last claim

  try {
    const ratePerSecond = base / BigInt(D); // CRO per second
    const accrued = ratePerSecond * BigInt(dt); // CRO since last claim
    return accrued.toString();
  } catch {
    return "0";
  }
}

function        counterColorForWei(mission, accruedWei){
  try {
    const feeWei   = BigInt(String(mission?.enrollment_amount_wei || "0"));
    const baseWei  = BigInt(String(mission?.cro_start_wei || "0"));
    const roundsBN = BigInt(String(mission?.mission_rounds_total ?? mission?.mission_rounds ?? 1) || "1");
    const perRoundWei = (roundsBN > 0n) ? (baseWei / roundsBN) : baseWei;

    let v = BigInt(String(accruedWei || "0"));
    if (v < 0n) v = 0n;

    // BigInt-safe fraction → [0,1]
    const unit = 10000n;
    const toT = (num, den) => {
      if (den <= 0n) return 0;
      const q = (num * unit) / den;
      return Number(q) / Number(unit);
    };

    // HSL → HEX
    const hslToHex = (h, s, l) => {
      h = Math.max(0, Math.min(360, h));
      s = Math.max(0, Math.min(100, s));
      l = Math.max(0, Math.min(100, l));
      const c = (1 - Math.abs(2*l/100 - 1)) * (s/100);
      const hp = h/60;
      const x = c * (1 - Math.abs(hp % 2 - 1));
      let r=0, g=0, b=0;
      if      (0 <= hp && hp < 1) { r=c; g=x; b=0; }
      else if (1 <= hp && hp < 2) { r=x; g=c; b=0; }
      else if (2 <= hp && hp < 3) { r=0; g=c; b=x; }
      else if (3 <= hp && hp < 4) { r=0; g=x; b=c; }
      else if (4 <= hp && hp < 5) { r=x; g=0; b=c; }
      else if (5 <= hp && hp < 6) { r=c; g=0; b=x; }
      const m = l/100 - c/2;
      const to255 = v => Math.round((v + m) * 255);
      const hex = (n) => n.toString(16).padStart(2, '0');
      return `#${hex(to255(r))}${hex(to255(g))}${hex(to255(b))}`;
    };

    const lerp = (a,b,t) => a + (b-a)*t;

    if (feeWei === 0n && perRoundWei === 0n) return "#cfe8ff"; // neutral fallback

    if (feeWei > 0n && v <= feeWei) {
      // 0 → Enrollment Fee: stay grey-ish blue (no green tint); jump to green at fee
      const t = toT(v, feeWei);
      const h = lerp(210, 195, t); // 210° (slate-blue) → 195° (cyan-blue), safely away from green
      const s = lerp(24, 42,  t);  // keep saturation modest for a muted look
      const l = lerp(78, 72,  t);  // slightly darker as it grows
      return hslToHex(h, s, l);
    } else {
      // Enrollment Fee → PoolStart/Rounds: green → yellow → orange → red
      const denom = (perRoundWei > feeWei) ? (perRoundWei - feeWei) : 1n;
      const t = toT((v - feeWei < 0n) ? 0n : (v - feeWei), denom);
      const h = Math.max(0, 120 - 120 * t); // 120° (green) → 0° (red)
      const s = 92;
      const l = 68;
      return hslToHex(h, s, l);
    }
  } catch {
    return "#d0e1ff";
  }
}

function        applyCounterColor(el, mission, accruedWei){
  const col = counterColorForWei(mission, accruedWei);
  el.style.fill = col;
  // subtle outline so text stays readable on your background
  el.style.paintOrder = "stroke fill";
  el.style.stroke = "rgba(0,0,0,0.35)";
  el.style.strokeWidth = "1px";
  return col;
}

function        cooldownInfo(mission, now = Math.floor(Date.now()/1000)){
  const st          = Number(mission?.status);
  const isPaused    = (st === 4);
  const roundsTotal = Number(mission?.mission_rounds_total ?? 0);
  const roundCount  = Number(mission?.round_count ?? 0);

  const roundPauseSecs      = Number(mission?.round_pause_secs      ?? 300);
  const lastRoundPauseSecs  = Number(mission?.last_round_pause_secs ?? 60);
  const secsTotal           = (roundCount === (roundsTotal - 1)) ? lastRoundPauseSecs : roundPauseSecs;

  const pauseTs     = Number(mission?.pause_timestamp || 0);
  const pauseEnd    = pauseTs ? (pauseTs + secsTotal) : 0;
  const secsLeft    = pauseEnd ? Math.max(0, pauseEnd - now) : 0;
  return { isPaused, secsTotal, secsLeft, pauseEnd };
}

function        formatMMSS(s){
  s = Math.max(0, Math.floor(Number(s)||0));
  const m = Math.floor(s/60), sec = s % 60;
  return `${String(m).padStart(2,'0')}:${String(sec).padStart(2,'0')}`;
}

function nonDecreasingRound(m) {
  const addr = String(m?.mission_address || "").toLowerCase();
  const cApi = Number(m?.round_count ?? 0);
  const cArr = Array.isArray(m?.rounds) ? m.rounds.length : 0; // final-round safe
  const cMem = Number(__maxRoundSeen[addr] || 0);
  const cur  = Math.max(cApi, cArr, cMem);
  __maxRoundSeen[addr] = cur; // remember
  return cur;
}

// Chain timestamp:

async function getMissionCreationTs(mission) {
  // Snapshot-driven only: prefer mission.mission_created, fall back to updated_at.
  const inline = Number(mission?.mission_created ?? 0);
  if (inline > 0) return inline;

  const ts = Number(mission?.updated_at ?? 0);
  return ts > 0 ? ts : 0;
}

// Winners/Failure:

function        topWinners(enrollments = [], rounds = [], n = 5){
  // Totals by address (BigInt)
  const totals = new Map();
  for (const r of (rounds || [])){
    const addr = String(r?.winner_address || "").toLowerCase();
    if (!addr) continue;
    const wei = BigInt(String(r?.payout_wei || "0"));
    totals.set(addr, (totals.get(addr) || 0n) + wei);
  }

  // Tie-break: earlier enrollment first
  const enrolledAt = new Map();
  for (const e of (enrollments || [])){
    const a = String(e?.player_address || "").toLowerCase();
    if (a) enrolledAt.set(a, Number(e.enrolled_at || 0));
  }

  const arr = [...totals.entries()].map(([addr, totalWei]) => ({
    addr,
    totalWei,
    enrolledAt: enrolledAt.get(addr) || 0
  }));

  arr.sort((a, b) => {
    if (a.totalWei === b.totalWei) return a.enrolledAt - b.enrolledAt;
    return a.totalWei > b.totalWei ? -1 : 1;
  });

  return arr.slice(0, n);
}

function        failureReasonFor(mission){
  if (Number(mission?.status) !== 7) return null;
  const min    = Number(mission?.enrollment_min_players ?? 0);
  const joined = Math.max(
    (mission?.enrolled_players != null) ? Number(mission.enrolled_players)
                                        : (Array.isArray(mission?.enrollments) ? mission.enrollments.length : 0),
    Number(optimisticGuard?.players || 0)
  );
  const rounds = Number(mission?.round_count ?? 0);

  // Your two failure modes:
  // A) Not enough players enrolled (never started)
  if (rounds === 0 && joined < min)  return "Not enough players";
  // B) Enough players, but no rounds were played before end
  if (rounds === 0 && joined >= min) return "No rounds played";

  return null;
}

function        prettyStatusForList(status, md, allRefunded) { // status: number (0..7), md: Mission.MissionData, failedRefundCount?: number
  // Enum: 0 Pending, 1 Enrolling, 2 Arming, 3 Active, 4 Paused, 5 PartlySuccess, 6 Success, 7 Failed
  if (status === 7) { // Failed
    if (!md || (Array.isArray(md.players) && md.players.length === 0)) {
      return { label: "Cancelled", css: "badge-cancelled", title: "No enrollments; game didn’t run." };
    }
    if (!allRefunded) {
      return { label: "Refunds pending", css: "badge-refund-pending", title: "Some refunds are still being retried." };
    }
    return { label: "Refunded", css: "badge-refunded", title: "All enrollments have been refunded." };
  }
  // fall through to your existing mapping for other statuses…
  return null;
}

// Misc

function        touchUpdatedAtStampFromPush() {
  const stamp = document.getElementById("updatedAtStamp");
  const icon  = document.getElementById("updatedAtIcon");
  const nowS  = Math.floor(Date.now() / 1000);

  if (stamp) {
    stamp.dataset.updated = String(nowS);
    stamp.textContent = formatLocalDateTime(nowS);
    stamp.classList.remove("text-error");
  }
  if (icon) icon.style.display = "none";
  staleWarningShown = false; // clear any prior warning state
}

// #endregion





// #region Stage layout primitive

// Stage background helpers:

let __vaultIsOpen = false; // NEW: remember open/closed

function        setVaultOpen(isOpen, force = false){
  // Normally block OPEN while the video is taking over; allow only when forced.
  if (__vaultVideoFlowActive && isOpen && !force) return;

  __vaultIsOpen = !!isOpen;
  if (!stageImg) return;
  stageImg.src = isOpen ? VAULT_IMG_OPEN : VAULT_IMG_CLOSED;

  // Hide center countdown + ring when the vault is open
  const timer = document.getElementById("vaultTimerText");
  if (timer) timer.style.display = isOpen ? "none" : "";

  const vd = document.getElementById("vaultDisplay");
  if (vd) vd.style.display = isOpen ? "none" : "";

  const ringCover = document.getElementById("ringCover");
  if (ringCover) ringCover.style.display = isOpen ? "none" : "";

  const ringTrack = document.getElementById("ringTrack");
  if (ringTrack) ringTrack.style.display = isOpen ? "none" : "";
}

function        viewerHasWin(mission) {
  const me = (walletAddress || "").toLowerCase();
  if (!me || !mission) return false;

  const addrLc = String(mission.mission_address || "").toLowerCase();
  if (__viewerWins.has(addrLc)) return true;

  // Check API rounds (covers reload / revisit)
  return (mission.rounds || []).some(r =>
    String(r?.winner_address || "").toLowerCase() === me
  );
}

function        updateVaultImageFor(mission) {
  // While the video overlay is active, don’t touch the base art (prevents “open” flash)
  if (__vaultVideoFlowActive) return;

  const ended = Number(mission?.status) >= 5;
  setVaultOpen(!ended && viewerHasWin(mission));
}

// Statusimage:

function        setStageStatusImage(slug){
  if (!stageStatusImgSvg || !slug) return;
  const path = `assets/images/statuses/${slug}.png`;

  // Set both attributes like svgImage() does so browsers reliably repaint
  stageStatusImgSvg.setAttribute("href", path);
  stageStatusImgSvg.setAttributeNS("http://www.w3.org/1999/xlink", "href", path);

  // Optional hard refresh for stubborn caches:
  // const bust = path + `?t=${Date.now()}`;
  // stageStatusImgSvg.setAttribute("href", bust);
  // stageStatusImgSvg.setAttributeNS("http://www.w3.org/1999/xlink", "href", bust);
}

// Center timer core:

async function  startStageTimer(endTs, phaseStartTs = 0, missionObj){
  stopStageTimer();
  const node = document.getElementById("vaultTimerText");
  if (!node || !endTs) { if (node) node.textContent = ""; return; }

  let zeroFired = false;
  let currentUnit = null;
  let pillFlipDone = false; // NEW: debounce single front-end flip at the deadline
  let __watchdogCooldownUntil = 0;

  const paint = async () => {
    const now  = Math.floor(Date.now()/1000);
    const left = Math.max(0, endTs - now);

    // center text only formats; does not drive windows
    node.textContent = formatStageShort(left);
    // Mirror the value into the new digital display
    try { setVaultDisplay(node.textContent); } catch {}

    // NEW: tick lower-HUD countdown pills (full d hh:mm:ss)
    document.querySelectorAll('#stageLowerHud [data-countdown], #stageCtaGroup [data-countdown]').forEach(el => {
      const ts = Number(el.getAttribute('data-countdown') || 0);
      el.textContent = ts ? formatCountdown(ts) : "—";
    });

    // Tick the live prize label (Active/Paused). Rewrites text + color each second.
    document.querySelectorAll('#stageCtaGroup [data-bank-now]').forEach(el => {
      if (!missionObj) return;

      const last = getLastBankTs(missionObj, missionObj?.rounds);
      const wei  = computeBankNowWei(missionObj, last, now);
      const cro  = weiToCro(String(wei), 2, true); // fixed 2 decimals to avoid jitter

      const st   = Number(missionObj?.status);
      const me   = (walletAddress || "").toLowerCase();
      const joined = !!(me && (missionObj?.enrollments || []).some(e => {
        const p = String(e?.player_address || e?.address || e?.player || "").toLowerCase();
        return p === me;
      }));

      const addrLc = String(missionObj?.mission_address || "").toLowerCase();
      const alreadyWon = !!(me && (
        __viewerWonOnce.has(addrLc) ||
        (missionObj?.rounds || []).some(r => {
          const w = String(r?.winner_address || "").toLowerCase();
          return w === me;
        })
      ));

      const eligible = !!walletAddress && joined && !alreadyWon;

      // Match the initial render labels used in renderCtaActive/Paused
      let label =                         "Bank this round to claim:";
      if (st === 3 && !eligible) label =  "Current round prize pool:";
      if (st === 4) label =               "Accumulating:"; // Paused version

      el.textContent = `${label} ${cro} CRO`;
      applyCounterColor(el, missionObj, wei);
    });

    // NEW: update Paused cooldown mm:ss
    document.querySelectorAll('#stageCtaGroup [data-cooldown-end]').forEach(async el => {
      const end = Number(el.getAttribute('data-cooldown-end') || 0);
      if (end > 0) {
        const left = Math.max(0, end - now);
        el.textContent = formatMMSS(left);

        if (left === 0 && missionObj && Number(missionObj.status) === 4 && !el.dataset.flipDone) {
          el.dataset.flipDone = "1"; // debounce
          const m2 = { ...missionObj, status: 3, pause_timestamp: 0 };

          // cooldown ended → allow 4→3 again
          __pauseUntilSec = 0;
          __postCooldownUntilSec = Math.floor(Date.now()/1000) + 60;

          // prevent double tickers and flip immediately
          stopStageTimer();
          setStageStatusImage(statusSlug(3));
          buildStageLowerHudForStatus(m2); // status 3 → paint directly (no chain hydrate)
          await bindRingToMission(m2);
          await bindCenterTimerToMission(m2);
          renderStageCtaForStatus(m2);
          await renderStageEndedPanelIfNeeded(m2);
          stageCurrentStatus = 3;
          refreshOpenStageFromServer(1);
        }
      }
    });

    // --- LOW-WEIGHT WATCHDOG (missed-push fallback) --------------------
    // If we are on the stage, the timer is still running, and we haven't
    // seen a push in a while, do one gentle reconcile with a short cooldown.
    const onStage = document.getElementById('gameMain')?.classList.contains('stage-mode');
    if (onStage && left > 0) {
      const gapMs = Date.now() - (__lastPushTs || 0);
      if (gapMs > 30000 && Date.now() > __watchdogCooldownUntil) {  // >30s silence
        __watchdogCooldownUntil = Date.now() + 15000;                // 15s cooldown
        smartReconcile("watchdog");
      }
    }
    // -------------------------------------------------------------------    

    // Rebind ring when the unit changes...
    const unit = stageUnitFor(left);
    if (unit !== currentUnit) {
      const [S, E] = ringWindowForUnit(unit, phaseStartTs, endTs);
      // seconds unit → 0.1 s ticks, otherwise 1 s
      const tickMs = 100; //(unit === "s") ? 100 : 1000;
      if (S && E && E > S) bindRingToWindow(S, E, tickMs); else setRingProgress(0);
      currentUnit = unit;
    }

    if (left <= 0) {
      stopStageTimer();
      if (!zeroFired) {
        zeroFired = true;

        if (missionObj && !pillFlipDone) {
          const next = statusByClock(missionObj, now); // Pending→Enrolling, Enrolling→Arming/Failed, Arming→Active, Active→Ended
          const cur  = Number(missionObj.status);
          if (typeof next === "number" && next !== cur) {
            pillFlipDone = true;

            // Start from current snapshot
            const m2 = { ...missionObj, status: next };
            setStageStatusImage(statusSlug(next));   // ← update the stage title/status image immediately
            if (next === 3) {
              // Arming → Active: hydrate before painting
              await rehydratePillsFromServer(m2, "deadlineFlip-arming→active", true);
              buildStageLowerHudForStatus({ ...m2, status: 3 });
            } else if (next === 2) {
              // Enrolling → Arming:
              // Freeze Pool (start) to the final enrollment pool immediately:
              // cro_start_wei = cro_initial_wei + fee * joined
              try {
                const initialWei = BigInt(String(m2.cro_initial_wei || "0"));
                const feeWei     = BigInt(String(m2.enrollment_amount_wei || "0"));
                const joined     = BigInt(Array.isArray(m2.enrollments) ? m2.enrollments.length : 0);

                // If we already computed current via enrichment, prefer that; else compute now from initial
                const currentFromEnrolling = (m2?.cro_current_wei != null) ? BigInt(String(m2.cro_current_wei)) : null;
                const computedFinal        = initialWei + feeWei * joined;
                const finalStart           = (currentFromEnrolling != null) ? currentFromEnrolling : computedFinal;

                m2.cro_start_wei = finalStart.toString();
              } catch {}

              // Paint immediately with corrected Pool (start), then hydrate to confirm

              await rehydratePillsFromServer(m2, "deadlineFlip-enrolling→arming", true);
              buildStageLowerHudForStatus(m2);
            } else {
              // keep existing behavior for other statuses
              buildStageLowerHudForStatus(m2);
              if (next === 1) {
                rehydratePillsFromServer(m2, "deadlineFlip", true).catch(()=>{});
              }
            }

            await bindRingToMission(m2);
            await bindCenterTimerToMission(m2);
            renderStageCtaForStatus(m2);
            await renderStageEndedPanelIfNeeded(m2);
            await maybeShowMissionEndPopup(m2);
            stageCurrentStatus = next;
          }
        }

        // Keep the quick reconcile so DB fields (players, rounds, pause flag) catch up safely
        refreshOpenStageFromServer(2);
      }
    }

  };

  paint();
  stageTicker = setInterval(paint, 1000);
}

function        stopStageTimer(){ 
  if (stageTicker){ clearInterval(stageTicker); stageTicker = null; } 
}

function        formatStageShort(leftSec){
  const s = Math.max(0, Math.floor(leftSec));
  if (s > 36*3600) return Math.round(s/86400) + "D";   // > 36h → days
  if (s > 90*60)   return Math.round(s/3600)  + "H";   // > 90m → hours
  if (s > 90)      return Math.round(s/60)    + "M";   // > 90s  → minutes
  return s + "S";                                      // ≤ 90s → seconds
}

// === Center digital display (7-segment) =====================================

function        ensureVaultDisplayBuilt(){
  const root = document.getElementById('vaultDisplay');
  if (!root) return null;

  // If segments already exist, skip
  if (root.__built) return root;
  root.__built = true;

  // For each digit group, draw 7 segments (A..G)
  const segW = 12, segT = 2.6;   // a touch thicker for legibility
  const on   = "#6ED3FF";        // brighter ON
  const off  = "rgba(110,211,255,.12)"; // dimmer OFF for contrast

  // How much to shorten the RIGHT side of the three horizontals (px)
  const H_TRIM = 1.6; // try 1.0–2.2

  // How much to trim top/bottom of verticals (px)
  const V_TRIM_TOP = 1.0, V_TRIM_BOTTOM = 1.2; // try 0.8–1.6

  // Horizontal with centered tips (diamond ends) and optional RIGHT trim
  const mkH = (x, y, w, t, rTrim = 0, lTrim = 0) => {
    const ht  = t / 2;
    const hw  = (w - rTrim - lTrim) / 2;
    const xL  = x - hw;
    const xR  = x + hw;
    // polygon: left tip → top edge → right tip → bottom edge
    return `
      M ${xL - ht} ${y}
      L ${xL} ${y - ht}
      L ${xR} ${y - ht}
      L ${xR + ht} ${y}
      L ${xR} ${y + ht}
      L ${xL} ${y + ht}
      Z
    `;
  };

  // Vertical with centered tips (diamond ends) and top/bottom trims
  const mkV = (x, y, w, t, tTrim = 0, bTrim = 0) => {
    const ht  = t / 2;
    const hv  = (w - tTrim - bTrim) / 2;
    const yT  = y - hv;          // trimmed top
    const yB  = y + hv;          // trimmed bottom
    return `
      M ${x - ht} ${yT}
      L ${x} ${yT - ht}
      L ${x + ht} ${yT}
      L ${x + ht} ${yB}
      L ${x} ${yB + ht}
      L ${x - ht} ${yB}
      Z
    `;
  };

  const segDefs = [
    { id: 'A', d: mkH(0, -segW, segW, segT, H_TRIM, 0) },
    { id: 'B', d: mkV( segW/2, -segW/2, segW, segT, V_TRIM_TOP, V_TRIM_BOTTOM) },
    { id: 'C', d: mkV( segW/2,  segW/2, segW, segT, V_TRIM_TOP, V_TRIM_BOTTOM) },
    { id: 'D', d: mkH(0,  segW,  segW, segT, H_TRIM, 0) },
    { id: 'E', d: mkV(-segW/2,  segW/2, segW, segT, V_TRIM_TOP, V_TRIM_BOTTOM) },
    { id: 'F', d: mkV(-segW/2, -segW/2, segW, segT, V_TRIM_TOP, V_TRIM_BOTTOM) },
    { id: 'G', d: mkH(0, 0, segW, segT, H_TRIM, 0) },
  ];

  const map = {
    "0": "A B C D E F",
    "1": "B C",
    "2": "A B D E G",
    "3": "A B C D G",
    "4": "B C F G",
    "5": "A C D F G",
    "6": "A C D E F G",
    "7": "A B C",
    "8": "A B C D E F G",
    "9": "A B C D F G"
  };
  root.__segMap = map;

  const digitsHost = root.querySelector('#vdDigits');
  digitsHost.querySelectorAll('.vdDigit').forEach(dg => {
    segDefs.forEach(s => {
      const p = document.createElementNS(SVG_NS, 'path');
      p.setAttribute('d', s.d);
      p.setAttribute('class', 'seg');
      p.setAttribute('data-seg', s.id);
      p.setAttribute('fill', off);
      dg.appendChild(p);
    });
  });

  // Colors for quick toggles
  root.__segOn  = on;
  root.__segOff = off;

  return root;
}

function        setVaultDisplay(valueStr){
  const root = ensureVaultDisplayBuilt();
  if (!root) return;

  // Expect things like "22H", "8M", "45S", "2D"
  valueStr = String(valueStr || "").toUpperCase().trim();
  const unit = valueStr.slice(-1);
  const num  = valueStr.slice(0, -1);

  // Left-pad number to 2 digits (blank is allowed on the left)
  // Always show 2 digits (e.g. 03 instead of 3)
  const padded = num.padStart(2, "0");
  const d0 = padded[0];
  const d1 = padded[1];

  // Update two digit groups
  const map   = root.__segMap;
  const color = (dg, set) => {
    dg.querySelectorAll('[data-seg]').forEach(seg => {
      const id = seg.getAttribute('data-seg');
      const isOn = set.has(id);
      seg.setAttribute('fill', isOn ? root.__segOn : root.__segOff);
      if (isOn) seg.setAttribute('filter', 'url(#vdGlow)');
      else      seg.removeAttribute('filter');
    });
  };

  const digs = root.querySelectorAll('.vdDigit');
  [d0, d1].forEach((ch, idx) => {
    const g  = digs[idx];
    const key= (ch === " ") ? "" : (map[ch] ? ch : " "); // blank or mapped digit
    const on = new Set((map[key] || "").split(/\s+/).filter(Boolean));
    color(g, on);
  });

  // Unit badge letter
  const unitText = root.querySelector('#vdUnitText');
  unitText.textContent = (unit || "").slice(0,1);
}

function        stageUnitFor(leftSec){ // Unit classifier used by the center text and the ring "reset" windows
  const s = Math.max(0, Math.floor(leftSec));
  if (s > 36*3600) return "d";
  if (s > 90*60)   return "h";
  if (s > 90)      return "m";
  return "s";
}

function        ringWindowForUnit(unit, phaseStart, endTs){
  if (!endTs) return [0,0];
  const start   = Number(phaseStart || 0);
  const H36     = 36 * 3600;
  const M90     = 90 * 60;
  const S90     = 90;

  // If start is missing, fall back so "d/h/m" still look sane
  const S0      = start || (endTs - H36);
  const phaseLen= Math.max(0, endTs - S0);

  if (unit === "d") return [S0, endTs];

  if (unit === "h") {
    const S = (phaseLen < H36) ? S0 : (endTs - H36);
    return [S, endTs];
  }
  if (unit === "m") {
    const S = (phaseLen < M90) ? S0 : (endTs - M90);
    return [S, endTs];
  }
  // unit === "s" → last 90 seconds
  return [endTs - S90, endTs];
}

// Deadline routing:

function        nextDeadlineFor(m){ // Choose the deadline shown in the vault center per status
  if (!m) return 0;
  const st = Number(m.status);
  if (st === 0) return Number(m.enrollment_start || m.mission_start || 0); // Pending
  if (st === 1) return Number(m.enrollment_end   || 0);                    // Enrolling
  if (st === 2) return Number(m.mission_start    || 0);                    // Arming
  if (st === 3 || st === 4) return Number(m.mission_end || 0);             // Active / Paused
  return 0; // Ended variants – no countdown in center
}

function        statusByClock(m, now = Math.floor(Date.now()/1000)) { // Compute the status purely from immutable times (front-end flip)
  const es = Number(m.enrollment_start || 0);
  const ee = Number(m.enrollment_end   || 0);
  const ms = Number(m.mission_start    || 0);
  const me = Number(m.mission_end      || 0);

  const cur = Math.max(
    (m?.enrolled_players != null) ? Number(m.enrolled_players)
                                  : (Array.isArray(m.enrollments) ? m.enrollments.length : 0),
    Number(optimisticGuard?.players || 0)
  );

  const min = Number(m.enrollment_min_players ?? 0);

  if (now < es)                           return 0;                               // Pending
  if (now < ee)                           return 1;                               // Enrolling
  if (now < ms) {
    const GRACE = 30; // seconds
    if (cur < min && (now - ee) <= GRACE) return 2;                               // Arming (grace period)          
                                          return (cur >= min ? 2 : 7);            // Arming vs Failed (not enough players)
  }
  if (now < me)                           return (m.pause_timestamp ? 4 : 3);     // Paused vs Active
                                          return (m.status >= 5 ? m.status : 6);  // Ended bucket (keep subtype if present)
}

async function  bindCenterTimerToMission(mission){
  const endTs  = nextDeadlineFor(mission);
  const st     = Number(mission?.status);
  let   startTs= 0;

  if (st === 0) {                                // Pending
    startTs = await getMissionCreationTs(mission);
    if (!startTs) startTs = Number(mission.updated_at || 0);
  } else if (st === 1) {                         // Enrolling
    startTs = Number(mission.enrollment_start || 0);
  } else if (st === 2) {                         // Arming
    startTs = Number(mission.enrollment_end   || 0);
  } else if (st === 3 || st === 4) {             // Active / Paused
    startTs = Number(mission.mission_start    || 0); // <- key change
  } else {
    startTs = 0;
  }

  // Below is for start from 'start' times. See also function ringWindowForUnit

  /* const st    = Number(mission?.status);

  let startTs = 0;
  if (st === 0) {
    // Pending: creation → enroll start
    startTs = await getMissionCreationTs(mission);
    if (!startTs) startTs = Number(mission.updated_at || 0); // fallback from API row
  } else if (st === 1) {
    startTs = Number(mission.enrollment_start || 0);
  } else if (st === 2) {
    startTs = Number(mission.enrollment_end   || 0);
  } else if (st === 3 || st === 4) {
    startTs = Number(mission.mission_start    || 0);
  }

  // pass mission to enable front-end flip at 0s
  */
  startStageTimer(endTs, startTs, mission);
}

// Ring overlay:

function        unbindRing(){
  if (ringTimer){ clearInterval(ringTimer); ringTimer = null; }
}

function        setRingProgress(pct){
  // pct: 0..100 revealed (0 = all covered, 100 = fully blue)
  const cover = document.getElementById("ringCover");
  if (!cover) return;

  const r   = Number(cover.getAttribute("r")) || 0;
  const C   = 2 * Math.PI * r;
  const clamped = Math.max(0, Math.min(100, pct));
  const coverFrac = 1 - (clamped / 100);          // 1 = fully covered → 0 = fully revealed
  const coverLen  = coverFrac * C;

  cover.setAttribute("stroke-dasharray", `${coverLen} ${C - coverLen}`);
  cover.setAttribute("stroke-dashoffset", String(coverLen)); // clockwise
}

function        bindRingToWindow(startSec, endSec, tickMs = 1000){
  unbindRing();
  const cover = document.getElementById("ringCover");
  if (!cover) return;
  if (!startSec || !endSec || endSec <= startSec) return;

  const tick = () => {
    const nowMs  = Date.now();
    const nowSec = nowMs / 1000; // high-resolution seconds
    let pct; // revealed percent 0..100
    if (nowSec <= startSec) pct = 0;
    else if (nowSec >= endSec) pct = 100;
    else pct = ((nowSec - startSec) / (endSec - startSec)) * 100;
    setRingProgress(pct);
  };

  tick(); // draw immediately
  ringTimer = setInterval(tick, Math.max(16, tickMs)); // clamp to ~60fps minimum spacing
}

async function  bindRingToMission(m){ /* Map mission.status to the correct time window */
  const st = Number(m?.status ?? -1);

  let S = 0, E = 0;
  if (st === 0) {                               // Pending
    S = m?.mission_address ? await getMissionCreationTs(m) : 0;
    if (!S) S = Number(m.updated_at || 0);
    E = Number(m.enrollment_start || m.mission_start || 0);
  } else if (st === 1) {                        // Enrolling
    S = Number(m.enrollment_start || 0);
    E = Number(m.enrollment_end   || 0);
  } else if (st === 2) {                        // Arming
    S = Number(m.enrollment_end   || 0);
    E = Number(m.mission_start    || 0);
  } else if (st === 3 || st === 4) {            // Active / Paused
    S = Number(m.mission_start    || 0);
    E = Number(m.mission_end      || 0);
  } else {
    setRingProgress(100);
    return;
  }

  if (S && E && E > S) {
    bindRingToWindow(S, E);                     // phase start → phase end
  } else {
    setRingProgress(st >= 5 ? 100 : 0);
  }
}

// Page scroll lock:

function        lockScroll(){
  const y = window.scrollY || document.documentElement.scrollTop || 0;
  document.documentElement.classList.add("scroll-lock");
  document.body.classList.add("scroll-lock");
  document.body.style.setProperty("--lock-top", `-${y}px`);
}

function        unlockScroll(){
  const top = parseInt(getComputedStyle(document.body).getPropertyValue("--lock-top")) || 0;
  document.documentElement.classList.remove("scroll-lock");
  document.body.classList.remove("scroll-lock");
  document.body.style.removeProperty("--lock-top");
  // restore previous scroll position
  window.scrollTo(0, -top);
}

// State helpers:

function        resetMissionLocalState() {
  // Clear cross-mission, “no-regress” caches so pills won’t bleed
  optimisticGuard    = { untilMs: 0, players: 0, croNow: "0" };
  __pillsHydrateBusy = false;
  __pillsHydrateLast = 0;
}

// #endregion





// #region SignalR hub&handlers

function        stateName(s){
  const H = signalR.HubConnectionState;
  return  s === H.Connected     ? "Connected"
        : s === H.Disconnected  ? "Disconnected"
        : s === H.Connecting    ? "Connecting"
        : s === H.Reconnecting  ? "Reconnecting"
        : String(s);
}

window.stateName = stateName; // Temp for debugging

async function  startHub() { // SignalR HUB via shared hub.js
  try {
    await startGameHub();
    // Keep backward-compat debug helpers that read window.hubConnection
    window.hubConnection = getHubConnection?.();
  } catch (err) {
    console.error("Hub start failed:", err);
    showAlert("Real-time channel failed to connect.", "error");
    throw err;
  }

  // Wire the three server → client events to our existing UI logic.
  setHandlers({
    onMissionUpdated: async (addr) => {
      __lastPushTs = Date.now();
      touchUpdatedAtStampFromPush();
      dbg("MissionUpdated PUSH", { addr, currentMissionAddr, groups: Array.from(subscribedGroups) });

      if (currentMissionAddr && addr?.toLowerCase() === currentMissionAddr) {
        try {
          const m = enrichMissionFromApi(await getMissionDebounced(currentMissionAddr));

          const apiStatus = Number(m.status);
          const curStatus = Number(stageCurrentStatus ?? -1);

          if (apiStatus < curStatus) {
            console.debug("[MissionUpdated] stale status from API; ignoring", apiStatus, "<", curStatus);
            refreshOpenStageFromServer(1);
            return;
          }

          if (curStatus === 4 && apiStatus === 3) {
            const el  = document.querySelector('#stageCtaGroup [data-cooldown-end]');
            const end = el ? Number(el.getAttribute('data-cooldown-end') || 0) : 0;
            if (end && (Date.now()/1000) < end) {
              dbg("Ignoring 4→3 while local cooldown still visible.");
              refreshOpenStageFromServer(2);
              return;
            }
          }

          const onStage = document.getElementById('gameMain')?.classList.contains('stage-mode');
          if (onStage) {
            stageCurrentStatus = Number(m.status);
            renderStage(m);
            refreshOpenStageFromServer(2);
          } else {
            if (m) renderMissionDetail({ mission: m });
          }
        } catch (err) {
          console.log("startHub MissionUpdated error: " + err)
        }
      }
    },

    onStatusChanged: async (addr, newStatus) => {
      __lastPushTs = Date.now();
      touchUpdatedAtStampFromPush();
      dbg("StatusChanged PUSH", { addr, newStatus, currentMissionAddr, groups: Array.from(subscribedGroups) });

      if (!currentMissionAddr || addr?.toLowerCase() !== currentMissionAddr) {
        return;
      }

      const gameMain = document.getElementById('gameMain');

      try {
        const m = enrichMissionFromApi(await getMissionDebounced(currentMissionAddr));
        stageCurrentStatus = Number(m.status);

        if (gameMain?.classList.contains('stage-mode')) {
          renderStage(m);
          refreshOpenStageFromServer(3);
        } else {
          renderMissionDetail({ mission: m });
        }
      } catch {
        // ignore; UI will catch up on next push
      }
    },

    onRoundResult: async (addr, round, winner, amountWei) => {
      __lastPushTs = Date.now();
      touchUpdatedAtStampFromPush();
      dbg("RoundResult PUSH", { addr, round, winner, amountWei, currentMissionAddr, groups: Array.from(subscribedGroups) });

      try {
        const aLc = String(addr || "").toLowerCase();
        if (__bankingInFlight && __bankingInFlight.mission === aLc) {
          __bankingInFlight.hadResult = true;
        }
      } catch {}

      const me  = (walletAddress || "").toLowerCase();
      const win = String(winner || "").toLowerCase();
      const cro = weiToCro(String(amountWei), 2);

      if (win === me) {
        if (window.__winPopupSuppressUntil && Date.now() < window.__winPopupSuppressUntil) {
          if (__vaultVideoFlowActive) {
            try { await finalizeVaultVideoFlow(addr, round, winner, amountWei); } catch {}
          }
        } else {
          window.__winPopupSuppressUntil = Date.now() + 5000;
          showAlert(`You won <b>${cro} CRO</b> in round ${round}!`, "success", 5000);
          try { await finalizeVaultVideoFlow(addr, round, winner, amountWei); } catch {}
        }
      } else {
        showStripe(`${copyableAddr(winner)} won <b>${cro} CRO</b> in round ${round}`, "info", 5000);
      }

      if (currentMissionAddr && addr?.toLowerCase() === currentMissionAddr) {
        try {
          const m = enrichMissionFromApi(await getMissionDebounced(currentMissionAddr));
          const gameMain = document.getElementById('gameMain');
          if (gameMain?.classList.contains('stage-mode')) {
            renderStage(m);
            refreshOpenStageFromServer(4);
          } else {
            renderMissionDetail({ mission: m });
          }
        } catch {}
      }
    }
  });

  // Ensure we are joined to the currently open mission (if any)
  await safeSubscribe();
}

async function  safeSubscribe(){
  const lc = String(currentMissionAddr || "").toLowerCase();
  if (!lc) return;

  await joinMissionGroup(lc);

  // Track for UI/debug parity
  let ck = null; try { ck = ethers.utils.getAddress(lc); } catch {}
  subscribedAddr   = lc;
  subscribedGroups = new Set([lc, ck].filter(Boolean));
}

window.hubState = () => {
  try {
    const st = window.hubConnection?.state;
    console.log("Hub state:", st, "(", window.stateName?.(st) ,")");
    console.log("currentMissionAddr:", currentMissionAddr);
    console.log("subscribedGroups:", Array.from(subscribedGroups));
    console.log("stageCurrentStatus:", window.stageCurrentStatus);
  } catch (e) { console.log(e); }
};

// #endregion





// #region Details auto-refresh

function        clearDetailRefresh(){          
  if (detailRefreshTimer) { 
    clearTimeout(detailRefreshTimer); 
    detailRefreshTimer = null; 
  }
}

function scheduleDetailRefresh(reset = false) {
  // Push-driven model: no periodic polling on the detail page.
  clearDetailRefresh();
  // no timer; on any hub push we already refetch via getMissionDebounced()
}

// #endregion





// #region API wrappers

async function  fetchAndRenderAllMissions () {
  const now = Date.now();
  if (__allListInflight) return __allListInflight;
  if ((now - __allListLastDone) < ALL_LIST_COOLDOWN_MS) { return; }

  const p = (async () => {
    try {
      // DB-read: latest 100 missions
      const list = await getMissionsAll(100);

      // Keep the same normalized shape the UI already uses
      const missions = (Array.isArray(list) ? list : []).map(m => {

        const mission_duration =
          (m.mission_start && m.mission_end) ? (Number(m.mission_end) - Number(m.mission_start)) : 0;

        return {
          mission_address:        m.mission_address,
          name:                   m.name,
          type:                   m.mission_type,
          status:                 Number(m.status),
          enrollment_start:       m.enrollment_start,
          enrollment_end:         m.enrollment_end,
          enrollment_amount_wei:  m.enrollment_amount_wei,
          enrollment_min_players: m.enrollment_min_players,
          enrollment_max_players: m.enrollment_max_players,
          mission_start:          m.mission_start, 
          mission_end:            m.mission_end,
          mission_rounds_total:   m.mission_rounds_total,
          round_count:            m.round_count,
          cro_initial_wei:        m.cro_initial_wei,
          cro_start_wei:          m.cro_start_wei,
          cro_current_wei:        m.cro_current_wei,
          pause_timestamp:        m.pause_timestamp,
          updated_at:             m.updated_at,
          mission_created:        m.mission_created,
          round_pause_secs:       m.round_pause_secs,
          last_round_pause_secs:  m.last_round_pause_secs,
          creator_address:        m.creator_address,
          all_refunded:           m.all_refunded,
          enrolled_players:       m.enrolled_players,
          mission_duration:       mission_duration,
        };
      });

      __allMissionsCache = missions;
      console.log("Fetched All Missions:", missions);
      applyAllMissionFiltersAndRender();   // existing filters continue to apply

      // Snapshot + SignalR only (unchanged)
      startJoinableTicker();
    } catch (e) {
      console.error(e);
      showAlert("Failed to load All Missions.", "error");
      renderAllMissions([]);
    } finally {
      __allListLastDone = Date.now();
      try { sessionStorage.setItem("_allListLastLoadAt", String(__allListLastDone)); } catch {}
    }
  })();

  __allListInflight = p;
  try { return await p; }
  finally { __allListInflight = null; }
}

async function  fetchAndRenderMyMissions  () {
  try {
    const me = (walletAddress || "").toLowerCase();
    if (!me) { showAlert("Connect your wallet to load your missions.", "warning"); return; }

    const list = await apiPlayerMissions(me);           // already in file
    // Keep the same normalized shape the list renderer expects
    const missions = (Array.isArray(list) ? list : []).map(m => ({
      mission_address:        m.mission_address,
      name:                   m.name,
      status:                 Number(m.status),
      enrollment_start:       m.enrollment_start,
      enrollment_end:         m.enrollment_end,
      enrollment_amount_wei:  m.enrollment_amount_wei,
      enrollment_min_players: m.enrollment_min_players,
      enrollment_max_players: m.enrollment_max_players,
      mission_start:          m.mission_start,
      mission_end:            m.mission_end,
      mission_rounds_total:   m.mission_rounds_total ?? m.missionRounds ?? 0,
      round_count:            m.round_count,
      cro_initial_wei:        m.cro_initial_wei,
      cro_start_wei:          m.cro_start_wei,
      cro_current_wei:        m.cro_current_wei,
      pause_timestamp:        m.pause_timestamp,
      updated_at:             m.updated_at,
      mission_created:        m.mission_created,
      round_pause_secs:       m.round_pause_secs,
      last_round_pause_secs:  m.last_round_pause_secs,
      creator_address:        m.creator_address,
      all_refunded:           m.all_refunded,
      enrolled_players:       (m.enrolled_players != null)
                                ? m.enrolled_players
                                : (Array.isArray(m.enrollments) ? m.enrollments.length : 0),
    }));

    __myMissionsCache = missions;
    applyMyMissionFiltersAndRender();
  } catch (e) {
    console.error(e);
    showAlert("Failed to load My Missions.", "error");
    renderMyMissions([]);
  }
}

async function  apiJoinable               (){
  return getMissionsJoinable();
}

async function  apiPlayerMissions         (addr){
  const a = String(addr || "").toLowerCase();
  return getPlayerMissions(a);
}

/**
 * Stage-aware mission reader:
 * - When in game stage: returns cached snapshot for 15s, unless `force` is true.
 * - When not in game stage: no caching is stored (always fresh fetch).
 * - Force calls refresh the cache and reset the 15s window.
 */
async function  apiMission                (addr, force = false) {
  const lcAddr  = String(addr || "").toLowerCase();
  const now     = Date.now();
  const onStage = !!(document.getElementById('gameMain')?.classList.contains('stage-mode'));

  if (            !force && __missionSnapCache.addr === lcAddr && __missionSnapCache.data && (now - __missionSnapCache.ts) < 900) {
    return __missionSnapCache.data;
  }

  // Stage cache (unchanged behavior)
  if (onStage &&  !force && __missionSnapCache.addr === lcAddr && __missionSnapCache.data && (now - __missionSnapCache.ts) < MISSION_CACHE_TTL_MS) {
      return __missionSnapCache.data;
  }

    // Micro window: if we fetched this addr very recently, reuse that payload (collapses “paired” calls)
    const mc = __missionMicroCache.get(lcAddr);
    if (mc && (now - mc.ts) < MICRO_TTL_MS) {
      return mc.payload;
    }

  // Coalesce concurrent requests for the same address (even if force=true)
  if (__missionInflight.has(lcAddr)) {
    return __missionInflight.get(lcAddr);
  }

  const p = (async () => {
    const data = await getMission(lcAddr);
    __missionMicroCache.set(lcAddr, { ts: Date.now(), payload: data });
    if (onStage) {
      __missionSnapCache = { addr: lcAddr, ts: Date.now(), data };
    }
    return data;
  })();

  __missionInflight.set(lcAddr, p);
  try { return await p; }
  finally { __missionInflight.delete(lcAddr); }
}

// #endregion





// #region Reconciliation (stage)

async function  refreshOpenStageFromServer(retries = 3, delay = 1600) {
  const gameMain = document.getElementById('gameMain');
  if (!gameMain || !gameMain.classList.contains('stage-mode')) return;

  // serialize: only one in-flight refresh; coalesce others
  if (stageRefreshBusy) { stageRefreshPending = true; return; }
  stageRefreshBusy = true;

  const scheduleRetry = (n) => {
    clearTimeout(stageRefreshTimer);
    if (n > 0) {
      stageRefreshTimer = setTimeout(() => refreshOpenStageFromServer(n - 1, delay), delay);
    }
  };

  try {
    const force = (Date.now() < (optimisticGuard?.untilMs || 0)) || (Date.now() - (__lastPushTs || 0) > 8000);
    const data  = await apiMission(currentMissionAddr, force);
    const m = enrichMissionFromApi(data);

    // Merge optimism for a short window so API can't regress the UI
    // Grow phases (Enrolling/Arming): prefer higher; Active/Paused: prefer lower.
    if (Date.now() < (optimisticGuard?.untilMs || 0)) {
      try {
        const apiCro = BigInt(String(m.cro_current_wei || m.cro_start_wei || "0"));
        const optCro = BigInt(String(optimisticGuard?.croNow || "0"));
        const st     = Number(m.status);
        if (st === 1 || st === 2) {
          if (optCro > apiCro) m.cro_current_wei = optCro.toString();
        } else if (st === 3 || st === 4) {
          if (optCro && optCro < apiCro) m.cro_current_wei = optCro.toString();
        }
      } catch {}
    }

    const newStatus = Number(m.status);
    const curStatus = Number(stageCurrentStatus ?? -1);

    // NO-REGRESS GUARD: ignore stale snapshots from API/indexer (except 3↔4)
    const toggle34 = (newStatus === 3 && curStatus === 4) || (newStatus === 4 && curStatus === 3);

    // Sticky cooldown: while Paused and cooldown not finished, ignore 4→3 flips
    if (curStatus === 4 && newStatus === 3) {
      const now = Math.floor(Date.now()/1000);
      if (__pauseUntilSec && now < __pauseUntilSec) {
        scheduleRetry(retries);
        return;
      }
    }

    // Post-cooldown: after we locally flipped to Active, ignore 3→4 unless it's a NEW pause
    if (curStatus === 3 && newStatus === 4) {
      const now = Math.floor(Date.now()/1000);
      const pauseTs = Number(m.pause_timestamp || 0);
      const looksStale = !pauseTs || pauseTs <= __lastPauseTs;
      if (__postCooldownUntilSec && now < __postCooldownUntilSec && looksStale) {
        scheduleRetry(retries);
        return;
      }
    }

    // Normal no-regress (except active↔paused)
    if (curStatus >= 0 && newStatus < curStatus && !toggle34) {
      scheduleRetry(retries);
      return;
    }

    buildStageLowerHudForStatus(m);

    //dbg("refreshOpenStageFromServer", { newStatus, stageCurrentStatus, retries });

    if (newStatus !== stageCurrentStatus) {
      setStageStatusImage(statusSlug(m.status));
      await bindRingToMission(m);
      await bindCenterTimerToMission(m);
      renderStageCtaForStatus(m);
      await renderStageEndedPanelIfNeeded(m);
      stageCurrentStatus = newStatus;

      // update sticky windows / last pause stamp
      if (newStatus === 4) {
        const info = cooldownInfo(m);
        __pauseUntilSec = info.pauseEnd || 0;
        __lastPauseTs   = Number(m.pause_timestamp || 0);
      } else if (newStatus === 3) {
        __pauseUntilSec = 0;
        // keep __postCooldownUntilSec as set by the local flip; it will expire on its own
      }

      dbg("refreshOpenStageFromServer APPLIED status", { stageCurrentStatus });
      await maybeShowMissionEndPopup(m);

      scheduleRetry(Math.min(retries, 1));
    } else {
      scheduleRetry(0);
    }
  } catch (e) {
    dbg("refreshOpenStageFromServer FAILED", e?.message || e);
    scheduleRetry(retries);
  } finally {
    stageRefreshBusy = false;
    if (stageRefreshPending) {
      stageRefreshPending = false;
      setTimeout(() => refreshOpenStageFromServer(1, Math.max(2500, delay)), 750);
    }
  }

}

function        smartReconcile(reason = "smart") {
  try {
    const gameMain = document.getElementById('gameMain');
    if (!gameMain || !gameMain.classList.contains('stage-mode')) return;
    if (!currentMissionAddr) return;
    if (Date.now() - (__lastPushTs || 0) < 2000) return;
    if (stageRefreshBusy) return;

    // Snapshot-only reconcile
    refreshOpenStageFromServer(1).catch(() => {});
  } catch {}
}

// #endregion





// #region Elements (DOM map)

const els = {
  joinableList:             document.getElementById("joinableList"),
  joinableEmpty:            document.getElementById("joinableEmpty"),
  refreshJoinableBtn:       document.getElementById("refreshJoinableBtn"),

  myMissionsList:           document.getElementById("myMissionsList"),
  myMissionsEmpty:          document.getElementById("myMissionsEmpty"),
  refreshMyBtn:             document.getElementById("refreshMyBtn"),

  missionDetail:            document.getElementById("missionDetailSection"),
  missionTitle:             document.getElementById("missionTitle"),
  missionCore:              document.getElementById("missionCore"),
  enrollmentsList:          document.getElementById("enrollmentsList"),
  enrollmentsEmpty:         document.getElementById("enrollmentsEmpty"),
  closeMissionBtn:          document.getElementById("closeMissionBtn"),
  reloadMissionBtn:         document.getElementById("reloadMissionBtn"),
  allMissionsList:          document.getElementById("allMissionsList"),
  allMissionsEmpty:         document.getElementById("allMissionsEmpty"),
  refreshAllBtn:            document.getElementById("refreshAllBtn"),
};

// Buttons & stage SVG roots:

const btnAllMissions      = document.getElementById("btnAllMissions"    );
const btnJoinable         = document.getElementById("btnJoinable"       );
const btnMyMissions       = document.getElementById("btnMyMissions"     );
const stage               = document.getElementById("gameStage"         );
const stageViewport       = document.getElementById("stageViewport"     );
const stageImg            = document.getElementById("stageImg"          );
const ringOverlay         = document.getElementById("ringOverlay"       );
const stageTitleText      = document.getElementById("stageTitleText"    );    
const stageStatusImgSvg   = document.getElementById("stageStatusImgSvg" );

// --- Vault video wiring (preloaded; no header buttons) ---

const vaultLayer = document.getElementById("vaultVideoLayer");
const vaultVideo = document.getElementById("vaultVideo");
if (vaultVideo) { try { vaultVideo.preload = "auto"; vaultVideo.load(); } catch {} }

let __vaultVideoFlowActive = false;
let __vaultVideoEndedAwaitingResult = false;
let __vaultVideoPendingWin = null; // { cro, round }

function closeAnyModals(){
  const overlay = document.getElementById("modalOverlay");
  const alertM  = document.getElementById("alertModal");
  const confirmM= document.getElementById("confirmModal");
  try { alertM?.classList.add("hidden"); } catch{}
  try { confirmM?.classList.add("hidden"); } catch{}
  try { overlay?.classList.remove("active"); } catch{}
}

function setRingAndTimerVisible(visible){
  if (__vaultIsOpen && visible) return; // NEW: never turn HUD back on once vault is open
  const disp = visible ? "" : "none";
  const timer = document.getElementById("vaultTimerText");
  if (timer) timer.style.display = disp;
  const vd = document.getElementById("vaultDisplay");
  if (vd) vd.style.display = disp;
  const ringCover = document.getElementById("ringCover");
  if (ringCover) ringCover.style.display = disp;
  const ringTrack = document.getElementById("ringTrack");
  if (ringTrack) ringTrack.style.display = disp;
}

function playVaultOpenVideoOnce(){
  if (!vaultLayer || !vaultVideo) return;

  __vaultVideoFlowActive = true;
  __vaultVideoEndedAwaitingResult = false;

  // Keep the vault CLOSED and HUD hidden before playback begins
  setVaultOpen(false);
  setRingAndTimerVisible(false);

  try {
    vaultLayer.style.display = "";
    vaultVideo.loop = false;
    vaultVideo.currentTime = 0;
    vaultVideo.play().catch(()=>{});
  } catch {}

  // Only switch the base art to OPEN after the video is actually playing.
  // (Prevents the pre-start flash of the open image.)
  const onPlaying = () => {
    vaultVideo.removeEventListener("playing", onPlaying);
    if (__vaultVideoFlowActive) {
      // small cushion so the first video frames are already on screen
      setTimeout(() => { if (__vaultVideoFlowActive) setVaultOpen(true, /*force*/ true); }, 300);
    }
  };
  vaultVideo.addEventListener("playing", onPlaying, { once: true });

  const onEnded = () => {
    vaultVideo.removeEventListener("ended", onEnded);
    if (__vaultVideoPendingWin){
      finalizeVaultOpenVideoWin();     // we already know the win result
    } else {
      // Ended but result not in yet; wait until RoundResult arrives
      __vaultVideoEndedAwaitingResult = true;
      vaultLayer.style.display = "none";
      // If ultimately not a win for the viewer, restore HUD later (RoundResult else branch)
    }
  };
  vaultVideo.addEventListener("ended", onEnded, { once:true });
}

function finalizeVaultOpenVideoWin(){
  // Switch to opened vault art (this also keeps timer/ring hidden when open)
  setVaultOpen(true);

  // Hide video now
  if (vaultLayer) vaultLayer.style.display = "none";

  // Show the success popup 2 seconds after the video ends (with final amount)
  const round = __vaultVideoPendingWin?.round ?? "?";
  const cro   = __vaultVideoPendingWin?.cro   ?? "?";
  setTimeout(() => {
    try { showAlert(`Congratulations! You banked ${cro} CRO in round ${round}!`, "success"); } catch {}
  }, 2000);

  // Clear flags
  __vaultVideoFlowActive = false;
  __vaultVideoEndedAwaitingResult = false;
  __vaultVideoPendingWin = null;
}

function disableTemporarily(btn, ms = 5000) {
  if (!btn) return;
  btn.disabled = true;
  btn.classList.add("is-disabled"); // optional CSS hook
  setTimeout(() => {
    btn.disabled = false;
    btn.classList.remove("is-disabled");
  }, ms);
}

// #endregion





// #region Stage utilities

async function  showGameStage(missionRaw){
  const mission = enrichMissionFromApi({ mission: missionRaw, enrollments: missionRaw.enrollments, rounds: missionRaw.rounds });
  document.getElementById('gameMain').classList.add('stage-mode');
  showOnlySection("gameStage");

  // Set title text (SVG)
  if (stageTitleText){
    const title = mission?.name || mission?.mission_address || "";
    stageTitleText.textContent = title;
  }

  // Load status image (SVG) and size it
  setStageStatusImage(statusSlug(mission.status));

  stageCurrentStatus = Number(mission?.status ?? -1);
  __lastPushTs = Date.now();

  // Remember which wallet the stage is currently painted for
  __lastViewerAddr = (window.walletAddress || "").toLowerCase();

  // If we enter while paused, initialize the local cooldown window
  if (stageCurrentStatus === 4) {
    const info = cooldownInfo(mission);
    __pauseUntilSec   = info.pauseEnd || 0;
    __lastPauseTs     = Number(mission.pause_timestamp || 0);
    // allow post-cooldown guard to function after resume
    __postCooldownUntilSec = 0;
  }

  // Scale image + overlay, then place everything from the vault center
  layoutStage();
  
  updateVaultImageFor(mission);

  // Build lower HUD pills using chain for Enrolling/Arming
  if (Number(mission?.status) === 1 || Number(mission?.status) === 2) {
    await rehydratePillsFromServer(mission, "openStage", true);
  } else {
    buildStageLowerHudForStatus(mission);
  }

  await bindRingToMission(mission);

  // Center timer bound to this mission's next deadline
  await bindCenterTimerToMission(mission);

  // CTA (status 1 → JOIN MISSION)
  renderStageCtaForStatus(mission);

  await renderStageEndedPanelIfNeeded(mission);

}

// #endregion





// #region HUD (pills) system

// Layout helpers

const IMG_W = 2000
const IMG_H = 2000;
const visibleRectangle  = { 
  x:566, 
  y:420, 
  w:914, 
  h:1238 
};  // visibleRectangle was the rectangle from the phone header to footer space and phone 
    // screen width on the vault bg image. This rectangle is always visible on every screen

function        layoutStage(){
  if (!stage || !stageViewport || !stageImg) return;

  // Keep scale tied to the "visible rectangle" between header & footer
  const headerH = (document.querySelector(".app-header")?.offsetHeight) || 0;
  const footerH = (document.querySelector(".app-footer")?.offsetHeight) || 0;

  const availW  = window.innerWidth;
  const availH  = Math.max(0, window.innerHeight - headerH - footerH);

  const scale = Math.min(availH / visibleRectangle.h, availW / visibleRectangle.w);

  const w = Math.round(IMG_W * scale);
  const h = Math.round(IMG_H * scale);

  stageImg.style.width  = w + "px";
  stageImg.style.height = h + "px";

  if (ringOverlay){
    ringOverlay.style.width  = w + "px";
    ringOverlay.style.height = h + "px";
  }

  // ▼ NEW: keep the video layer locked to the same box as #stageImg
  const vLayer = document.getElementById('vaultVideoLayer');
  if (vLayer){
    vLayer.style.width  = w + "px";
    vLayer.style.height = h + "px";
  }
}

function        stageTextFill(){
  // Try to reuse the CTA note color; fall back to blue
  const sample = document.querySelector('#stageCtaGroup .cta-note');
  const cs = sample ? getComputedStyle(sample) : null;
  const val = cs?.fill || cs?.color || "";
  return val && val !== "none" ? val : "#00c0f0";
}

// SVG constants:

const SVG_NS = "http://www.w3.org/2000/svg";
const HUD = {
  maxRows:    4,                          // The maximum rows of pills              
  rectW:      214,                        // Pill rectangle width
  rectH:      32,                         // Pill rectangle height
  gapX:       10,                         // Horizontal gap between 2 pills
  gapY:       10,                         // Vertical gap between 2 pill lines
  xCenter:    500,                        // Center of visual image (1000 x 1000 viewbox)
  yFirst:     640, // !!!!!! First pill line y. Correct if rectH and/or gapY are changed. yFirst = Yfirst - (4x rectH diff + 3x gapY diff).
  labelFill:  "#7ad2ff",                // Label color
  valueFill:  "#fff",                   // Value color
  pillFill:   "rgba(26,29,35,.8)",      // Background color
  pillStroke: "rgba(62,211,245,.35)",   // Stroke color
  font:       "system-ui,Segoe UI,Arial", // Font
  fontSize:   14,                         // Font size
  labelY:     21, // !!!!!!! Label center y. Correct if rectH is changed. labelY = LabelY - rectH diff / 2.
  valueY:     21, // !!!!!!! Value center y. Correct if rectH is changed. valueY = valueY - rectH diff / 2.
  valueX:     140,                        // Value center x
  rx:         12,                         // Radius (?) x
  ry:         12,                         // Radius (?) y
};

// Helper used by pill library (single source)
function        playersAllStatsParts(m){
  const min    = Number(m?.enrollment_min_players ?? 0);
  const fromApi= (m?.enrolled_players != null)
    ? Number(m.enrolled_players)
    : Array.isArray(m?.enrollments) ? m.enrollments.length : 0;

  const joined = Math.max(
    fromApi,
    Number(optimisticGuard?.players || 0)
  );

  const max    = (m?.enrollment_max_players == null) ? "—" : String(m.enrollment_max_players);
  const color  = joined >= min ? "var(--success)" : "var(--error)";
  return { min, joined, max, color };
}

// Pill data:

const PILL_LIBRARY = { // Single source of truth for pill behaviors/labels
  missionType:    { label: "Mission Type",     value: m => missionTypeName[Number(m?.mission_type ?? 0)] },
  joinFrom:       { label: "Join from",        value: m => m?.enrollment_start ? formatLocalDateTime(m.enrollment_start) : "—" },
  joinUntil:      { label: "Join until",       value: m => m?.enrollment_end ? formatLocalDateTime(m.enrollment_end) : "—" },
  missionStartAt: { label: "Start At",         value: m => m?.mission_start    ? formatLocalDateTime(m.mission_start)    : "—" },
  duration:       { label: "Duration",         value: m => (m?.mission_start && m?.mission_end)
                                                 ? formatDurationShort(Number(m.mission_end) - Number(m.mission_start)) : "—" },
  fee:            { label: "Mission Fee",      value: m => (m && m.enrollment_amount_wei != null) ? `${weiToCro(m.enrollment_amount_wei, 2)} CRO` : "—" },
  poolStart:      { label: "Pool (start)",     value: m => (m && m.cro_start_wei    != null)      ? `${weiToCro(m.cro_start_wei, 2)} CRO`    : "—" },
  // game.js — PILL_LIBRARY
  poolCurrent: {
    label: "Pool (current)",
    value: m => {
      const st = Number(m?.status);
      if (st === 1 && m?.cro_initial_wei != null && m?.enrollment_amount_wei != null) {
        const initialWei = BigInt(String(m.cro_initial_wei || "0"));
        const feeWei     = BigInt(String(m.enrollment_amount_wei || "0"));

        const fromApi = (m?.enrolled_players != null)
          ? Number(m.enrolled_players)
          : Array.isArray(m?.enrollments) ? m.enrollments.length : 0;

        const joined  = BigInt(Math.max(fromApi, Number(optimisticGuard?.players || 0)));
        const derived = initialWei + feeWei * joined;
        const current = BigInt(String(m?.cro_current_wei || "0"));
        return `${weiToCro((current > derived ? current : derived).toString(), 2)} CRO`;
      }
      if (m?.cro_current_wei != null) return `${weiToCro(m.cro_current_wei, 2)} CRO`;
      return "—";
    }
  },
  playersCap:     { label: "Players cap",      value: m => (m?.enrollment_max_players ?? "—") },
  players: { label: "Players", value: m => {
    const fromApi = (m?.enrolled_players != null)
      ? Number(m.enrolled_players)
      : (Array.isArray(m?.enrollments) ? m.enrollments.length : 0);
    return Math.max(fromApi, Number(optimisticGuard?.players || 0));
  }},
  rounds:         { label: "Rounds",           value: m => Number(m?.mission_rounds_total ?? 0) },
  roundsOff: { 
    label: "Round", 
    value: m => {
      const cur   = nonDecreasingRound(m);
      const total = Number(m?.mission_rounds_total ?? 0);
      return `${Math.min(cur + 1, total)}/${total}`;
    }
  },
  roundsBanked: {
    label: "Rounds banked",
    value: m => {
      const cur   = nonDecreasingRound(m);                            // includes array + memory
      const total  = Number(m?.mission_rounds_total ?? 0);
      return `${Math.min(cur, total)}/${total}`;
    }
  },
  playersAllStats: {label: "Players",          value: m => {
    const { min, joined, max } = playersAllStatsParts(m);
      return `${joined} (Min ${min}/Max ${max})`;
    },
    // SVG renderer used by the stage HUD
    renderSvg: (valNode, m) => {
      const { min, joined, max, color } = playersAllStatsParts(m);
      const tCur = document.createElementNS(SVG_NS, "tspan");
      tCur.textContent = String(joined);
      tCur.setAttribute("style", `fill:${color}`);

      const tMin = document.createElementNS(SVG_NS, "tspan");
      tMin.textContent = ` (Min ${min}`;

      const tMax = document.createElementNS(SVG_NS, "tspan");
      tMax.textContent = `/Max ${max})`;

      valNode.textContent = "";
      valNode.appendChild(tCur);
      valNode.appendChild(tMin);
      valNode.appendChild(tMax);
    }
  },
  closesIn:       { label: "Closes In",        countdown: m => Number(m?.enrollment_end  || 0) },
  startsIn:       { label: "Starts In",        countdown: m => Number(m?.mission_start   || 0) },
  endsIn:         { label: "Ends In",          countdown: m => Number(m?.mission_end     || 0) },
};

const PILL_SETS = { // Which pills to show per status (0..7) — only using fields that exist in your payloads :contentReference[oaicite:0]{index=0}
  0:        ["joinFrom","joinUntil","missionStartAt","duration","fee","poolStart","playersCap","rounds"],           // Pending
  1:        ["missionType","rounds","fee","poolCurrent","playersAllStats","duration","closesIn","missionStartAt"],  // Enrolling
  2:        ["poolStart","players","rounds","startsIn","duration"],                                                 // Arming
  3:        ["poolCurrent","players","roundsOff","endsIn"],                                                         // Active
  4:        ["poolCurrent","players","roundsOff","endsIn"],                                                         // Paused
  default:  ["poolCurrent","players","roundsBanked"],                                                               // Ended variants
};

// Pills refresher:

async function  rehydratePillsFromServer(missionOverride = null) {
  try {
    if (!currentMissionAddr) return;

    let mSnap = missionOverride;
    if (!mSnap) {
      const data = await apiMission(currentMissionAddr, true);
      mSnap = enrichMissionFromApi(data);
    }

    try {
      __lastChainPlayers = Number(
        mSnap?.enrolled_players ??
        (Array.isArray(mSnap?.enrollments) ? mSnap.enrollments.length : 0)
      ) || 0;
    } catch {}

    buildStageLowerHudForStatus(mSnap);
  } catch (e) {
    console.warn("[pills/rehydrate] failed:", e?.message || e);
  } finally {
    __pillsHydrateLast = Date.now();
    __pillsHydrateBusy = false;
  }
}

// Chain/No-regress merge:

function        applyNoRegress(m){
  try {
    const st = Number(m?.status);
    if (st === 1 || st === 2) {
      const merged   = { ...m };
      const apiCro   = BigInt(String(m?.cro_current_wei ?? m?.cro_start_wei ?? "0"));

      // During the optimism window, never regress vs optimistic croNow
      if (Date.now() < (optimisticGuard?.untilMs || 0)) {
        try {
          const optCro = BigInt(String(optimisticGuard?.croNow || "0"));
          if (optCro > apiCro) merged.cro_current_wei = optCro.toString();
        } catch {}
      }
      return merged;
    }
  } catch {}
  return m;
}

// Builder:

function        buildStageLowerHudForStatus(mission){ // Build (and fill) the pills for the current mission/status
  const host = document.getElementById("stageLowerHud");
  if (!host) return;
  while (host.firstChild) host.removeChild(host.firstChild);

  const safe = applyNoRegress(mission);
  const keys = PILL_SETS[hudStatusFor(safe)] ?? PILL_SETS.default;

  // layout helpers
  const { rectW, rectH, gapX, gapY, xCenter, yFirst, rx, ry,
          pillFill, pillStroke, labelFill, valueFill, font, fontSize,
          labelY, valueY, valueX } = HUD;

  const xLeft   = xCenter - rectW - (gapX / 2);
  const xRight  = xCenter + (gapX / 2);
  const xSingle = xCenter - (rectW / 2);

  // 2 per row (left/right), last odd becomes single
  const placed = keys.map((k, i) => ({
    key: k,
    row: Math.floor(i/2),
    col: (i % 2 === 0) ? "left" : "right"
  }));
  if (keys.length % 2 === 1) { // last becomes single
    placed[placed.length - 1].col = "single";
  }

  const rowsUsed  = Math.ceil(keys.length / 2);
  const startRow  = Math.max(0, (HUD.maxRows || 4) - rowsUsed);  // bottom anchor

  for (const p of placed){
    const def = PILL_LIBRARY[p.key];
    if (!def) continue;
    const y = yFirst + (rectH + gapY) * (startRow + p.row);
    const x = (p.col === "left") ? xLeft : (p.col === "right") ? xRight : xSingle;

    const g = document.createElementNS(SVG_NS, "g");
    g.setAttribute("class", "pillGroup");
    g.setAttribute("transform", `translate(${Math.round(x)},${Math.round(y)})`);

    const rect = document.createElementNS(SVG_NS, "rect");
    rect.setAttribute("rx", rx); rect.setAttribute("ry", ry);
    rect.setAttribute("width", rectW); rect.setAttribute("height", rectH);
    rect.setAttribute("fill", pillFill);
    rect.setAttribute("stroke", pillStroke);
    g.appendChild(rect);

    const label = document.createElementNS(SVG_NS, "text");
    label.setAttribute("x", 10);
    label.setAttribute("y", labelY);
    label.setAttribute("font-family", font);
    label.setAttribute("font-size", fontSize);
    label.setAttribute("fill", labelFill);
    label.textContent = def.label;
    g.appendChild(label);

    const val = document.createElementNS(SVG_NS, "text");
    val.setAttribute("x", valueX);
    val.setAttribute("y", valueY);
    val.setAttribute("text-anchor", "middle");
    val.setAttribute("font-family", font);
    val.setAttribute("font-size", fontSize);
    val.setAttribute("fill", valueFill);

    if (typeof def.countdown === "function") {
      const ts = def.countdown(safe);
      if (ts > 0) {
        val.setAttribute("data-countdown", String(ts));
        val.textContent = formatCountdown(ts);
      } else {
        val.textContent = "—";
      }
    } else if (typeof def.renderSvg === "function") {
      // Let the pill define its own SVG rendering (single source)
      def.renderSvg(val, safe);
    } else {
      val.textContent = def.value ? def.value(safe) : "—";
    }

    g.appendChild(val);
    host.appendChild(g);
  }
}

// SVG image helper:

function        svgImage(href, x, y, w, h){
  const el = document.createElementNS(SVG_NS, "image");
  if (x != null) el.setAttribute("x", String(x));
  if (y != null) el.setAttribute("y", String(y));
  el.setAttribute("width",  String(w));
  el.setAttribute("height", String(h));
  el.setAttribute("href", href);
  el.setAttributeNS("http://www.w3.org/1999/xlink", "href", href);
  return el;
}

// UI status shim:

function        uiStatusFor(mission){ // Use "Active" (3) when simulating during Enrolling (1)
  const st = Number(mission?.status ?? -1);
  return st;
}

// HUD status shim:
function        hudStatusFor(mission){
  const st = Number(mission?.status ?? -1);
  const cur = Number(stageCurrentStatus ?? -1);
  const toggle34 = (st === 3 && cur === 4) || (st === 4 && cur === 3);

  // Never let pills go “backwards” below the visible stage, except allow 3↔4.
  if (cur >= 0 && st < cur && !toggle34) return cur;
  return st;
}

// #endregion





// #region CTA render&actions

// Layout constants:

const CTA_LAYOUT  = { // ── CTA assets & layout (single source of truth) 
  xCenter: 500,       // viewBox center
  topY:    555,       // same top Y for all CTAs (matches Join)
};

const CTA_JOIN    = { // JOIN (status 1)
  bg:    "assets/images/buttons/Button extra wide.png",
  text:  "assets/images/buttons/Join Mission text.png",
  btnW:  213, btnH: 50,
  txtW:  158, txtH: 23,
  txtDy: -1,           // vertical nudge (shadow compensation)
};

const CTA_ARMING  = { // ARMING (status 2) — disabled 2-line button: ENROLLMENT / CLOSED
  bg:     "assets/images/buttons/Button 2 lines wide.png",// ← replace with 2-line bg if you have one
  line1:  "assets/images/buttons/Enrollment text.png",    // ← set to your uploaded filename
  line2:  "assets/images/buttons/Closed text.png",        // ← set to your uploaded filename
  btnW:   213, btnH: 50,
  l1W:    120, l1H: 12,     // ← update to your actual text image sizes
  l2W:    90,  l2H: 12,     // ← update to your actual text image sizes
  gap:    4,                // vertical gap between lines
};

const CTA_ACTIVE  = { // ACTIVE (status 3) — BANK IT!
  bg:   "assets/images/buttons/Button extra wide.png",  // provided
  text: "assets/images/buttons/Bank it text.png", // provided
  btnW: 213, btnH: 50,     // consistent with other CTAs; PNG scales down nicely
  txtW: 160, txtH: 30,     // tweak if you want tighter fit
  txtDy: -1,
};

// Actions:

async function  handleEnrollClick       (mission){
  const signer = getSigner?.();
  if (!signer) { showAlert("Connect your wallet first.", "error"); return; }

  if (ctaBusy) return;     // ← guard
  ctaBusy = true;          // ← lock
  
  try {
    // freeze UI
    const btn = document.querySelector("#stageCtaGroup .cta-btn");
    const note = document.getElementById("stageCtaNote");
    if (btn) btn.classList.add("cta-disabled");
    if (note) note.textContent = "Joining…";

    const c  = new ethers.Contract(mission.mission_address, MISSION_ABI, signer);
    const val = mission.enrollment_amount_wei ?? "0";
    const tx  = await c.enrollPlayer({ value: val });

    // Kick the indexer as soon as we have the tx hash (UI also de-dupes ~2s)
    try {
      const me = (await signer.getAddress()).toLowerCase();
      postKickEnrolled({ mission: mission.mission_address, player: me, txHash: tx.hash }).catch(()=>{});
    } catch {}

    await tx.wait();

    // remember locally + instant disable for this wallet
    try {
      const me = (await signer.getAddress()).toLowerCase();
      joinedCacheAdd(mission.mission_address, me);
    } catch {}

    mission._joinedByMe = true;          // in-memory flag for this session
    renderStageCtaForStatus(mission);    // repaint CTA immediately
    showAlert("You joined the mission!", "success");
    
    // OPTIMISTIC UI UPDATE so the player sees the new numbers instantly
    try {
      const feeWei = String(mission.enrollment_amount_wei || "0");

      // base before-join count from snapshot fields only
      const beforeJoined = (mission?.enrolled_players != null)
        ? Number(mission.enrolled_players)
        : (Array.isArray(mission?.enrollments) ? mission.enrollments.length : 0);

      // remember optimistic values (longer window so DB/indexer can catch up)
      optimisticGuard = {
        untilMs: Date.now() + 15000, // 15s guard window
        players: beforeJoined + 1,
        croNow:  (BigInt(mission.cro_current_wei || mission.cro_start_wei || "0") + BigInt(feeWei)).toString()
      };

      // paint immediately
      const m2 = {
        ...mission,
        cro_current_wei: optimisticGuard.croNow,
      };
      buildStageLowerHudForStatus(m2);
      renderStageCtaForStatus(m2);

      // also kick a chain rehydrate to set __lastChainPlayers quickly
      rehydratePillsFromServer(mission, "joined:post-tx").catch(()=>{});
    } catch (e) {
      console.warn("[CTA/JOIN] optimistic paint failed:", e?.message || e);
    }

    // Let indexer catch up, then reconcile (no await, slight delay)
    // setTimeout(() => { refreshOpenStageFromServer(2).catch(()=>{}); }, 1200);
    refreshOpenStageFromServer(2).catch(()=>{});
  } catch (err) {
    // wallet cancel → warning, no “stuck” UI
    if (err?.code === 4001 || err?.code === "ACTION_REJECTED") {
      showAlert("Join canceled.", "warning");
    } else {
      // Prefer custom Mission errors; fall back to generic decodeError
      const custom = missionCustomErrorMessage(err);
      const msg = custom || `Join failed: ${decodeError(err)}`;
      showAlert(msg, custom ? "warning" : "error");
    }
  } finally {
    // UNLOCK first, then re-render (prevents “Joining…” from sticking)
    ctaBusy = false;
    try {
      const data = await apiMission(mission.mission_address, true);
      const m2 = enrichMissionFromApi(data);
      renderStageCtaForStatus(m2);
    } catch {
      renderStageCtaForStatus(mission);
    }
  }

}

async function  handleBankItClick       (mission){
  const signer = getSigner?.();
  if (!signer) { showAlert("Connect your wallet first.", "error"); return; }

  // Disable BANK IT CTA during tx to prevent multiple clicks
  try {
    const btn = document.querySelector("#stageCtaGroup .cta-btn");
    if (btn) {
      btn.classList.add("cta-disabled");
      btn.setAttribute("tabindex", "-1");
    }
  } catch {}

  // Show immediately for the clicking player (prevents race with fast RoundResult)
  showAlert("Round called. Waiting for result…", "info");

  // Mark a short local “banking in flight” window for this mission
  try {
    __bankingInFlight = {
      mission: String(mission.mission_address || "").toLowerCase(),
      startedAt: Math.floor(Date.now()/1000),
      hadResult: false
    };
  } catch {}

  try {
    const c  = new ethers.Contract(mission.mission_address, MISSION_ABI, signer);
    const tx = await c.callRound();

    // Kick the indexer as soon as we have the tx hash (UI also de-dupes ~2s)
    try {
      const me = (await signer.getAddress()).toLowerCase();
      postKickBanked({ mission: mission.mission_address, player: me, txHash: tx.hash }).catch(()=>{});
    } catch {}

    await tx.wait();

    // ▶ Hide any “waiting for result” modal and play the vault animation once
    closeAnyModals();

    // Mark self as already-won immediately so CTA hides and labels switch now
    try {
      const addrLc = String(mission.mission_address || "").toLowerCase();
      __viewerWonOnce?.add?.(addrLc);
    } catch {}

    // --- NEW: compute the just-banked amount now and reflect it immediately ---
    const lastTs   = getLastBankTs(mission, mission?.rounds);
    const winWei   = computeBankNowWei(mission, lastTs);                   // on-chain math you already use
    const winCro   = weiToCro(String(winWei), 2);
    const nextRnd  = Number(mission.round_count || 0) + 1;

    // Defer the winner popup to after the vault video finishes.
    // Stash the result so finalizeVaultOpenVideoWin() can display it later.
    __vaultVideoPendingWin = { cro: winCro, round: nextRnd };

    // Optimistically drop the pool by the payout right away
    let croAfter = mission.cro_current_wei;
    try {
      const prev = BigInt(String(mission.cro_current_wei ?? mission.cro_start_wei ?? "0"));
      croAfter   = (prev - BigInt(String(winWei || "0"))).toString();
    } catch {}
    optimisticGuard = { untilMs: Date.now() + 15000, players: optimisticGuard.players, croNow: croAfter };

    // Also bump round_count locally so “Round” pill stays in sync with pool
    const nextRoundCount = Number(mission.round_count || 0) + 1;

    // Remember the highest round we've seen for this mission
    try {
      const addrLc = String(mission.mission_address || "").toLowerCase();
      __maxRoundSeen[addrLc] = Math.max(Number(__maxRoundSeen[addrLc] || 0), nextRoundCount);
    } catch {}

    // Paint HUD with both new pool + incremented round
    buildStageLowerHudForStatus({ ...mission, cro_current_wei: croAfter, round_count: nextRoundCount });

    // Pre-arm the video flow immediately to prevent the open-art flash
    __vaultVideoFlowActive = true;                 // block updateVaultImageFor() from opening the art
    setVaultOpen(false, /*force*/ true);           // keep base art closed
    setRingAndTimerVisible(false);                 // hide HUD under the overlay

    // Keep your video + optimistic pause flip
    setTimeout(() => {
      try { playVaultOpenVideoOnce(); } catch {}
    }, 1000);
    await flipStageToPausedOptimistic(mission);

    // Quick chain hydrate to lock in truth (still fine if slow)
    try { rehydratePillsFromServer(mission, "bank:post-tx").catch(()=>{}); } catch {}

    // 4) (keep) quick reconcile so DB fields catch up
    await refreshOpenStageFromServer(2);

    // Pull pool from chain immediately in case push is slow
    rehydratePillsFromServer(mission, "post-callRound", true).catch(()=>{});

  } catch (err) {
    // clear the local “banking in flight” marker on failure/cancel
    __bankingInFlight = null;

    // Replace the waiting info with a clear outcome on failure/cancel
    if (err?.code === 4001 || err?.code === "ACTION_REJECTED") {
      showAlert("Banking canceled.", "warning");
      // keep a brief suppression so a transient ended snapshot doesn't pop a wrong modal
      __bankingInFlight = { mission: String(mission.mission_address||"").toLowerCase(),
                            startedAt: Math.floor(Date.now()/1000),
                            hadResult: false };
      setTimeout(() => { __bankingInFlight = null; }, 12000);

      // Re-enable BANK IT (we didn't change status)
      try {
        const btn = document.querySelector("#stageCtaGroup .cta-btn");
        if (btn) {
          btn.classList.remove("cta-disabled");
          btn.setAttribute("tabindex", "0");
        }
      } catch {}

      return;
    } else {
      const custom = missionCustomErrorMessage(err);
      const msg    = custom || `Bank it failed: ${decodeError(err)}`;
      showAlert(msg, custom ? "warning" : "error");

      // If revert indicates cooldown, flip UI to Paused optimistically
      if (stageCurrentStatus === 3 && (isCooldownError(err) || /Cooldown/i.test(custom || ""))) {
        await flipStageToPausedOptimistic(mission);
      }

      // If we are still in Active (3), re-enable BANK IT; otherwise (Paused), renderer controls state
      try {
        if (Number(stageCurrentStatus) === 3) {
          const btn = document.querySelector("#stageCtaGroup .cta-btn");
          if (btn) {
            btn.classList.remove("cta-disabled");
            btn.setAttribute("tabindex", "0");
          }
        }
      } catch {}
    }
  }
}

async function  refreshStageCtaIfOpen   (){
  const gameMain = document.getElementById('gameMain');
  if (!gameMain || !gameMain.classList.contains('stage-mode')) return;
  if (!currentMissionAddr) return;
  try {
    const data = await apiMission(currentMissionAddr, false);
    const m = enrichMissionFromApi(data);

    // ensure walletAddress is fresh from walletConnect before painting
    if (window.walletAddress) {
      m._viewerAddr = (window.walletAddress || "").toLowerCase();
    }

    // If the viewer changed (e.g., user switched accounts), force the base vault art
    const curViewer = (window.walletAddress || "").toLowerCase();
    if (curViewer !== __lastViewerAddr) {
      __lastViewerAddr = curViewer;
      // Decide open/closed strictly from the *new* viewer’s wins
      const shouldOpen = (Number(m.status) < 5) && viewerHasWin(m);
      // Force base art even if a previous video flow flag was set
      setVaultOpen(shouldOpen, /*force*/ true);
    }

    renderStageCtaForStatus(m);
    updateVaultImageFor(m);

  } catch {}
}

try {
  // When the app regains focus, refresh CTA + vault art for the current viewer
  window.addEventListener("focus", () => { 
    try { refreshStageCtaIfOpen(); } catch {}
  });
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) {
      try { refreshStageCtaIfOpen(); } catch {}
    }
  });
} catch {}

// CTA router:

async function  renderStageCtaForStatus (mission) {
  const host = document.getElementById("stageCtaGroup");
  if (!host) return;

  // ensure viewer identity is present for gating logic
  if (window.walletAddress) {
    mission._viewerAddr = (window.walletAddress || "").toLowerCase();
  }

  const st = uiStatusFor(mission);

  // Ignore stale snapshots that would repaint an older CTA
  if (typeof stageCurrentStatus === "number") {
    const now = Math.floor(Date.now()/1000);

    // While local cooldown is ticking, ignore 4→3 attempts
    if (Number(stageCurrentStatus) === 4 && st === 3) {
      if (__pauseUntilSec && now < __pauseUntilSec) {
        console.debug("[CTA] 4→3 ignored during cooldown");
        return;
      }
    }

    // Just after cooldown, ignore stale 3→4 unless it’s a NEW pause (higher pause_timestamp)
    if (Number(stageCurrentStatus) === 3 && st === 4) {
      const pauseTs = Number(mission?.pause_timestamp || 0);
      const looksStale = !pauseTs || pauseTs <= __lastPauseTs;
      if (__postCooldownUntilSec && now < __postCooldownUntilSec && looksStale) {
        console.debug("[CTA] 3→4 ignored post-cooldown (stale pause)");
        return;
      }
    }

    const toggle34 = (st === 3 && stageCurrentStatus === 4) || (st === 4 && stageCurrentStatus === 3);
    if (st < stageCurrentStatus && !toggle34) {
      console.debug("[CTA] stale snapshot ignored", { st, stageCurrentStatus });
      return;
    }
  }

  // Enrolling → show a disabled JOIN immediately, then refine gating async.
  if (st === 1) {
    // NEW: If an interactive JOIN is already on screen, don't flash the placeholder.
    const hasInteractiveJoin =
      !!host.querySelector('.cta-btn') &&
      host.querySelector('.cta-btn')?.getAttribute('tabindex') === '0';

    if (hasInteractiveJoin) {
      // Refine gating without the placeholder swap that can swallow the first click
      try { await renderCtaEnrolling(host, mission); } catch (e) { console.warn(e); }
      return;
    }

    host.innerHTML = "";
    renderJoinPlaceholder(host);                    // instant, no-await
    (async () => {                                  // refine without removing the CTA
      try { await renderCtaEnrolling(host, mission); } catch (e) { console.warn(e); }
    })();
    return;
  }

  // other statuses: keep existing “clear then draw”
  host.innerHTML = "";
  if (st === 2) return renderCtaArming (host, mission);
  if (st === 3) return renderCtaActive (host, mission);
  if (st === 4) return renderCtaPaused (host, mission);
}

function        renderJoinPlaceholder   (host){
  const { xCenter, topY } = CTA_LAYOUT;
  const { bg, text, btnW, btnH, txtW, txtH, txtDy } = CTA_JOIN;

  const x = xCenter - Math.round(btnW / 2);
  const y = topY;

  const g = document.createElementNS(SVG_NS, "g");
  g.setAttribute("class", "cta-btn cta-disabled");
  g.setAttribute("transform", `translate(${x},${y})`);
  g.setAttribute("role", "button");
  g.setAttribute("tabindex", "-1");

  g.appendChild(svgImage(bg, null, null, btnW, btnH));
  const txtX = x + Math.round((btnW - txtW) / 2);
  const txtY = y + Math.round((btnH - txtH) / 2) + (txtDy || 0);
  g.appendChild(svgImage(text, txtX - x, txtY - y, txtW, txtH));
  host.appendChild(g);

  const note = document.createElementNS(SVG_NS, "text");
  note.setAttribute("id", "stageCtaNote");
  note.setAttribute("x", String(xCenter));
  note.setAttribute("y", String(y + btnH + 18));
  note.setAttribute("text-anchor", "middle");
  note.setAttribute("class", "cta-note");
  note.textContent = walletAddress ? "Checking eligibility…" : "Connect your wallet to join";
  host.appendChild(note);
}

// Per-status renderers:

async function  renderCtaEnrolling      (host, mission)   {
  const { xCenter, topY } = CTA_LAYOUT;
  const { bg, text, btnW, btnH, txtW, txtH, txtDy } = CTA_JOIN;

  // center under vault
  const x = xCenter - Math.round(btnW / 2);
  const y = topY;

  // Gating (wallet, window, already, spots, optional canEnroll)
  const now   = Math.floor(Date.now()/1000);
  const inWin = now < Number(mission.enrollment_end || 0);

  const me = (walletAddress || "").toLowerCase();
  let hasSpots = true
  let canEnrollSoft = true;
  let eligibilityReason = "";

  // (A) local cache → instant after join & survives reload
  const alreadyByCache = joinedCacheHas(mission.mission_address, me);

  // (B) API enrollments → spectators / slower confirmation
  const list = Array.isArray(mission.enrollments) ? mission.enrollments : [];
  const alreadyByApi = !!list.find(e => {
    const a = String(e.player_address || e.address || e.player || "").toLowerCase();
    return a === me;
  });
  
  let already = alreadyByCache || alreadyByApi;

  try {
    // Pure snapshot gating (no on-chain reads)
    const maxP    = Number(mission.enrollment_max_players ?? 0);
    const joinedN = Array.isArray(mission.enrollments) ? mission.enrollments.length
                  : (typeof mission.enrolled_players === "number" ? mission.enrolled_players : 0);
    hasSpots      = maxP ? (joinedN < maxP) : true;

    // Server-side preflight to avoid revert gas
    // Disabled if no wallet address yet
    if (me) {
      try {
        const eg = await getPlayerEligibility(me);
        if (eg && eg.can_enroll === false) {
          canEnrollSoft = false;
          // Prefer backend reason if present, fallback to a generic note below
          // We’ll wire this into the final CTA message using `eligibilityReason`
          eligibilityReason = eg.reason || "";
        }
      } catch {
        // best-effort; on failure we keep canEnrollSoft=true and let tx revert (rare)
      }
    }
  } catch (err) {
    console.warn("[CTA/JOIN] chain probe failed:", err?.message || err);
  }

  // If the click is in progress, keep it disabled
  const justJoined = !!mission._joinedByMe;

  let disabled = false, note = "";
  if (!walletAddress)               { disabled = true; note = "Connect your wallet to join"; }
  else if (!inWin)                  { disabled = true; note = "Enrollment closed"; }
  else if (already || justJoined)   { disabled = true; note = "You already joined this mission"; }
  else if (!hasSpots)               { disabled = true; note = "No spots left for this mission"; }
  else if (!canEnrollSoft)          { disabled = true; note = eligibilityReason || "You’re not eligible to join right now"; }
  if (ctaBusy)                      { disabled = true; note = "Joining…"; }


  console.debug("[CTA/JOIN] gating", {
    me, inWin, alreadyByCache, alreadyByApi, already, hasSpots, canEnrollSoft
  });

  host.innerHTML = "";

  const g = document.createElementNS(SVG_NS, "g");
  g.setAttribute("class", "cta-btn" + (disabled ? " cta-disabled" : ""));
  g.setAttribute("transform", `translate(${x},${y})`);
  g.setAttribute("role", "button");
  g.setAttribute("tabindex", disabled ? "-1" : "0");

  // bg + text
  g.appendChild(svgImage(bg, null, null, btnW, btnH));

  const txtX = x + Math.round((btnW - txtW) / 2);
  const txtY = y + Math.round((btnH - txtH) / 2) + (txtDy || 0);
  const tx = svgImage(text, txtX - x, txtY - y, txtW, txtH);
  g.appendChild(tx);

  // note
  if (note) {
    const n = document.createElementNS(SVG_NS, "text");
    n.setAttribute("id", "stageCtaNote");
    n.setAttribute("x", String(xCenter));
    n.setAttribute("y", String(y + btnH + 18));
    n.setAttribute("text-anchor", "middle");
    n.setAttribute("class", "cta-note");
    n.textContent = note;
    host.appendChild(n);
  }

  if (!disabled) {
    g.addEventListener("click", () => handleEnrollClick(mission));
    g.addEventListener("mousedown", () => g.setAttribute("transform", `translate(${x},${y+1})`));
    const reset = () => g.setAttribute("transform", `translate(${x},${y})`);
    g.addEventListener("mouseup", reset);
    g.addEventListener("mouseleave", reset);
    g.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); handleEnrollClick(mission); }
    });
  }

  host.appendChild(g);
} 

function        renderCtaArming         (host, mission)   {
  const { xCenter, topY } = CTA_LAYOUT;
  const { bg, line1, line2, btnW, btnH, l1W, l1H, l2W, l2H, gap } = CTA_ARMING;

  const x = xCenter - Math.round(btnW / 2);
  const y = topY;

  const g = document.createElementNS(SVG_NS, "g");
  g.setAttribute("class", "cta-btn cta-disabled");
  g.setAttribute("transform", `translate(${x},${y})`);

  // bg
  g.appendChild(svgImage(bg, null, null, btnW, btnH));

  // two centered lines
  const blockH = l1H + (gap || 0) + l2H;
  const topPad = Math.max(0, Math.round((btnH - blockH) / 2));

  const l1x = x + Math.round((btnW - l1W) / 2);
  const l1y = y + topPad;
  g.appendChild(svgImage(line1, l1x - x, l1y - y, l1W, l1H));

  const l2x = x + Math.round((btnW - l2W) / 2);
  const l2y = l1y + l1H + (gap || 0);
  g.appendChild(svgImage(line2, l2x - x, l2y - y, l2W, l2H));

  host.appendChild(g);

  // One-line: "Starts in 00:12:34"
  const line = document.createElementNS(SVG_NS, "text");
  line.setAttribute("x", String(xCenter));
  line.setAttribute("y", String(y + btnH + 20));   // tweak this if you want it higher/lower
  line.setAttribute("text-anchor", "middle");
  line.setAttribute("class", "cta-note");

  // static label
  const tLabel = document.createElementNS(SVG_NS, "tspan");
  tLabel.textContent = "Mission starts in ";

  // dynamic value
  const tVal = document.createElementNS(SVG_NS, "tspan");
  const startTs = Number(mission?.mission_start || 0);
  if (startTs > 0) {
    tVal.setAttribute("data-countdown", String(startTs));
    tVal.textContent = formatCountdown(startTs);
  } else {
    tVal.textContent = "—";
  }

  tVal.style.fontWeight = "700";

  line.appendChild(tLabel);
  line.appendChild(tVal);
  host.appendChild(line);
}

function        renderCtaActive         (host, mission)   {
  const { xCenter, topY } = CTA_LAYOUT;
  const { bg, text, btnW, btnH, txtW, txtH, txtDy } = CTA_ACTIVE;
  const x = xCenter - Math.round(btnW / 2);
  const y = topY;

  // --- NEW: pre-render gating for view-only states ---
  const me = (mission._viewerAddr || (walletAddress || "")).toLowerCase();
  const addrLc = String(mission?.mission_address || "").toLowerCase();

  // joined? — trust multiple signals (flag/cache/API), then refine with chain
  let joined =
    !!(me) && (
      mission?._joinedByMe === true ||                              // set on successful local join
      joinedCacheHas(addrLc, me) ||                                 // local storage cache
      (mission?.enrollments || []).some(e => {                      // API snapshot
        const p = String(e?.player_address || e?.address || e?.player || "").toLowerCase();
        return p === me;
      })
    );

    // If still unsure, do a one-off snapshot check and repaint on success (no chain calls).
    if (!joined && me && addrLc) {
      (async () => {
        try {
          // Uses GET /missions/player/{address} via apiPlayerMissions()
          const mine = await apiPlayerMissions(me);
          const involved = Array.isArray(mine) && mine.some(m =>
            String(m?.mission_address || m?.address || "").toLowerCase() === addrLc
          );

          if (involved) {
            mission._joinedByMe = true;
            try { joinedCacheAdd(addrLc, me); } catch {}
            renderStageCtaForStatus(mission);
          }
        } catch {/* ignore – UI will catch up on next push */}
      })();
    }

  // already won any round?
  const alreadyWon = !!(me && (
    __viewerWonOnce.has(addrLc) ||
    (mission?.rounds || []).some(r => {
      const w = String(r?.winner_address || "").toLowerCase();
      return w === me;
    })
  ));

  let blockReason = "";
  if (!walletAddress)        blockReason = "Connect your wallet to bank";
  else if (!joined)          blockReason = "You did not join this mission";
  else if (alreadyWon)       blockReason = "View only. You already won a round";
  else if (__vaultIsOpen)    blockReason = "View only. You already won a round";  // never show BANK IT when vault is open

  if (blockReason) {
    // Centered message in the CTA area, no button rendered
    const msg = document.createElementNS(SVG_NS, "text");
    msg.setAttribute("x", String(xCenter));
    msg.setAttribute("y", String(y + Math.round(btnH / 2) + 25));
    msg.setAttribute("text-anchor", "middle");
    msg.setAttribute("class", "cta-block");         // was cta-note
    msg.style.fill = stageTextFill();               // paint now → no black flash
    msg.textContent = blockReason;
    host.appendChild(msg);
  } else {
    // --- Original clickable BANK IT! button ---
    const g = document.createElementNS(SVG_NS, "g");
    g.setAttribute("class", "cta-btn");
    g.setAttribute("transform", `translate(${x},${y})`);
    g.setAttribute("role", "button");
    g.setAttribute("tabindex", "0");

    // button background + text
    g.appendChild(svgImage(bg, null, null, btnW, btnH));
    const txtX = x + Math.round((btnW - txtW) / 2);
    const txtY = y + Math.round((btnH - txtH) / 2) + (txtDy || 0);
    g.appendChild(svgImage(text, txtX - x, txtY - y, txtW, txtH));

    // click → wallet popup
    g.addEventListener("click", () => handleBankItClick(mission));
    g.addEventListener("mousedown", () => g.setAttribute("transform", `translate(${x},${y+1})`));
    const reset = () => g.setAttribute("transform", `translate(${x},${y})`);
    g.addEventListener("mouseup", reset);
    g.addEventListener("mouseleave", reset);
    g.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); handleBankItClick(mission); }
    });

    host.appendChild(g);
  }

  // Line 1: "Ends in …" (auto-ticked by [data-countdown])
  const line = document.createElementNS(SVG_NS, "text");
  line.setAttribute("x", String(xCenter));
  line.setAttribute("y", String(y + btnH + 38));
  line.setAttribute("text-anchor", "middle");
  line.setAttribute("dominant-baseline", "middle");
  line.setAttribute("class", "cta-sub");   // was cta-note
  line.style.fill = stageTextFill();

  const tLabel = document.createElementNS(SVG_NS, "tspan");
  tLabel.textContent = "Ends in ";

  const tVal = document.createElementNS(SVG_NS, "tspan");
  const endTs = Number(mission?.mission_end || 0);
  if (endTs > 0) {
    tVal.setAttribute("data-countdown", String(endTs));
    tVal.textContent = formatCountdown(endTs); // paint initial value, no dash flash
  } else {
    tVal.textContent = "—";
  }
  tVal.style.fontWeight = "700";
  line.appendChild(tLabel);
  line.appendChild(tVal);
  host.appendChild(line);

  // Line 2: live bank/prize label (updates via [data-bank-now])
  const bank = document.createElementNS(SVG_NS, "text");
  bank.setAttribute("x", String(xCenter));
  bank.setAttribute("y", String(y + btnH + 64));
  bank.setAttribute("text-anchor", "middle");
  bank.setAttribute("class", "cta-note");
  bank.setAttribute("data-bank-now", "1");

  // decide label based on viewer eligibility
  const eligible  = !!walletAddress && joined && !alreadyWon;

  const lastTs = getLastBankTs(mission, mission?.rounds);
  const weiNow = computeBankNowWei(mission, lastTs);
  const croNow = weiToCro(String(weiNow), 2, true);
  const label  = eligible ? "Bank this round to claim:" : "Current round prize pool:";
  bank.textContent = `${label} ${croNow} CRO`;
  // apply progressive color based on how much has accrued
  applyCounterColor(bank, mission, weiNow);
  host.appendChild(bank);

}

function        renderCtaPaused         (host, mission){
  const { xCenter, topY } = CTA_LAYOUT;
  const { bg, text, btnW, btnH, txtW, txtH, txtDy } = CTA_ACTIVE; // keep sizes

  const x = xCenter - Math.round(btnW / 2);
  const y = topY;

  // NEW: if viewer already won → no future CTA, only info lines (shifted lower)
  const me  = (walletAddress || "").toLowerCase();
  const addrLc = String(mission?.mission_address || "").toLowerCase();
  const alreadyWon = !!(me && (
    __viewerWonOnce?.has?.(addrLc) ||
    (mission?.rounds || []).some(r => String(r?.winner_address || "").toLowerCase() === me)
  ));

  if (alreadyWon) {

    // 1) Center “View only. You already won a round”
    const msg = document.createElementNS(SVG_NS, "text");
    msg.setAttribute("class", "cta-block");
    msg.setAttribute("x", String(xCenter));
    msg.setAttribute("y", String(Math.round(y + btnH/2) + txtDy + 25));
    msg.setAttribute("text-anchor", "middle");
    msg.setAttribute("dominant-baseline", "middle");
    msg.style.fill = stageTextFill(); // color immediately → no black flash
    msg.textContent = "View only. You already won a round";
    host.appendChild(msg);

    // 2) Lines one step lower (+18 px)
    const line = document.createElementNS(SVG_NS, "text");
    line.setAttribute("class", "cta-sub");
    line.setAttribute("x", String(xCenter));
    line.setAttribute("y", String(y + btnH + 38));
    line.setAttribute("text-anchor", "middle");
    line.setAttribute("dominant-baseline", "middle");
    line.style.fill = stageTextFill();

    const tLabel = document.createElementNS(SVG_NS, "tspan");
    tLabel.textContent = "Ends in ";
    line.appendChild(tLabel);

    const tVal = document.createElementNS(SVG_NS, "tspan");
    const endTs = Number(mission?.mission_end || 0);
    if (endTs > 0) {
      tVal.setAttribute("data-countdown", String(endTs));
      tVal.textContent = formatCountdown(endTs);   // set initial value now (no dash flash)
    } else {
      tVal.textContent = "—";
    }
    tVal.style.fontWeight = "700";
    line.appendChild(tVal);

    host.appendChild(line);

    const bank = document.createElementNS(SVG_NS, "text");
    bank.setAttribute("class", "cta-sub");
    bank.style.fill = stageTextFill();  // ensure colored
    bank.setAttribute("x", String(xCenter));
    bank.setAttribute("y", String(y + btnH + 64));
    bank.setAttribute("text-anchor", "middle");
    bank.setAttribute("dominant-baseline", "middle");
    const nowWei = computeBankNowWei(mission, getLastBankTs(mission, mission?.rounds), Math.floor(Date.now()/1000));
    bank.setAttribute("data-bank-now", "1"); // will tick every second
    bank.textContent = `Accumulating: ${weiToCro(String(nowWei), 2, true)} CRO`;
    host.appendChild(bank);

    return; // important: no Cooldown button
  }

  // Paused: disabled button with visible "Cooldown: mm:ss"
  const g = document.createElementNS(SVG_NS, "g");
  g.setAttribute("class", "cta-btn cta-disabled");
  g.setAttribute("transform", `translate(${x},${y})`);

  // draw the PNG button background (bg is an image path, not a CSS class)
  g.appendChild(svgImage(bg, 0, 0, btnW, btnH));

  // centered countdown text on top of the button
  const label = document.createElementNS(SVG_NS, "text");
  label.setAttribute("x", String(Math.round(btnW / 2)));
  label.setAttribute("y", String(Math.round(btnH / 2) + txtDy + 5));
  label.setAttribute("text-anchor", "middle");
  label.setAttribute("dominant-baseline", "middle");
  label.setAttribute("class", "cta-sub");
  label.style.fill = stageTextFill();
  label.textContent = "Cooldown: ";

  // live-updating countdown value (ticked elsewhere via data-cooldown-end)
  const t = document.createElementNS(SVG_NS, "tspan");
  const info = cooldownInfo(mission);
  const end = Number(info.pauseEnd || 0);
  t.setAttribute("data-cooldown-end", String(end));
  // paint an initial value to avoid a dash flash
  t.textContent = end > 0 ? formatMMSS(Math.max(0, end - Math.floor(Date.now() / 1000))) : "—";

  label.appendChild(t);
  g.appendChild(label);
  host.appendChild(g);

  // Lines unchanged for non-winners:
  const line = document.createElementNS(SVG_NS, "text");
  line.setAttribute("class", "cta-sub");
  line.setAttribute("x", String(xCenter));
  line.setAttribute("y", String(y + btnH + 38));
  line.setAttribute("text-anchor", "middle");
  line.setAttribute("dominant-baseline", "middle");
  line.style.fill = stageTextFill();

  const tLabel = document.createElementNS(SVG_NS, "tspan");
  tLabel.textContent = "Ends in ";
  line.appendChild(tLabel);

  const tVal = document.createElementNS(SVG_NS, "tspan");
  const endTs = Number(mission?.mission_end || 0);
  if (endTs > 0) {
    tVal.setAttribute("data-countdown", String(endTs));
    tVal.textContent = formatCountdown(endTs);   // set initial value now (no dash flash)
  } else {
    tVal.textContent = "—";
  }
  tVal.style.fontWeight = "700";
  line.appendChild(tVal);

  host.appendChild(line);

  const bank = document.createElementNS(SVG_NS, "text");
  bank.setAttribute("class", "cta-sub");
  bank.style.fill = stageTextFill();
  bank.setAttribute("x", String(xCenter));
  bank.setAttribute("y", String(y + btnH + 64));
  bank.setAttribute("text-anchor", "middle");
  bank.setAttribute("dominant-baseline", "middle");
  const nowWei = computeBankNowWei(mission, getLastBankTs(mission, mission?.rounds), Math.floor(Date.now()/1000));
  bank.setAttribute("data-bank-now", "1");
  bank.textContent = `Accumulating: ${weiToCro(String(nowWei), 2, true)} CRO`;
  host.appendChild(bank);
}

async function  flipStageToPausedOptimistic(mission){
  const gameMain = document.getElementById('gameMain');
  if (!gameMain || !gameMain.classList.contains('stage-mode')) return;

  const now = Math.floor(Date.now()/1000);
  const m2  = { ...mission, status: 4, pause_timestamp: now }; // synthesize pause ts

  const info = cooldownInfo(m2);
  __pauseUntilSec = info.pauseEnd || 0;
  __lastPauseTs   = now;

  setStageStatusImage(statusSlug(4));

  // 1) Render CTA first so the timer's immediate paint can find data-countdown nodes.
  renderStageCtaForStatus(m2);

  // 2) Then bind ring and timer (startStageTimer runs paint() immediately)
  await bindRingToMission(m2);
  await bindCenterTimerToMission(m2);

  await renderStageEndedPanelIfNeeded(m2);
  stageCurrentStatus = 4;

  refreshOpenStageFromServer(2);
}

// Ended panel:

async function  maybeShowMissionEndPopup(mission){
  try{
    if (!mission) return;
    const st = Number(mission.status ?? -1);
    if (st < 5 || st === 7) return; // only Success/Partly Success

    // If the vault video is (or was just) playing, let it complete and then wait 5s
    if (__vaultVideoFlowActive) {
      const addr = String(mission.mission_address || currentMissionAddr || "").toLowerCase();
      if (!addr) return;
      if (__endPopupDeferred.has(addr)) return; // already queued

      __endPopupDeferred.add(addr);

      const trigger = () => {
        setTimeout(() => {
          __endPopupDeferred.delete(addr);
          // Re-run with the same mission; it will pass this guard and show the popup.
          maybeShowMissionEndPopup(mission);
        }, 5000);
      };

      // If the video already ended (waiting for RoundResult), trigger now; else on "ended"
      if (__vaultVideoEndedAwaitingResult) {
        trigger();
      } else if (typeof vaultVideo !== "undefined" && vaultVideo) {
        vaultVideo.addEventListener("ended", trigger, { once: true });
      } else {
        // Fallback: if we somehow lack the element, still delay ~5s
        trigger();
      }
      return;
    }

    const addr = String(mission.mission_address || currentMissionAddr || "").toLowerCase();
    if (!addr || __endPopupShown.has(addr)) return;

    // Suppress for the local last banker: if we just called a round and haven’t seen RoundResult yet
    try {
      if (__bankingInFlight && __bankingInFlight.mission === addr) {
        const now = Math.floor(Date.now()/1000);
        const age = now - Number(__bankingInFlight.startedAt || 0);
        if (!__bankingInFlight.hadResult && age <= 12) {
          console.debug("[EndPopup] Suppressed during local bank result race");
          return;
        }
      }
    } catch {}

    // Ensure we have enrollments + rounds for role detection
    let enrollments = Array.isArray(mission.enrollments) ? mission.enrollments : null;
    let rounds      = Array.isArray(mission.rounds)      ? mission.rounds      : null;
    if (!enrollments || !rounds){
      try{
        // Force fresh so role detection and any “you won” copy reflect the final round
        const data = await apiMission(addr, true);
        enrollments = data?.enrollments || [];
        rounds      = data?.rounds      || [];
      } catch {}
    }

    const me = (walletAddress || "").toLowerCase();
    const joined = !!(me && (enrollments || []).some(e => {
      const a = String(e.player_address || e.address || e.player || "").toLowerCase();
      return a === me;
    }));

    const isAllBanked = Number(mission?.round_count ?? -1) == Number(mission?.mission_rounds_total ?? -1);
    const reasonText  = isAllBanked ? "All rounds were banked." : "The mission timer ran out.";

    // Winner detection — prefer local truth first, then API
    const hasLocalWin = !!(addr && (__viewerWonOnce.has(addr) || __viewerWins.has(addr)));
    let myWins = [];
    if (me && Array.isArray(rounds)){
      myWins = rounds.filter(r => String(r.winner_address || "").toLowerCase() === me)
                     .sort((a,b) => Number(a.round_no || a.round || 0) - Number(b.round_no || b.round || 0));
    }

    // If we know locally that the viewer won but API hasn't caught up, retry later instead of showing a wrong “didn’t bank” message.
    if (hasLocalWin && myWins.length === 0) {
      setTimeout(() => { maybeShowMissionEndPopup(mission); }, 4000);
      return;
    }

    if (hasLocalWin || myWins.length){
      __endPopupShown.add(addr); // mark only when we are about to show something
      const last = myWins.length ? myWins[myWins.length - 1] : null;
      const roundNo = last ? Number(last.round_number ?? last.round_no ?? last.round ?? 0) : "";
      const cro = last ? weiToCro(String(last.payout_wei || last.amountWei || "0"), 2)
                       : ""; // local-only: amount may be unknown here
      const winMsg = last
        ? `🎉 Congratulations!<br/>You won round ${roundNo} and claimed <b>${cro} CRO</b>.<br/><small>${reasonText}</small>`
        : `🎉 Congratulations!<br/>You won a round!<br/><small>${reasonText}</small>`;
      showAlert(winMsg, "success");
      return;
    }

    if (joined){
      __endPopupShown.add(addr);
      const msg = isAllBanked
        ? "🏁 Mission complete.<br/>All rounds were banked and you didn’t bank a round this time.<br/><small>Better luck next mission!</small>"
        : "⏱️ Mission complete.<br/>Time’s up, and you didn’t bank a round this time.<br/><small>Better luck next mission!</small>";
      showAlert(msg, "info");
      return;
    }

    // Spectator: neutral reason-only line
    const spec = isAllBanked
      ? "🏁 Mission ended: all rounds were banked."
      : "⏱️ Mission ended: the mission time expired.";
    showAlert(spec, "info");

  } catch {/* silent */}
}

async function  renderStageEndedPanelIfNeeded(mission){
  const host = document.getElementById("stageEndedGroup");
  if (!host) return;
  host.innerHTML = "";

  const st  = Number(mission?.status ?? -1);
  if (st < 5) return; // only ended bucket

  // NEW — Failed (status 7): render reason + refund line and exit
  if (st === 7) {
    const x = 500, y = 585;
    const g = document.createElementNS("http://www.w3.org/2000/svg","g");
    g.setAttribute("fill", stageTextFill());

    const title = document.createElementNS("http://www.w3.org/2000/svg","text");
    title.setAttribute("x", String(x));
    title.setAttribute("y", String(y));
    title.setAttribute("text-anchor", "middle");
    title.setAttribute("class", "notice-text");
    title.textContent = "FAILED";
    title.style.fill = "#ff6b6b";
    title.style.fontWeight = "700";
    g.appendChild(title);

    const reason = failureReasonFor(mission); // ← NEW helper
    if (reason){
      const r = document.createElementNS("http://www.w3.org/2000/svg","text");
      r.setAttribute("x", String(x));
      r.setAttribute("y", String(y + 18));
      r.setAttribute("text-anchor", "middle");
      r.setAttribute("class", "notice-text");
      r.textContent = reason;
      g.appendChild(r);
    }

    const note = document.createElementNS("http://www.w3.org/2000/svg","text");
    note.setAttribute("x", String(x));
    note.setAttribute("y", String(y + 36));
    note.setAttribute("text-anchor", "middle");
    note.setAttribute("class", "notice-text");
    note.textContent = "All players will be refunded soon.";
    g.appendChild(note);

    host.appendChild(g);
    return; // <- do not fetch winners for failed
  }

  // Partly Success / Success (5/6) — “Top winners” + subtype note
  let enrollments = [], rounds = [];
  try {
    // Force a fresh snapshot so the last-banked round is included
    const data = await apiMission(mission.mission_address, true);
    enrollments = data?.enrollments || [];
    rounds      = data?.rounds       || [];
  } catch { /* keep empty */ }

  const winners = topWinners(enrollments, rounds, 3);

  const x = 500, y = 603, lineH = 16;
  const g = document.createElementNS("http://www.w3.org/2000/svg","g");
  g.setAttribute("fill", stageTextFill());

  // Title
  const t = document.createElementNS("http://www.w3.org/2000/svg","text");
  t.setAttribute("x", String(x));
  t.setAttribute("y", String(y));
  t.setAttribute("text-anchor", "middle");
  t.setAttribute("class", "notice-text");
  t.textContent = "Mission ended — Top winners";
  g.appendChild(t);

  // NEW: subtype note (one extra line for 5/6)
  let preLines = 1; // title only
  if (st === 5 || st === 6) {
    const sub = document.createElementNS("http://www.w3.org/2000/svg","text");
    sub.setAttribute("x", String(x));
    sub.setAttribute("y", String(y + lineH));
    sub.setAttribute("text-anchor", "middle");
    sub.setAttribute("class", "notice-text");
    sub.textContent = Number(mission?.round_count ?? -1) == Number(mission?.mission_rounds_total ?? -1) ? "All rounds were banked." : "Mission time ended.";
    g.appendChild(sub);
    preLines = 2; // title + subtitle
  }

  // Winners list
  winners.forEach((w, i) => {
    const row = document.createElementNS("http://www.w3.org/2000/svg","text");
    row.setAttribute("x", String(x));
    row.setAttribute("y", String(y + preLines*lineH + i*lineH));
    row.setAttribute("text-anchor", "middle");
    row.setAttribute("class", "notice-text");
    const cro = weiToCro(String(w.totalWei), 2);
    row.textContent = `${w.addr.slice(0,6)}…${w.addr.slice(-4)} — ${cro} CRO`;
    g.appendChild(row);
  });

  // “View all winners” link (shifted down by preLines)
  const link = document.createElementNS("http://www.w3.org/2000/svg","text");
  link.setAttribute("x", String(x));
  link.setAttribute("y", String(y + (preLines + winners.length + 1)*lineH));
  link.setAttribute("text-anchor", "middle");
  link.setAttribute("class", "notice-text");
  link.style.cursor = "pointer";
  link.textContent = "View all winners";
  link.addEventListener("click", async () => {
    stageReturnTo = "stage";
    try {
      const data = await apiMission(mission.mission_address, true);
      renderMissionDetail(data);
      els.missionDetail.classList.add("overlay");
      els.missionDetail.style.display = "block";
      lockScroll();
    } catch {
      // fallback if fetch fails
      await openMission(mission.mission_address);
    }
  });

  g.appendChild(link);
  host.appendChild(g);
}

// #endregion





// #region Lists & detail screens

function        buildMissionListCard    (m){
  const li = document.createElement("li");
  li.className = "mission-card";

  // live status (same approach used in All Missions)
  const stNum   = Number(m.status ?? 0);
  const mdShim  = { players: Array.from({ length: Number(m.enrolled_players || 0) }) };
  const pretty  = prettyStatusForList(stNum, mdShim, m.all_refunded);

  const stText  = pretty ? pretty.label : statusText(stNum);
  const stClass = pretty ? pretty.css   : statusColorClass(stNum);
  const stTitle = pretty ? pretty.title : "";

  // compute “next timebox” values for the mini rows
  const enrollStart = Number(m.enrollment_start || 0);
  const enrollEnd   = Number(m.enrollment_end   || 0);
  const missionStart= Number(m.mission_start    || 0);
  const missionEnd  = Number(m.mission_end      || 0);
  let timeKey1 = "", timeValAttr1 = "", timeVal1 = "—";
  let timeKey2 = "", timeValAttr2 = "", timeVal2 = "—";
  let timeKey3 = "", timeValAttr3 = "", timeVal3 = "—";
    if (stNum === 0) { // Pending
      timeKey1 = "Join from:";
      timeValAttr1 = ""; // no ticker on a fixed datetime
      timeVal1 = enrollStart ? formatLocalDateTime(enrollStart) : "—";
      timeKey2 = "Join until:";
      timeValAttr2 = ""; // no ticker on a fixed datetime
      timeVal2 = enrollStart ? formatLocalDateTime(enrollEnd) : "—";
      timeKey3 = "Start at:";
      timeValAttr3 = ""; // no ticker on a fixed datetime
      timeVal3 = enrollStart ? formatLocalDateTime(missionStart) : "—";
    } else if (stNum === 1) { // Enrolling
      timeKey1 = "Join until:";
      timeValAttr1 = ""; // no ticker on a fixed datetime
      timeVal1 = enrollEnd ? formatLocalDateTime(enrollEnd) : "—";
      timeKey3 = "Start at:";
      timeValAttr3 = ""; // no ticker on a fixed datetime
      timeVal3 = enrollStart ? formatLocalDateTime(missionStart) : "—";
    } else if (stNum === 2) { // Arming
      timeKey1 = "Starts in:";
      timeValAttr1 = `data-start="${missionStart}"`;
      timeVal1 = missionStart ? formatCountdown(missionStart) : "—";
    } else if (stNum === 3 || stNum === 4) { // Active / Paused
      timeKey1 = "Ends in:";
      timeValAttr1 = `data-end="${missionEnd}"`;
      timeVal1 = missionEnd ? formatCountdown(missionEnd) : "—";
    } else {
      timeKey1 = "Join from:";
      timeValAttr1 = ""; // no ticker on a fixed datetime
      timeVal1 = enrollStart ? formatLocalDateTime(enrollStart) : "—";
      timeKey2 = "Join until:";
      timeValAttr2 = ""; // no ticker on a fixed datetime
      timeVal2 = enrollStart ? formatLocalDateTime(enrollEnd) : "—";
      timeKey3 = "Start at:";
      timeValAttr3 = ""; // no ticker on a fixed datetime
      timeVal3 = enrollStart ? formatLocalDateTime(missionStart) : "—";
    }
  const duration  = (m.mission_start && m.mission_end) ? (Number(m.mission_end) - Number(m.mission_start)) : 0;
  const rounds    = Number(m.round_count || 0);
  const maxRounds = Number(m.mission_rounds_total || 0);
  const feeWei    = m.enrollment_amount_wei;
  const feeCro    = weiToCro(feeWei, 2);
  const curPlayers= Number(m.enrolled_players || 0);
  const minPlayers= Number(m.enrollment_min_players || 0);
  const maxPlayers= Number(m.enrollment_max_players || 0);
  const showDuration = duration > 0;
  const showRounds   = (rounds > 0) || (maxRounds > 0);
  const showFee      = Number(feeWei) > 0;
  const showPlayers  = (maxPlayers > 0) || (curPlayers > 0) || (minPlayers > 0);
  const playersPct   = maxPlayers > 0 ? Math.min(100, Math.round((curPlayers / maxPlayers) * 100)) : 0;

  const statusBadges = (() => {
    const main = `<span class="status-pill ${stClass}" title="${stTitle}">${stText}</span>`;
    const extras = (pretty && Array.isArray(pretty.extra) && pretty.extra.length)
      ? pretty.extra.map(e => `<span class="status-pill ${e.css}" title="${e.title||""}">${e.label}</span>`).join("")
      : "";
    return main + extras;
  })();

  li.dataset.addr = (m.mission_address || "").toLowerCase();
  li.innerHTML = `
    <div class="mission-head d-flex justify-content-between align-items-center">
      <div class="mission-title">
        <span class="title-text">${m.name || m.mission_address}</span>
      </div>
      <div class="d-flex gap-2 align-items-center">${statusBadges}</div>
    </div>

    ${ (showDuration || showRounds) ? `
    <div class="mini-row">
      ${ showDuration ? `
        <div class="label">Duration:</div>
        <div class="value">${formatDurationShort(duration)}</div>
      ` : `<div class="label"></div><div class="value"></div>`}
      ${ showRounds ? `<div class="ms-auto fw-bold">Rounds ${rounds}/${maxRounds}</div>` : ``}
    </div>` : ``}

    ${ showFee ? `
    <div class="mini-row">
      <div class="label">Mission Fee:</div>
      <div class="value">${feeCro} CRO</div>
    </div>` : ``}

    ${timeKey1 ? `
    <div class="mini-row">
      <div class="label">${timeKey1}</div>
      <div class="value"><span ${timeValAttr1}>${timeVal1}</span></div>
    </div>` : ""}

    ${timeKey2 ? `
    <div class="mini-row">
      <div class="label">${timeKey2}</div>
      <div class="value"><span ${timeValAttr2}>${timeVal2}</span></div>
    </div>` : ""}

    ${timeKey3 ? `
    <div class="mini-row">
      <div class="label">${timeKey3}</div>
      <div class="value"><span ${timeValAttr3}>${timeVal3}</span></div>
    </div>` : ""}

    ${ showPlayers ? `
    <div class="players-line">
      <div class="label me-2">Players</div>
      <div class="players-count">
        <span class="current" data-current="${curPlayers}" data-min="${minPlayers}">${curPlayers}</span>/<span class="max">${maxPlayers || "—"}</span>
      </div>
    </div>
    ${ maxPlayers > 0 ? `<div class="progress-slim"><i style="--w:${playersPct}%"></i></div>` : ``}
    ` : ``}
  `;

  li.querySelector('.mission-title .title-text').title = m.name || m.mission_address;
  li.addEventListener("click", () => openMission(m.mission_address));
  li.style.visibility = "hidden";
  return li;
}

function        renderAllMissions       (missions = []) {
  const ul = document.getElementById("allMissionsList");
  const empty = document.getElementById("allMissionsEmpty");
  if (!ul || !empty) return;
  ul.classList.add("card-grid");
  ul.innerHTML = "";
  if (!missions.length) { empty.style.display = ""; return; }
  empty.style.display = "none";

  const list = missions;
  for (const m of list) {
    const li = buildMissionListCard(m);
    ul.appendChild(li);
  }

  const cards = [...ul.querySelectorAll("li.mission-card")];
  if (cards.length) {
    const ver = ++__allMissionsRenderVersion;
    const stepMs = Math.ceil(500 / cards.length);
    cards.forEach((el, i) => {
      setTimeout(() => { if (ver === __allMissionsRenderVersion) el.style.visibility = ""; }, i * stepMs);
    });
  }

  startJoinableTicker();
}

function        renderJoinable          (items){
  const host = els.joinableList;
  host.innerHTML = "";
  els.joinableEmpty.style.display = items?.length ? "none" : "";
  host?.classList.add('card-grid');

  // newest first
  const list = (items || []).slice().reverse();
  for (const raw of list){
    // normalize a Joinable item to the All-Missions shape the helper expects
    const m = {
      mission_address:        raw.mission_address,
      name:                   raw.name,
      status:                 Number(raw.status),
      enrollment_start:       raw.enrollment_start,
      enrollment_end:         raw.enrollment_end,
      enrollment_amount_wei:  raw.enrollment_amount_wei,
      enrollment_min_players: raw.enrollment_min_players,
      enrollment_max_players: raw.enrollment_max_players,
      mission_start:          raw.mission_start,
      mission_end:            raw.mission_end,
      mission_rounds_total:   (raw.mission_rounds_total ?? raw.missionRounds ?? 0),
      round_count:            Number(raw.round_count || 0),
      cro_initial_wei:        raw.cro_initial_wei,
      cro_start_wei:          raw.cro_start_wei,
      cro_current_wei:        raw.cro_current_wei,
      pause_timestamp:        raw.pause_timestamp,
      updated_at:             raw.updated_at,
      mission_created:        raw.mission_created,
      round_pause_secs:       raw.round_pause_secs,
      last_round_pause_secs:  raw.last_round_pause_secs,
      creator_address:        raw.creator_address,
      all_refunded:           raw.all_refunded,
      // critically: enrolled_players for badge & players-line
      enrolled_players:       (raw.enrolled_players != null)
                                ? raw.enrolled_players
                                : (Array.isArray(raw.enrollments) ? raw.enrollments.length : 0),
    };

    host.appendChild(buildMissionListCard(m));
  }

  const cards = [...host.querySelectorAll("li.mission-card")];
  if (cards.length) {
    const ver = ++__allMissionsRenderVersion;
    const stepMs = Math.ceil(500 / cards.length);
    cards.forEach((el, i) => {
      setTimeout(() => { if (ver === __allMissionsRenderVersion) el.style.visibility = ""; }, i * stepMs);
    });
  }

  startJoinableTicker();
}

function        renderMyMissions        (items){
  const host = els.myMissionsList;
  host.innerHTML = "";
  els.myMissionsEmpty.style.display = items?.length ? "none" : "";
  host?.classList.add('card-grid');

  const list = (items || []).slice().reverse(); // newest first
  for (const raw of list){
    // normalize to the helper’s shape
    const m = {
      mission_address:        raw.mission_address,
      name:                   raw.name,
      status:                 Number(raw.status),
      enrollment_start:       raw.enrollment_start,
      enrollment_end:         raw.enrollment_end,
      enrollment_amount_wei:  raw.enrollment_amount_wei,
      enrollment_min_players: raw.enrollment_min_players,
      enrollment_max_players: raw.enrollment_max_players,
      mission_start:          raw.mission_start,
      mission_end:            raw.mission_end,
      mission_rounds_total:   Number(raw.mission_rounds_total || raw.missionRounds || 0),
      round_count:            Number(raw.round_count || 0),
      cro_initial_wei:        raw.cro_initial_wei,
      cro_start_wei:          raw.cro_start_wei,
      cro_current_wei:        raw.cro_current_wei,
      pause_timestamp:        raw.pause_timestamp,
      updated_at:             raw.updated_at,
      mission_created:        raw.mission_created,
      round_pause_secs:       raw.round_pause_secs,
      last_round_pause_secs:  raw.last_round_pause_secs,
      creator_address:        raw.creator_address,
      all_refunded:           raw.all_refunded,
      enrolled_players:       (raw.enrolled_players != null)
                                ? raw.enrolled_players
                                : (Array.isArray(raw.enrollments) ? raw.enrollments.length : 0),
    };

    host.appendChild(buildMissionListCard(m));
  }

  const cards = [...host.querySelectorAll("li.mission-card")];
  if (cards.length) {
    const ver = ++__allMissionsRenderVersion;
    const stepMs = Math.ceil(500 / cards.length);
    cards.forEach((el, i) => {
      setTimeout(() => { if (ver === __allMissionsRenderVersion) el.style.visibility = ""; }, i * stepMs);
    });
  }

  // optional: we can also start the ticker here if you want live countdowns on this tab
  startJoinableTicker();
}

async function  renderMissionDetail     ({ mission, enrollments, rounds }){
  const me   = (walletAddress || "").toLowerCase();
  const now  = Math.floor(Date.now()/1000);
  const isEnrolling = mission.status === 1 && now < mission.enrollment_end;
  const alreadyEnrolled = enrollments?.some(e => (e.player_address || "").toLowerCase() === me);
  const hasSpots = mission.enrollment_max_players > (enrollments?.length || 0);
  const canEnroll = isEnrolling && !alreadyEnrolled && hasSpots && walletAddress;

  const actions = document.getElementById("missionActions");
  actions.innerHTML = "";

  // If the mission is NOT ended (< 6) show an “Enter Mission” action
  if (Number(mission.status) < 6) {
    const btn = document.createElement("button");
    btn.className = "btn btn-cyan";
    btn.id = "btnEnterMission";
    btn.textContent = "Enter Mission";
    actions.appendChild(btn);
    // placeholder: wire up later to the in-mission HUD
    btn.addEventListener("click", async () => {
      await cleanupMissionDetail();

      // ↓↓↓ prevent pill bleed from previous mission
      resetMissionLocalState();

      currentMissionAddr = String(mission.mission_address).toLowerCase();
      await startHub();
      await subscribeToMission(currentMissionAddr);
      lockScroll();
      // pass enrollments/rounds so CTA can gate immediately
      await showGameStage({ ...mission, enrollments, rounds });
    });

  }

  els.missionDetail.classList.add("overlay");
  showOnlySection("missionDetailSection");
  els.missionTitle.textContent = mission.name || mission.mission_address;

  const st = Number(mission.status); // 0..7

  // Show only ONE countdown at most:
  // Pending   → Enrollment start
  // Enrolling → Enrollment end
  // Arming    → Mission start
  // Active    → Mission end
  // Paused    → Mission end
  // Ended     → none (all fixed)
  let countdownKey = null;
  if (st === 0)          countdownKey = "enrollStart";
  else if (st === 1)     countdownKey = "enrollEnd";
  else if (st === 2)     countdownKey = "missionStart";
  else if (st === 3 || st === 4) countdownKey = "missionEnd";
  // st >= 5 → ended → keep countdownKey = null

  const showEnrollStartCountdown  = (countdownKey === "enrollStart");
  const showEnrollEndCountdown    = (countdownKey === "enrollEnd");
  const showMissionStartCountdown = (countdownKey === "missionStart");
  const showMissionEndCountdown   = (countdownKey === "missionEnd");

  const statusCls = statusColorClass(mission.status);

  // No-regress: prefer chain-truth for Enrolling/Arming
  const safe = applyNoRegress(mission);
  const listJoined = Array.isArray(enrollments) ? enrollments.length : 0;
  const fromApi    = (mission?.enrolled_players != null)
    ? Number(mission.enrolled_players)
    : listJoined;

  const joinedPlayers = Math.max(
    fromApi,
    Number(__lastChainPlayers || 0),
    Number(optimisticGuard?.players || 0)
  );

  const minP = Number(safe?.enrollment_min_players ?? mission.enrollment_min_players ?? 0);
  const maxP = Number(safe?.enrollment_max_players ?? mission.enrollment_max_players ?? 0);
  const joinedCls   = (joinedPlayers >= minP) ? "text-success" : "text-error";

  const prettyDet = prettyStatusForList(
    Number(mission.status),
    mission,                                      // detail has the full object
    mission.all_refunded);
  const statusLbl  = prettyDet ? prettyDet.label : statusText(mission.status);
  const statusCls2 = prettyDet ? prettyDet.css   : statusColorClass(mission.status);
  const statusTtl  = prettyDet ? prettyDet.title : "";

  els.missionCore.innerHTML = `
    <div class="row g-3">
      <!-- Legend chips under the title (keep yours) -->
      <div class="col-12">
        <div class="d-flex flex-wrap align-items-center gap-2">
          <span class="status-pill status-${mission.status}">
            ${statusText(mission.status)}
          </span>
          <span class="status-pill">
            Type: ${missionTypeName[mission.mission_type] ?? mission.mission_type}
          </span>
          <span class="status-pill">
            Rounds: ${mission.round_count}/${mission.mission_rounds_total}
          </span>
          <span class="status-pill">
            Players: Minimum ${minP} | Joined <span class="${joinedCls}">${joinedPlayers}</span> | Maximum ${maxP || "—"}
          </span>
        </div>
      </div>

      <!-- 👇 Two-by-two aligned key/value pairs -->
      <div class="col-12">
        <div class="kv-grid">

          <div class="label">Mission address</div>
          <div class="value">
            ${copyableAddr(mission.mission_address)} ${addrLinkIcon(mission.mission_address)}
          </div>

          <div class="label">Mission type</div>
          <div class="value">${missionTypeName[mission.mission_type] ?? mission.mission_type}</div>

          <div class="label">Mission status</div>
          <div class="value"><span class="${statusCls2}" title="${statusTtl}">${statusLbl}</span></div>

          <div class="label">Rounds played</div>
          <div class="value">${mission.round_count}/${mission.mission_rounds_total}</div>

          <div class="label">Mission Fee</div>
          <div class="value">${weiToCro(mission.enrollment_amount_wei, 2)} CRO</div>

          <div class="label">Prize pool</div>
          <div class="value">${weiToCro(mission.cro_start_wei, 2)} CRO</div>

          <div class="label">Enrollment Start</div>
          <div class="value">
            ${showEnrollStartCountdown
              ? `<span id="enrollStartCountdown" class="countdown-time">${formatCountdown(mission.enrollment_start)}</span>`
              : `${formatLocalDateTime(mission.enrollment_start)}`}
          </div>

          <div class="label">Enrollment End</div>
          <div class="value">
            ${showEnrollEndCountdown
              ? `<span id="enrollEndCountdown" class="countdown-time">${formatCountdown(mission.enrollment_end)}</span>`
              : `${formatLocalDateTime(mission.enrollment_end)}`}
          </div>

          <div class="label">Mission Start</div>
          <div class="value">
            ${showMissionStartCountdown
              ? `<span id="missionStartCountdown" class="countdown-time">${formatCountdown(mission.mission_start)}</span>`
              : `${formatLocalDateTime(mission.mission_start)}`}
          </div>

          <div class="label">Mission End</div>
          <div class="value">
            ${showMissionEndCountdown
              ? `<span id="missionEndCountdown" class="countdown-time">${formatCountdown(mission.mission_end)}</span>`
              : `${formatLocalDateTime(mission.mission_end)}`}
          </div>

        <div class="label">Data updated at</div>
          <div class="value">
            <span id="updatedAtStamp"
                  data-updated="${Math.max(mission.updated_at || 0, Math.floor((__lastPushTs || 0)/1000))}">
              ${formatLocalDateTime(Math.max(mission.updated_at || 0, Math.floor((__lastPushTs || 0)/1000)))}
            </span>
            <i id="updatedAtIcon"

              class="fa-solid fa-circle-exclamation ms-1 text-error"
              style="display:none"
              title="Data may be stale"></i>
          </div>

        </div>

        <div class="mt-3">
          <h4>Players</h4>
          <div class="fw-bold">
            Minimum ${minP} | Joined <span class="${joinedCls}">${joinedPlayers}</span> | Maximum ${maxP || "—"}
          </div>
        </div>

      </div>
    </div>
  `;

  // countdowns + updated-at staleness (tick every second)
  stopCountdown();
  countdownTimer = setInterval(() => {
    const ids = [
      ["enrollStartCountdown", mission.enrollment_start],
      ["enrollEndCountdown",   mission.enrollment_end],
      ["missionStartCountdown",mission.mission_start],
      ["missionEndCountdown",  mission.mission_end]
    ];
    for (const [id, ts] of ids) {
      const el = document.getElementById(id);
      if (el) el.textContent = formatCountdown(ts);
    }

    // UPDATED AGE → mark red + exclamation when > 180s AND no pushes for > 75s
    const stamp = document.getElementById("updatedAtStamp");
    const icon  = document.getElementById("updatedAtIcon");
    if (stamp) {
      const t = Number(stamp.dataset.updated || 0);
      const age = Math.floor(Date.now()/1000) - t;
      const staleAge = age > 180;

      const lastPushAgeMs = Date.now() - (__lastPushTs || 0);
      const pushQuiet = lastPushAgeMs > 75000; // ~75s without any push

      // Only flag/warn during live states (1=enrolling, 2=arming, 3=active, 4=paused)
      const liveState = [1, 2, 3, 4].includes(Number(mission.status));

      // Visual staleness only when API looks old AND pushes have been quiet
      const showStaleMarker = liveState && staleAge && pushQuiet;
      stamp.classList.toggle("text-error", showStaleMarker);
      if (icon) icon.style.display = showStaleMarker ? "inline-block" : "none";

      // Modal warning only for live missions
      const warn = liveState && staleAge && pushQuiet;
      if (warn && !staleWarningShown) {
        showAlert("Live data looks quiet for over 2 minutes. Try reloading or check your connection.", "warning");
        staleWarningShown = true;
      }

      // If no longer stale (or not live), ensure any warning is cleared
      if (!warn) {
        staleWarningShown = false;
        const alertModal   = document.getElementById("alertModal");
        const modalOverlay = document.getElementById("modalOverlay");
        alertModal.classList.add("hidden");
        modalOverlay.classList.remove("active");
      }
    }
  }, 1000);

  // enrollments
  els.enrollmentsList.innerHTML = "";
  els.enrollmentsEmpty.style.display = enrollments?.length ? "none" : "";
  // build winner totals from rounds
  const winTotals = new Map();  // addr -> BigInt
  for (const r of (rounds || [])) {
    const w = (r.winner_address || "").toLowerCase();
    const amt = BigInt(r.payout_wei || "0");
    winTotals.set(w, (winTotals.get(w) || 0n) + amt);
  }

  // sort players by winnings desc, then by enrolled_at asc
  const sorted = [...(enrollments || [])].sort((a, b) => {
    const aw = winTotals.get((a.player_address || "").toLowerCase()) || 0n;
    const bw = winTotals.get((b.player_address || "").toLowerCase()) || 0n;
    if (aw !== bw) return aw > bw ? -1 : 1;
    const at = a.enrolled_at || 0, bt = b.enrolled_at || 0;
    return (at || 0) - (bt || 0);
  });

  els.enrollmentsList.innerHTML = "";
  els.enrollmentsEmpty.style.display = sorted.length ? "none" : "";
  for (const e of sorted) {
    const addr = (e.player_address || "");
    const wwei = winTotals.get(addr.toLowerCase()) || 0n;
    const won  = wwei > 0n;
    const wonLabel = won ? `${weiToCro(String(wwei), 2)} CRO` : "";

    const li = document.createElement("li");
    li.className = "view-card mb-2";
    li.innerHTML = `
      <div class="d-flex justify-content-between align-items-center">
        <div class="text-bold">
          ${copyableAddr(addr)} ${addrLinkIcon(addr)}
          ${won ? `<span class="small ms-2">
                    <i class="fa-solid fa-trophy text-warning" title="Winner"></i>
                    ${wonLabel}
                  </span>` : ""}
        </div>
        <div class="text-end small">
          ${e.refunded ? `<div class="text-warning">Refunded ${txLinkIcon(e.refund_tx_hash)}</div>` : ""}
        </div>
      </div>
    `;
    els.enrollmentsList.appendChild(li);
  }

}

// #endregion





// #region Global listeners

window.addEventListener("resize", layoutStage);

// #endregion





// #region Click handlers (nav)

// Wallet:

connectBtn.addEventListener     ("click", () => {
  if (walletAddress){
    showConfirm("Disconnect current wallet?", disconnectWallet);
  } else {
    connectWallet(); 
  }
});

// Tabs:

btnAllMissions?.addEventListener("click", async () => {
  await cleanupMissionDetail();
  showOnlySection("allMissionsSection");
  // fast paint from cache; guarded fetch will no-op during cool-down
  try { applyAllMissionFiltersAndRender(); } catch {}
  fetchAndRenderAllMissions();
});

btnJoinable?.addEventListener   ("click", async () => {
  await cleanupMissionDetail();
  showOnlySection("joinableSection");
  const joinable = await apiJoinable();
  renderJoinable(joinable);
  startJoinableTicker();
});

btnMyMissions?.addEventListener ("click", async () => {
  await cleanupMissionDetail();
  showOnlySection("myMissionsSection");
  if (walletAddress) {
    try {
      const mine = await apiPlayerMissions(walletAddress.toLowerCase());
      renderMyMissions(mine);
    } catch (err) { 
      console.error("[myMissions] failed" , err);
      showAlert("Couldn't load your missions.", "error"); 
    }
  }
});

// #endregion





// #region Tickers

// Detail countdown:

let countdownTimer = null;
function        stopCountdown(){ if (countdownTimer) { clearInterval(countdownTimer); countdownTimer = null; }}

// Joinable list:

let joinableTimer = null;
let joinableFetchInFlight = false;
let lastJoinableRefreshAt = 0;

function        startJoinableTicker(){
  stopJoinableTicker();

  const tick = () => {
    if (document.hidden) return; // don’t churn in background tabs

    const nowSec = Math.floor(Date.now() / 1000);
    let needsRefresh = false;

    document.querySelectorAll('[data-start]').forEach(el => {
      const t = Number(el.getAttribute('data-start') || 0);
      if (!t) { el.textContent = '—'; return; }

      const left = Math.max(0, t - nowSec);
      const prev = Number(el.dataset.prevLeft ?? Number.POSITIVE_INFINITY);
      el.dataset.prevLeft = String(left);
      el.textContent = formatCountdown(t);

      // EDGE: only when we cross from >0 to 0
      if (prev > 0 && left === 0) needsRefresh = true;
    });

    // NEW: players color (green when cur >= min, red otherwise)
    document.querySelectorAll('.players-count .current').forEach(el => {
      const cur = Number(el.dataset.current || 0);
      const min = Number(el.dataset.min || 0);
      const ok  = min > 0 && cur >= min;
      el.classList.toggle('text-success', ok);
      el.classList.toggle('text-error',  !ok && min > 0);
    });

    // small, safe cooldown and in-flight guard
    if (needsRefresh && !joinableFetchInFlight && (Date.now() - lastJoinableRefreshAt) > 5000) {
      joinableFetchInFlight = true;
      lastJoinableRefreshAt = Date.now();
      stopJoinableTicker();
      apiJoinable()
        .then(items => { renderJoinable(items); })
        .catch(() => {})
        .finally(() => { joinableFetchInFlight = false; startJoinableTicker(); });
    }
  };

  tick(); // one paint
  joinableTimer = setInterval(tick, 1000);
}

function        stopJoinableTicker(){
  if (joinableTimer) { clearInterval(joinableTimer); joinableTimer = null; }
}

// #endregion





// #region Interactions

async function  openMission(addr){
  try {
    currentMissionAddr = addr.toLowerCase();
    await subscribeToMission(currentMissionAddr);
    const data = await apiMission(currentMissionAddr, false);
    renderMissionDetail(data);
    scheduleDetailRefresh(true);
    window.scrollTo({ top: els.missionDetail.offsetTop - 20, behavior: "smooth" });
  } catch (e) {
    showAlert("Failed to load mission details.", "error");
    console.error(e);
  }
}

async function  closeMission(){
  const cameFromStage = (stageReturnTo === "stage") && !!currentMissionAddr;
  const addr = currentMissionAddr;

  els.missionDetail.classList.remove("overlay");
  els.missionDetail.style.display = "none";
  unlockScroll();

  await cleanupMissionDetail();
  stopCountdown();
  clearDetailRefresh();

  if (cameFromStage && addr){
    // Re-open the stage for the same mission
    try {
      const data = await apiMission(addr, false);
      const m = enrichMissionFromApi(data);
      await showGameStage(m);
      await apiMission(currentMissionAddr, true);
      await renderStageEndedPanelIfNeeded(m);
      stageReturnTo = null;
      return;
    } catch { /* fall through */ }
  }

  stageReturnTo = null;
  currentMissionAddr = null;

  let target = lastListShownId;
  try {
    const saved = localStorage.getItem("b6:lastList");
    if (saved) target = saved;
  } catch {}

  showOnlySection(target);
}

async function  subscribeToMission(addr){
  const targetLc = String(addr||"").toLowerCase();
  if (!targetLc) return;

  // Leave previous (if any)
  if (subscribedAddr && subscribedAddr !== targetLc) {
    try { await leaveMissionGroup(subscribedAddr); dbg("Unsubscribed group:", subscribedAddr); } catch {}
    subscribedGroups.clear();
  }

  // Join new
  await joinMissionGroup(targetLc);

  // Track for UI/debug parity
  let targetCk = null; try { targetCk = ethers.utils.getAddress(targetLc); } catch {}
  subscribedAddr   = targetLc;
  subscribedGroups = new Set([targetLc, targetCk].filter(Boolean));
}

// #endregion





// #region Init & boot

function        updateConnectText(){
  const span = document.getElementById("connectBtnText");
  if (span) span.textContent = walletAddress ? shorten(walletAddress) : "Connect Wallet";
}

async function init(){
  // 0) show list immediately
  showOnlySection("allMissionsSection");

  // Disable All-Missions refresh button for 5 seconds after showing the page
  try {
    if (els.refreshAllBtn) {
      els.refreshAllBtn.disabled = true;
      setTimeout(() => { try { els.refreshAllBtn.disabled = false; } catch {} }, 5000);
    }
  } catch {}

  try {
    if (els.refreshMyBtn) {
      els.refreshMyBtn.disabled = true;
      setTimeout(() => { try { els.refreshMyBtn.disabled = false; } catch {} }, 5000);
    }
  } catch {}

  document.addEventListener('click', enableVaultSoundOnce, { once: true });

  // 1) wire buttons BEFORE any awaited network work
  els.refreshJoinableBtn?.addEventListener("click", async () => {
    try {
      disableTemporarily(els.refreshJoinableBtn, 5000);
      const joinable = await apiJoinable();
      renderJoinable(joinable);
      startJoinableTicker();
    } catch(e){ console.error(e); }
  });

  // guard to avoid overlapping loads
  let allLoadBusy = false;
  els.refreshAllBtn?.addEventListener("click", async () => {
    if (allLoadBusy) return;
    allLoadBusy = true;
    try {
      disableTemporarily(els.refreshAllBtn, 5000);
      await withTimeout(fetchAndRenderAllMissions(), 12000);
    } catch (e) {
      console.warn("Refresh All Missions timed out/failed:", e);
      showAlert("Loading All Missions timed out. Please try again.", "warning");
    } finally {
      allLoadBusy = false;
    }
  });

  let allLoadBusy2 = false;
  els.refreshMyBtn?.addEventListener("click", async () => {
    if (allLoadBusy2) return;
    allLoadBusy2 = true;
    try {
      disableTemporarily(els.refreshMyBtn, 5000);
      await withTimeout(fetchAndRenderMyMissions(), 12000);
    } catch (e) {
      console.warn("Refresh My Missions timed out/failed:", e);
      showAlert("Loading My Missions timed out. Please try again.", "warning");
    } finally {
      allLoadBusy2 = false;
    }
  });

  // 2) other existing listeners (unchanged)
  els.closeMissionBtn?.addEventListener("click", closeMission);

  // 3) kick off first load; add a small delay if page was reloaded to avoid stampede
  let __startupDelayMs = 0;
  try {
    const nav = performance.getEntriesByType?.("navigation");
    const isReload = !!(nav && nav[0] && nav[0].type === "reload");
    if (isReload) __startupDelayMs = 800;
  } catch {}

  // 3) kick off first load, but throttle if the tab was just hard-refreshed
  try {
    const key = "_allListLastLoadAt";
    const last = Number(sessionStorage.getItem(key) || 0);
    const gap  = Date.now() - last;

    const start = async () => {
      await withTimeout(fetchAndRenderAllMissions(), 12000);
      try { sessionStorage.setItem(key, String(Date.now())); } catch {}
    };

    if (gap < 3500) {
      // Delay just enough so the total gap is ≥ 3.5s
      setTimeout(() => { start().catch(()=>{}); }, 3500 - gap);
    } else {
      await start();
    }
  } catch (e) {
    console.warn("Initial All Missions load timed out/failed:", e);
    showAlert("Loading All Missions timed out. Tap Refresh to retry.", "warning");
  }

  // 4) Manual reload button
  els.reloadMissionBtn?.addEventListener("click", async () => {
    if (!currentMissionAddr) { return; }
    try {
      const data = await apiMission(currentMissionAddr, true);
      renderMissionDetail(data);
      __lastPushTs = Date.now();
      touchUpdatedAtStampFromPush();
    } catch (e) {
      showAlert("Reload failed. Please check your connection.", "error");
    } 
  });

  buildAllFiltersUI();
  buildMyFiltersUI();

  // ───────────────────────────────────────────────────────────────
  // Deep-links:
  // - ?view=joinable|active|all     → keep existing behavior (lists)
  // - ?mission=0x...                → open mission DETAIL (existing)
  // - ?0x...                        → open mission directly in GAME STAGE (new)
  // ───────────────────────────────────────────────────────────────
  try {
    const rawQuery = (location.search || "").replace(/^\?/, "");
    const q        = new URLSearchParams(location.search);
    const view     = (q.get("view") || "").toLowerCase();
    const missionParam = q.get("mission");

    // Helper: extract bare 0x… from a query like "?0xabc..." or "?[0xabc...]"
    const bareAddr = (() => {
      if (!rawQuery) return null;
      // decode and strip brackets if user used "[0x...]" form
      let s = decodeURIComponent(rawQuery).trim();
      s = s.replace(/^\[|\]$/g, "");         // remove leading "[" or trailing "]"
      // only accept pure single token, no "&", no "="
      if (/[&=]/.test(s)) return null;
      return /^0x[a-fA-F0-9]{40}$/.test(s) ? s : null;
    })();

    if (bareAddr) {
      // NEW: open the GAME STAGE directly for a bare address deep-link
      await cleanupMissionDetail();
      // prevent pill bleed from any previous mission
      resetMissionLocalState?.();

      currentMissionAddr = String(bareAddr).toLowerCase();
      await startHub?.();
      await subscribeToMission(currentMissionAddr);
      lockScroll?.();

      // load and enrich before showing the stage
      const data = await apiMission(currentMissionAddr, true);
      if (!data) {
        showAlert("Mission not found.", "warning");
      } else {
        const m = enrichMissionFromApi?.(data) || data;
        await showGameStage(m);
        // make sure stage-side refresh & ended panel logic runs as normal
        await apiMission(currentMissionAddr, true);
        await renderStageEndedPanelIfNeeded?.(m);
      }
    } else if (missionParam) {
      // existing: open a specific mission’s DETAIL
      await cleanupMissionDetail();
      showOnlySection("missionDetailSection");
      const data = await apiMission(missionParam, true);
      if (data) {
        renderMissionDetail(data);
      } else {
        showAlert("Mission not found.", "warning");
      }
    } else if (view === "joinable") {
      await cleanupMissionDetail();
      showOnlySection("joinableSection");
      const joinable = await apiJoinable();
      renderJoinable(joinable);
      startJoinableTicker();
    } else if (view === "active") {
      await cleanupMissionDetail();
      showOnlySection("allMissionsSection");
      // statuses 2,3,4 = Arming/Active/Paused in your UI filter
      __allSelected = new Set([2,3,4]);
      applyAllMissionFiltersAndRender();
      const fltActive = document.getElementById("fltActive");
      if (fltActive) fltActive.checked = true;
    } else if (view === "all") {
      await cleanupMissionDetail();
      showOnlySection("allMissionsSection");
      __allSelected = null; // clear filters
      const host = document.getElementById("allFilters");
      host?.querySelectorAll('input[type="checkbox"]').forEach(i => i.checked = false);
      applyAllMissionFiltersAndRender();
    }
  } catch {}
  // ───────────────────────────────────────────────────────────────

  // event-based (if we add wallet events in walletConnect.js, see 1D)
  window.addEventListener("wallet:connected", () => {
    ctaBusy = false;
    fetchAndRenderAllMissions();
    if (walletAddress) enableGamePush(walletAddress);
  });

  window.addEventListener("wallet:changed", async () => {
    ctaBusy = false;
    try { __viewerWins?.clear?.(); } catch {}
    try { __viewerWonOnce?.clear?.(); } catch {}

    // Force-close vault art and stop any video immediately
    try {
      const layer = document.getElementById("vaultVideoLayer");
      if (layer) layer.style.display = "none";
      __vaultVideoFlowActive = false;
      __vaultVideoEndedAwaitingResult = false;
      __vaultVideoPendingWin = null;
      setVaultOpen(false, /*force*/ true);
      setRingAndTimerVisible(true);
    } catch {}

    updateConnectText();

    // Repaint CTA and vault art for the currently open mission
    try { await refreshStageCtaIfOpen(); } catch {}
  });

  window.addEventListener("wallet:disconnected", () => {
    ctaBusy = false;
    try { __viewerWins?.clear?.(); } catch {}
    try { __viewerWonOnce?.clear?.(); } catch {}

    // Force-close vault art and stop any video immediately
    try {
      const layer = document.getElementById("vaultVideoLayer");
      if (layer) layer.style.display = "none";
      __vaultVideoFlowActive = false;
      __vaultVideoEndedAwaitingResult = false;
      __vaultVideoPendingWin = null;
      setVaultOpen(false, /*force*/ true);
      setRingAndTimerVisible(true);
    } catch {}

    renderAllMissions([]);
    // optional: no-op (enableGamePush(null) returns early)
  });

  if (walletAddress) enableGamePush(walletAddress);

  window.addEventListener("wallet:connected",               refreshStageCtaIfOpen);
  window.addEventListener("wallet:changed",                 refreshStageCtaIfOpen);
  window.addEventListener("wallet:disconnected",            refreshStageCtaIfOpen);

  // Fallback if those custom events aren’t emitted:
  if (window.ethereum) {
    window.ethereum.on("accountsChanged", () => {
      // Force-close instantly, then refresh CTA/art
      try {
        const layer = document.getElementById("vaultVideoLayer");
        if (layer) layer.style.display = "none";
        __vaultVideoFlowActive = false;
        __vaultVideoEndedAwaitingResult = false;
        __vaultVideoPendingWin = null;
        setVaultOpen(false, /*force*/ true);
        setRingAndTimerVisible(true);
      } catch {}
      setTimeout(refreshStageCtaIfOpen, 0);
    });
  }

  window.addEventListener("wallet:connected",               updateConnectText);
  window.addEventListener("wallet:changed",                 updateConnectText);
  window.addEventListener("wallet:disconnected",            updateConnectText);

  // keep your existing CTA refreshers; add this fallback too:
  if (window.ethereum) {
    window.ethereum.on("accountsChanged", () => setTimeout( updateConnectText, 0));
  }

  // Tiny poll as last resort (stops once it detects a change)
  let lastWalletForCta = walletAddress || null;
  const ctaPoll = setInterval(() => {
    if ((walletAddress || null) !== lastWalletForCta) {
      lastWalletForCta = walletAddress || null;
      refreshStageCtaIfOpen();
      if (lastWalletForCta) clearInterval(ctaPoll);
    }
  }, 500);

  // When the browser tab comes back to foreground → one smart reconcile
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) smartReconcile("visible");
  });

  // When network goes back online → one smart reconcile
  window.addEventListener("online", () => smartReconcile("online"));

}

function enableVaultSoundOnce() {
  const vaultVideo = document.getElementById('vaultVideo');
  if (vaultVideo) {
    vaultVideo.muted = false;
  }
}

if (document.readyState === "loading"){
  document.addEventListener("DOMContentLoaded", () => {
    init();
    connectWallet();
    startHub();
  }, { once:true });
} else {
    init();
    connectWallet();
    startHub();
}

// #endregion
