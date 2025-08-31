/**********************************************************************
 game.js – home page bootstrap, re-uses core.js & walletConnect.js
**********************************************************************/

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
  READ_ONLY_RPC, 
  FACTORY_ADDRESS,
  MISSION_ABI,
  setBtnLoading, 
  decodeError,
  getReadProvider,
  shorten,
} from "./core.js";
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
const __mcIface           = new ethers.utils.Interface([
  "event MissionCreated(address indexed mission,string name,uint8 missionType,uint256 enrollmentStart,uint256 enrollmentEnd,uint8 minPlayers,uint8 maxPlayers,uint256 enrollmentAmount,uint256 missionStart,uint256 missionEnd,uint8 missionRounds)"
]);
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
const __endPopupShown       = new Set();
// All missions cache & filters:
let   __allMissionsCache    = [];     // last fetched list (raw objects)
let   __allFilterOpen       = false;
let   __allSelected         = null;   // null  → all statuses; otherwise Set<number> of allowed statuses
// Realtime:
let   __lastPushTs          = 0;      // updated on any hub push we care about
// Optimistic:
let   optimisticGuard       = { untilMs: 0, players: 0, croNow: "0" };
// Cache:
const __missionCreatedCache = new Map();
const JOIN_CACHE_KEY        = "_b6joined";

// miscellaneous 
let   staleWarningShown     = false;
let   ctaBusy               = false;

let __pillsHydrateBusy      = false;
let __pillsHydrateLast      = 0;
let __lastChainPlayers      = 0;
let __lastChainCroWei       = "0";
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
  hydrateAllMissionsRealtime(els.allMissionsList);
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

  // Fallbacks so HUD pills show live values even if API hasn’t denormalized yet
  if (Array.isArray(m.enrollments)) {

    // Pool (current) during Enrolling: start + fee * joined (no payouts yet)
    if (Number(m.status) === 1 &&
        m.cro_current_wei == null &&
        m.cro_start_wei != null &&
        m.enrollment_amount_wei != null) {
      const startWei = BigInt(String(m.cro_start_wei || "0"));
      const feeWei   = BigInt(String(m.enrollment_amount_wei || "0"));
      const joined   = BigInt(m.enrollments.length);
      m.cro_current_wei = (startWei + feeWei * joined).toString();
    }
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
      return `Incorrect entry fee. Expected ${weiToCro(String(expected))} CRO, sent ${weiToCro(String(sent))} CRO.`;
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
      return `Payout failed (${weiToCro(String(amount))} CRO to ${winner}).`;
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

  // Remember the last list view the user opened (now includes All Missions)
  if (["joinableSection","myMissionsSection","allMissionsSection"].includes(sectionId)) {
    lastListShownId = sectionId;
  }
}

// Cleanup:

async function  cleanupMissionDetail(){
  stopCountdown();
  stopStageTimer();
  unbindRing();
  clearDetailRefresh();
  staleWarningShown = false;

  if (hubConnection && subscribedGroups.size){
    for (const g of Array.from(subscribedGroups)) {
      try { await hubConnection.invoke("UnsubscribeMission", g); dbg("Unsubscribed (cleanup):", g); } catch {}
    }
    subscribedGroups.clear();
  }
  subscribedAddr = null;
  currentMissionAddr = null;
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

function        waitForMyRoundWin(meAddrLc, missionAddrLc, timeoutMs = 25000){
  return new Promise((resolve, reject) => {
    if (!hubConnection) { reject(new Error("hub not started")); return; }

    const onRR = (addr, round, winner, amountWei) => {
      try {
        const a = String(addr||"").toLowerCase();
        const w = String(winner||"").toLowerCase();
        if (a === missionAddrLc && w === meAddrLc) {
          cleanup();
          resolve({ round: Number(round), amountWei: String(amountWei || "0") });
        }
      } catch { /* ignore parse errors */ }
    };

    const cleanup = () => {
      try { hubConnection.off("RoundResult", onRR); } catch {}
      clearTimeout(to);
    };

    const to = setTimeout(() => { cleanup(); reject(new Error("timeout")); }, timeoutMs);
    hubConnection.on("RoundResult", onRR);
  });
}

function        getLastBankTs(mission, rounds){
  const t0 = Number(mission?.mission_start || 0);
  let last = t0;
  if (Array.isArray(rounds)) {
    for (const r of rounds) {
      const t = Number(r?.created_at || r?.played_at || 0);
      if (t > last) last = t;
    }
  }
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

function        cooldownInfo(mission, now = Math.floor(Date.now()/1000)){
  const st          = Number(mission?.status);
  const isPaused    = (st === 4);
  const roundsTotal = Number(mission?.mission_rounds_total ?? mission?.mission_rounds ?? 0);
  const roundCount  = Number(mission?.round_count ?? 0);
  const secsTotal   = (roundCount === (roundsTotal - 1)) ? 60 : 300;  // last round → 60s, else 300s
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

// Chain timestamp:

async function  getMissionCreationTs(mission){ // Used for circle timer if mission creation is used for start time timer.
  const inline = Number(mission.mission_created ?? 0);
  if (inline > 0) return inline;           // skip log scan when API provides it
  const addr = mission.mission_address;
  const keyLower = String(addr).toLowerCase();
  if (__missionCreatedCache.has(keyLower)) return __missionCreatedCache.get(keyLower);

  try{
    const provider = getReadProvider();

    // Event topic and indexed address topic
    const topic0        = __mcIface.getEventTopic("MissionCreated");
    const missionAddr   = ethers.utils.getAddress(addr);             // checksum
    const topicMission  = ethers.utils.hexZeroPad(missionAddr, 32);  // 0x00..addr

    const latest = await provider.getBlockNumber();
    const STEP   = 1800; // keep under 2000 range cap

    async function search(addressFilter){
      let to = latest;
      while (to >= 0){
        const from = Math.max(0, to - STEP + 1);
        const logs = await provider.getLogs({
          address: addressFilter,                 // FACTORY_ADDRESS or undefined
          topics:  [topic0, topicMission],        // MissionCreated + mission address
          fromBlock: from,
          toBlock:   to
        });

        if (logs && logs.length){
          // pick the earliest log we saw
          let first = logs[0];
          for (const L of logs) if (L.blockNumber < first.blockNumber) first = L;
          const blk = await provider.getBlock(first.blockNumber);
          return Number(blk?.timestamp || 0);
        }
        to = from - 1;
      }
      return 0;
    }

    // 1) Fast path: only the configured factory
    let ts = await search(FACTORY_ADDRESS);

    // 2) Fallback: global search (covers other factory instances)
    if (!ts) ts = await search(undefined);

    __missionCreatedCache.set(keyLower, ts || 0);
    return ts || 0;

  }catch(err){
    console.log("getMissionCreationTs failed:", err);
    return 0;
  }
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
  const min   = Number(mission?.enrollment_min_players ?? 0);
  const joined = Math.max(
    Array.isArray(mission?.enrollments) ? mission.enrollments.length : 0,
    Number(__lastChainPlayers || 0)
  );
  const rounds= Number(mission?.round_count            ?? 0);

  // Your two failure modes:
  // A) Not enough players enrolled (never started)
  if (rounds === 0 && joined < min)  return "Not enough players";
  // B) Enough players, but no rounds were played before end
  if (rounds === 0 && joined >= min) return "No rounds played";

  return null;
}

// #endregion





// #region Stage layout primitive

// Statusimage:

function        setStageStatusImage(slug){ /* Load + size the status word image and center it under the title */
  if (!stageStatusImgSvg || !slug) return;
  stageStatusImgSvg.setAttribute("href", `assets/images/statuses/${slug}.png`);
}

// Center timer core:

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

function        stageUnitFor(leftSec){ // Unit classifier used by the center text and the ring "reset" windows
  const s = Math.max(0, Math.floor(leftSec));
  if (s > 36*3600) return "d";
  if (s > 90*60)   return "h";
  if (s > 90)      return "m";
  return "s";
}

function        ringWindowForUnit(unit, phaseStart, endTs){
  if (!endTs) return [0,0];

  const now = Math.floor(Date.now()/1000);

  /*
  const H36 = 36 * 3600;
  const M90 = 90 * 60;
  const S90 = 90;

  // phaseStart is: creation (pending) / enroll_start / enroll_end (arming) / mission_start
  // If creation couldn’t be fetched, you already fall back when computing phaseStartTs.
  const start = phaseStart || (endTs - H36); // safe fallback to keep D/H sane
  const phaseLen = Math.max(0, endTs - start);

  if (unit === "d") {
    return [start, endTs];
  }
  if (unit === "h") {
    const S = (phaseLen < H36) ? start : (endTs - H36);
    return [S, endTs];
  }
  if (unit === "m") {
    const S = (phaseLen < M90) ? start : (endTs - M90);
    return [S, endTs];
  }
  // seconds unit stays a fixed trailing window
  return [endTs - S90, endTs];
  */
  return [now, endTs];
}

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

    // NEW: tick lower-HUD countdown pills (full d hh:mm:ss)
    document.querySelectorAll('#stageLowerHud [data-countdown], #stageCtaGroup [data-countdown]').forEach(el => {
      const ts = Number(el.getAttribute('data-countdown') || 0);
      el.textContent = ts ? formatCountdown(ts) : "—";
    });

    // NEW: update Bank-now (Active only; 2 decimals)
    document.querySelectorAll('#stageCtaGroup [data-bank-now]').forEach(el => {
      if (!missionObj) return;
      const last = getLastBankTs(missionObj, missionObj?.rounds);
      const wei  = computeBankNowWei(missionObj, last, now);
      const cro  = weiToCro(String(wei), 2);

      const st   = Number(missionObj?.status);
      const me   = (walletAddress || "").toLowerCase();
      const joined = !!(me && (missionObj?.enrollments || []).some(e => {
        const p = String(e?.player_address || e?.address || e?.player || "").toLowerCase();
        return p === me;
      }));
      const alreadyWon = !!(me && (missionObj?.rounds || []).some(r => {
        const w = String(r?.winner_address || "").toLowerCase();
        return w === me;
      }));
      const eligible = !!walletAddress && joined && !alreadyWon;

      let label =                         "Bank this round to claim:";       // Active & eligible
      if (st === 3 && !eligible) label =  "Prize pool right now:";  // Active view-only
      if (st === 4) label =               "Prize pool accruing:";   // Paused

      el.textContent = `${label} ${cro} CRO`;
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
      if (gapMs > 12000 && Date.now() > __watchdogCooldownUntil) {  // >12s silence
        __watchdogCooldownUntil = Date.now() + 6000;                 // 6s cooldown
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
            const m2 = { ...missionObj, status: next };

            setStageStatusImage(statusSlug(next));
            if (next === 1 || next === 2) {
              await rehydratePillsFromChain(m2, "deadlineFlip");
            } else {
              buildStageLowerHudForStatus(m2);
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
    Array.isArray(m.enrollments) ? m.enrollments.length : 0,
    Number(__lastChainPlayers || 0),
    Number(optimisticGuard?.players || 0)
  );
  const min = Number(m.enrollment_min_players ?? 0);

  if (now < es) return 0;                                              // Pending
  if (now < ee) return 1;                                              // Enrolling
  if (now < ms) return (cur >= min ? 2 : 7);                           // Arming OR Failed if min not met
  if (now < me) return (m.pause_timestamp ? 4 : 3);                    // Paused vs Active
  return (m.status >= 5 ? m.status : 6);                               // Ended bucket (keep subtype if present)
}

async function  bindCenterTimerToMission(mission){
  const endTs = nextDeadlineFor(mission);
  const startTs = Math.floor(Date.now()/1000);    // start windows from NOW

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
  const now = Math.floor(Date.now()/1000);
  let   E   = 0;

  if (st === 0) {                       // Pending
    E = Number(m.enrollment_start || m.mission_start || 0);
  } else if (st === 1) {                // Enrolling
    E = Number(m.enrollment_end || 0);
  } else if (st === 2) {                // Arming
    E = Number(m.mission_start || 0);
  } else if (st === 3 || st === 4) {    // Active / Paused
    E = Number(m.mission_end || 0);
  } else {
    setRingProgress(100);
    return;
  }

  if (E && E > now) {
    bindRingToWindow(now, E);           // ring window: NOW → deadline
  } else {
    setRingProgress(st >= 5 ? 100 : 0);
  }

  /*
  let S = 0, E = 0;

  if (st === 0) {                         // Pending: MissionCreated → enrollment_start
    E = Number(m.enrollment_start || 0);

    // MissionCreated event timestamp (cached). Fallback: updated_at.
    S = m?.mission_address ? await getMissionCreationTs(m) : 0;
    if (!S) S = Number(m.updated_at || 0);

    if (S && E && E > S) { bindRingToWindow(S, E); }
    else { setRingProgress(0); }
    return;
  } else if (st === 1) {                  // Enrolling
    S = Number(m.enrollment_start || 0);
    E = Number(m.enrollment_end   || 0);
  } else if (st === 2) {                  // Arming
    S = Number(m.enrollment_end   || 0);
    E = Number(m.mission_start    || 0);
  } else if (st === 3 || st === 4) {      // Active / Paused
    S = Number(m.mission_start    || 0);
    E = Number(m.mission_end      || 0);
  } else {
    // Ended variants
    setRingProgress(100);
    return;
  }

  if (S && E && E > S) {
    bindRingToWindow(S, E);
  } else {
    setRingProgress(st >= 5 ? 100 : 0);
  }
    */
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

async function  startHub() { // SignalR HUB
  if (!window.signalR) { showAlert("SignalR client script not found.", "error"); return; }

  if (!hubConnection) {
    hubConnection = new signalR.HubConnectionBuilder()
      .withUrl("/api/hub/game")
      .withAutomaticReconnect()
      .build();

    window.hubConnection = hubConnection;

    hubConnection.on("ServerPing", (msg) => {
      //showAlert(`Server ping:<br>${msg}`, "info");
      console.log(msg);
    });

    hubConnection.on("RoundResult", async (addr, round, winner, amountWei) => {
      __lastPushTs = Date.now();
      dbg("RoundResult PUSH", { addr, round, winner, amountWei, currentMissionAddr, groups: Array.from(subscribedGroups) });
      const me  = (walletAddress || "").toLowerCase();
      const win = String(winner || "").toLowerCase();
      const cro = weiToCro(String(amountWei));
      if (win === me) {
        showAlert(`Congratulations!<br/>You banked ${cro} CRO in round ${round}!`, "success");
      } else {
        showAlert(`Round ${round} banked by ${shorten(winner)}<br/>The winner banked: ${cro} CRO!`, "success");
      }

      if (!currentMissionAddr || addr?.toLowerCase() !== currentMissionAddr) {
        return;
      }

      // refresh the detail view (used for the auto-dismiss timer, etc.)
      try {
        const data = await apiMission(currentMissionAddr);

        // Only refresh the detail panel if the user is not on the stage
        const onStage = document.getElementById('gameMain')?.classList.contains('stage-mode');
        if (!onStage) renderMissionDetail(data);

        const mission = data?.mission || data;
        const { pauseEnd } = cooldownInfo(mission);
        if (pauseEnd) {
          const now = Math.floor(Date.now()/1000);
          const ms  = Math.max(0, (pauseEnd - 10) - now) * 1000;
          setTimeout(() => {
            const g = document.getElementById("stageNoticeGroup");
            if (g) g.innerHTML = "";
          }, ms);
        }
      } catch (err) {console.log("startHub RoundResult error: " + err)}

      // show inline “Round N banked by …” on the stage + rebuild the stage
      const gameMain = document.getElementById('gameMain');
      if (gameMain && gameMain.classList.contains('stage-mode')) {
        const me  = (walletAddress || "").toLowerCase();
        const win = String(winner || "").toLowerCase();
        if (win !== me) {
          renderRoundBankedNotice(Number(round), String(winner || ""), String(amountWei || "0"));
        }
        await refreshOpenStageFromServer(2);
      }

    });

    hubConnection.on("StatusChanged", async (addr, newStatus) => {
      __lastPushTs = Date.now();
      dbg("StatusChanged PUSH", { addr, newStatus, currentMissionAddr, groups: Array.from(subscribedGroups) });
      if (!currentMissionAddr || addr?.toLowerCase() !== currentMissionAddr) {
        return;
      }

      const gameMain = document.getElementById('gameMain');

      // try to fetch a fresh snapshot (for times/pills), but don't block the flip
      let mApi = null;
      try {
        const data = await apiMission(currentMissionAddr);
        mApi = enrichMissionFromApi(data);
      } catch (err) {console.log("startHub Statuschanged error: " + err)}

      if (gameMain && gameMain.classList.contains('stage-mode')) {
        const target = Number(newStatus);
        const mLocal = { ...(mApi || {}), status: target };

        setStageStatusImage(statusSlug(target));
        await bindRingToMission(mLocal);
        await bindCenterTimerToMission(mLocal);
        renderStageCtaForStatus(mLocal);
        await renderStageEndedPanelIfNeeded(mLocal);
        stageCurrentStatus = target;

        await maybeShowMissionEndPopup(mLocal);

        if (target === 1 || target === 2) {
          await rehydratePillsFromChain(mLocal, "statusChanged");
        } else {
          buildStageLowerHudForStatus(mLocal);
        }

        setTimeout(() => refreshOpenStageFromServer(2), 800);
      } else {
        if (mApi) renderMissionDetail({ mission: mApi });
      }
    });

    hubConnection.on("MissionUpdated", async (addr) => {
      __lastPushTs = Date.now();
      dbg("MissionUpdated PUSH", { addr, currentMissionAddr, groups: Array.from(subscribedGroups) });
      if (currentMissionAddr && addr?.toLowerCase() === currentMissionAddr) {
        try {
          const data = await apiMission(currentMissionAddr);
          const m = enrichMissionFromApi(data);

          const apiStatus = Number(m.status);
          const curStatus = Number(stageCurrentStatus ?? -1);

          // If API is behind our locally flipped stage, ignore now but retry soon.
          if (apiStatus < curStatus) {
            console.debug("[MissionUpdated] stale status from API; ignoring", apiStatus, "<", curStatus);
            setTimeout(() => refreshOpenStageFromServer(1), 1200); // gentle retry
            return;
          }

          // If API moved forward, do a full rebuild now.
          if (apiStatus !== curStatus) {
            setStageStatusImage(statusSlug(apiStatus));
            await bindRingToMission(m);
            await bindCenterTimerToMission(m);
            renderStageCtaForStatus(m);
            await renderStageEndedPanelIfNeeded(m);
            stageCurrentStatus = apiStatus;

            if (apiStatus === 1 || apiStatus === 2) {
              await rehydratePillsFromChain(m, "missionUpdated-forward");
            } else {
              buildStageLowerHudForStatus(m);
            }

            setTimeout(() => refreshOpenStageFromServer(2), 1600);
            return;
          }

          if (apiStatus === 1 || apiStatus === 2) {
            await rehydratePillsFromChain(m, "missionUpdated-same");
          } else {
            buildStageLowerHudForStatus(m);
          }
          if (walletAddress) renderStageCtaForStatus(m);

          setTimeout(() => refreshOpenStageFromServer(2), 1600);

        } catch (err) {console.log("startHub MissionUpdated error: " + err)}
      }
    });

    // on reconnect, re-subscribe to the open mission (if any)
    hubConnection.onreconnected(async () => {
      const H = signalR.HubConnectionState;
      if (hubConnection?.state !== H.Connected) return;

      // Re-subscribe to the groups we track
      if (subscribedGroups.size) {
        for (const g of Array.from(subscribedGroups)) {
          try { await hubConnection.invoke("SubscribeMission", g); dbg("Resubscribed:", g); }
          catch(e){ console.error("Resubscribe failed:", g, e); }
        }
      } else if (currentMissionAddr) {
        await subscribeToMission(currentMissionAddr);
      }

      // Mark "fresh push" time and reconcile once (lightweight)
      __lastPushTs = Date.now();
      smartReconcile("reconnected");
    });

  }

  const H = signalR.HubConnectionState;
  const st = hubConnection.state;

  // Already connected → just (re)subscribe
  if (st === H.Connected) {
    await safeSubscribe();
    return;
  }

  // If connecting or reconnecting, don't call start(); just wait and exit.
  if (st === H.Connecting || st === H.Reconnecting) {
    // optional: you can rely on onreconnected() to resubscribe
    console.debug("Hub is", stateName(st), "— not starting again.");
    return;
  }

  // Only start when Disconnected
  if (st === H.Disconnected) {
    if (!hubStartPromise) {
      hubStartPromise = hubConnection.start()
        .catch(err => { console.error("Hub start failed:", err); showAlert("Real-time channel failed to connect.", "error"); throw err; })
        .finally(() => { hubStartPromise = null; });
    }
    await hubStartPromise;
    await safeSubscribe();
  }
}

async function  safeSubscribe(){
  const H = signalR.HubConnectionState;
  if (hubConnection?.state !== H.Connected) return;

  // Need an address to follow
  const lc = String(currentMissionAddr || "").toLowerCase();
  if (!lc) return;

  // Also compute checksum; both may be used server-side for group names
  let ck = null;
  try { ck = ethers.utils.getAddress(lc); } catch { /* ignore */ }

  // Subscribe to any missing groups (no unsubscribe here)
  for (const g of [lc, ck]) {
    if (!g || subscribedGroups.has(g)) continue;
    try {
      await hubConnection.invoke("SubscribeMission", g);
      subscribedGroups.add(g);
      dbg("Subscribed group:", g);
    } catch (err) {
      console.error("SubscribeMission failed:", g, err);
    }
  }

  subscribedAddr = lc;
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

function        scheduleDetailRefresh(reset=false){ 
  if (els.missionDetail.style.display === "none" || !currentMissionAddr) {
        return;
      }

  if (reset) { detailBackoffMs = 15000; detailFailures = 0; }
  clearDetailRefresh();
  detailRefreshTimer = setTimeout(async () => {
    if (els.missionDetail.style.display === "none" || !currentMissionAddr) {
        return;
      }
    try {
      const data = await apiMission(currentMissionAddr);
      renderMissionDetail(data);
      detailFailures = 0;
      detailBackoffMs = 15000; // reset on success
    } catch (e) {
      detailFailures++;
      detailBackoffMs = Math.min(detailBackoffMs * 2, 60000); // cap @ 1 min
      if (detailFailures === 1) {
        showAlert("Auto-refresh failed; will retry with backoff. Check your connection.", "warning");
      }
    } finally {
      scheduleDetailRefresh(); // chain next attempt
    }
  }, detailBackoffMs);
}

// #endregion





// #region API wrappers

async function  fetchAndRenderAllMissions(){
  try {
    // 1) Factory call (chain): get the full mission index
    const provider = getReadProvider();
    const factory  = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, provider);
    const [addrs, statuses, names] = await withTimeout(factory.getAllMissions(), 10000);

    // 2) Map and reverse (NEWEST FIRST)
    const rows = addrs.map((a, i) => ({
      mission_address: a,
      status: Number(statuses[i]),
      name: names[i]
    })).reverse();

    // 3) Hydrate with details from your API (address → mission object)
    const details = await Promise.all(
      rows.map(r => apiMission(r.mission_address).catch(() => null))
    );

    const missions = details.filter(Boolean).map(d => {
      const m = d.mission;

      // enrich with fields the All Missions renderer expects
      m.current_players  = (d.enrollments?.length || 0);
      m.min_players      = m.enrollment_min_players;
      m.max_players      = m.enrollment_max_players;
      m.rounds           = m.round_count;
      m.max_rounds       = m.mission_rounds_total;
      m.mission_duration = (m.mission_start && m.mission_end)
                            ? (Number(m.mission_end) - Number(m.mission_start)) : 0;
      m.mission_fee      = m.enrollment_amount_wei;   // show fee in CRO via weiToCro
      return m;
    });

    // if you use the filtered cache path:
    if (typeof __allMissionsCache !== "undefined") {
      __allMissionsCache = missions;
      applyAllMissionFiltersAndRender();
    } else {
      renderAllMissions(missions);
    }

    startJoinableTicker();                            // update any data-start/data-end timers
    hydrateAllMissionsRealtime(els.allMissionsList);  // NEW: live status (concurrency 4)
  } catch (e) {
    console.error(e);
    showAlert("Failed to load All Missions.", "error");
    renderAllMissions([]);                       // show empty state
  }
}

async function  apiJoinable(){
  const r = await fetch("/api/missions/joinable", { cache: "no-store" });
  if (!r.ok) throw new Error("/api/missions/joinable failed");
  return r.json();
}

async function  apiPlayerMissions(addr){
  const r = await fetch(`/api/missions/player/${addr}`, { cache: "no-store" });
  if (!r.ok) throw new Error("/api/missions/player failed");
  return r.json();
}

async function  apiMission(addr){
  const r = await fetch(`/api/missions/mission/${addr}`, { cache: "no-store" });
  if (!r.ok) throw new Error("/api/missions/mission failed");
  return r.json();
}

// #endregion





// #region Reconciliation (stage)

async function  refreshOpenStageFromServer(retries = 3, delay = 1600) {
  const gameMain = document.getElementById('gameMain');
  if (!gameMain || !gameMain.classList.contains('stage-mode')) return;

  // serialize: only one in-flight refresh
  if (stageRefreshBusy) return;
  stageRefreshBusy = true;

  const scheduleRetry = (n) => {
    clearTimeout(stageRefreshTimer);
    if (n > 0) {
      stageRefreshTimer = setTimeout(() => refreshOpenStageFromServer(n - 1, delay), delay);
    }
  };

  try {
    const data = await apiMission(currentMissionAddr);
    const m = enrichMissionFromApi(data);

    // Merge optimism for a short window so API can't regress the UI
    if (Date.now() < (optimisticGuard?.untilMs || 0)) {
      try {
        const apiCro     = BigInt(String(m.cro_current_wei || m.cro_start_wei || "0"));
        const optCro     = BigInt(String(optimisticGuard.croNow || "0"));

        if (optCro     > apiCro)     m.cro_current_wei  = optCro.toString();
      } catch {}
    }

    const newStatus = Number(m.status);
    const curStatus = Number(stageCurrentStatus ?? -1);

    // NO-REGRESS GUARD: ignore stale snapshots from API/indexer (except 3↔4)
    const toggle34 = (newStatus === 3 && curStatus === 4) || (newStatus === 4 && curStatus === 3);
    if (curStatus >= 0 && newStatus < curStatus && !toggle34) {
      scheduleRetry(retries);
      return;
    }

    // Now (safely) rebuild pills for the accepted status
    if (newStatus === 1 || newStatus === 2) {
      await rehydratePillsFromChain(m, "serverRefresh");
    } else {
      buildStageLowerHudForStatus(m);
    }

    //dbg("refreshOpenStageFromServer", { newStatus, stageCurrentStatus, retries });

    if (newStatus !== stageCurrentStatus) {
      setStageStatusImage(statusSlug(m.status));
      await bindRingToMission(m);
      await bindCenterTimerToMission(m);
      renderStageCtaForStatus(m);
      await renderStageEndedPanelIfNeeded(m);
      stageCurrentStatus = newStatus;
      dbg("refreshOpenStageFromServer APPLIED status", { stageCurrentStatus });

      await maybeShowMissionEndPopup(m);

      // one gentle reconcile after DB denorms settle
      scheduleRetry(Math.min(retries, 1));
    } else {
      scheduleRetry(retries);
    }
  } catch (e) {
    dbg("refreshOpenStageFromServer FAILED", e?.message || e);
    scheduleRetry(retries);
  } finally {
    stageRefreshBusy = false;
  }
}

function        smartReconcile(reason = "smart") { // Lightweight reconcile when we suspect a missed push or transport hiccup
  try {
    const gameMain = document.getElementById('gameMain');
    if (!gameMain || !gameMain.classList.contains('stage-mode')) return;
    if (!currentMissionAddr) return;

    // Don’t spam: if a hub push landed in the last 2–3s, skip
    if (Date.now() - (__lastPushTs || 0) < 2000) return;
    if (stageRefreshBusy) return;

    const st = Number(stageCurrentStatus ?? -1);

    // During Enrolling/Arming, pills are chain-truth; do a quick chain hydrate
    if (st === 1 || st === 2) {
      rehydratePillsFromChain(null, `smart:${reason}`).catch(() => {});
    }

    // Always follow with one light API reconcile to catch denorms
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

// #endregion





// #region Stage entry

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

  // Scale image + overlay, then place everything from the vault center
  layoutStage();
  
  // Build lower HUD pills using chain for Enrolling/Arming
  if (Number(mission?.status) === 1 || Number(mission?.status) === 2) {
    await rehydratePillsFromChain(mission, "openStage");
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
}

function        stageTextFill(){
  // Try to reuse the CTA note color; fall back to white
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
  fontSize:   13,                         // Font size
  labelY:     21, // !!!!!!! Label center y. Correct if rectH is changed. labelY = LabelY - rectH diff / 2.
  valueY:     21, // !!!!!!! Value center y. Correct if rectH is changed. valueY = valueY - rectH diff / 2.
  valueX:     130,                        // Value center x
  rx:         12,                         // Radius (?) x
  ry:         12,                         // Radius (?) y
};

// Helper used by pill library (single source)
function        playersAllStatsParts(m){
  const min    = Number(m?.enrollment_min_players ?? 0);
  const joined = Math.max(
    Array.isArray(m?.enrollments) ? m.enrollments.length : 0,
    Number(__lastChainPlayers || 0),
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
  fee:            { label: "Mission Fee",      value: m => (m && m.enrollment_amount_wei != null) ? `${weiToCro(m.enrollment_amount_wei)} CRO` : "—" },
  poolStart:      { label: "Pool (start)",     value: m => (m && m.cro_start_wei    != null)      ? `${weiToCro(m.cro_start_wei)} CRO`    : "—" },
  poolCurrent:    { label: "Pool (current)",   value: m => {
    if (m?.cro_current_wei != null) return `${weiToCro(m.cro_current_wei)} CRO`;
    if (Number(m?.status) === 1 && Array.isArray(m?.enrollments) && m?.cro_start_wei != null && m?.enrollment_amount_wei != null){
      const startWei = BigInt(String(m.cro_start_wei || "0"));
      const feeWei   = BigInt(String(m.enrollment_amount_wei || "0"));
      const joined   = BigInt(m.enrollments.length || 0);
      return `${weiToCro((startWei + feeWei * joined).toString())} CRO`;
    }
    return "—";
  }},
  playersCap:     { label: "Players cap",      value: m => (m?.enrollment_max_players ?? "—") },
  players: { label: "Players",                 value: m => Math.max(
    Array.isArray(m?.enrollments) ? m.enrollments.length : 0,
    Number(__lastChainPlayers || 0),
    Number(optimisticGuard?.players || 0)
  )},
  rounds:         { label: "Rounds",           value: m => Number(m?.mission_rounds_total ?? 0) },
  roundsOff:      { label: "Rounds",           value: m => `${Number(m?.round_count ?? 0)}/${Number(m?.mission_rounds_total ?? 0)}` },
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
  default:  ["poolCurrent","players","roundsOff"],                                                                  // Ended variants
};

// Pills refresher:

async function  rehydratePillsFromChain(missionOverride = null, why = "") {
  try {
    if (!currentMissionAddr) return;

    // throttle to ~1 call per 2.5s
    const now = Date.now();
    if (__pillsHydrateBusy || (now - __pillsHydrateLast) < 2500) return;
    __pillsHydrateBusy = true;

    // 1) Read players from chain (source of truth)
    const ro = getReadProvider();
    const mc = new ethers.Contract(currentMissionAddr, MISSION_ABI, ro);
    const md = await mc.getMissionData();
    const tuple = md?.[0] || md;
    const playersArr = Array.isArray(tuple?.players) ? tuple.players : [];
    const playersCnt = playersArr.length;

    // 2) Get immutable amounts from the API snapshot (or use the passed mission)
    let mSnap = missionOverride;
    if (!mSnap) {
      const data = await apiMission(currentMissionAddr);
      mSnap = enrichMissionFromApi(data);
    }

    // 3) Pool: prefer chain truth, else derive
    const chainNow = tuple?.ethCurrent != null ? String(tuple.ethCurrent) : "";
    const feeWei   = BigInt(String(mSnap.enrollment_amount_wei || "0"));
    const startWei = BigInt(String(mSnap.cro_start_wei        || "0"));
    const derived  = (startWei + feeWei * BigInt(playersCnt)).toString();
    const croNow   = chainNow || derived;

    // NEW: keep a cache of the last chain-truth so UI never regresses
    try {
      if (playersCnt > __lastChainPlayers) __lastChainPlayers = playersCnt;
      if (BigInt(croNow) > BigInt(__lastChainCroWei || "0")) __lastChainCroWei = croNow;
    } catch { /* ignore BigInt issues */ }

    // 4) Paint
    const snapStatus = Number(mSnap?.status ?? -1);
    const curStatus  = Number(stageCurrentStatus ?? -1);
    const toggle34   = (snapStatus === 3 && curStatus === 4) || (snapStatus === 4 && curStatus === 3);

    // Use the higher of (API snapshot, current stage), except allow 3↔4 either way.
    const effectiveStatus = (curStatus >= 0 && snapStatus < curStatus && !toggle34) ? curStatus : snapStatus;

    buildStageLowerHudForStatus({
      ...mSnap,
      status:           effectiveStatus,
      cro_current_wei:  croNow
    });

  } catch (e) {
    console.warn("[pills/rehydrate] failed:", e?.message || e);
  } finally {
    __pillsHydrateLast = Date.now();
    __pillsHydrateBusy = false;
  }
}

// Chain/No-regress merge:

function        applyChainNoRegress(m){
  try {
    const st = Number(m?.status);
    if (st === 1 || st === 2) {
      const apiCro   = BigInt(String(m?.cro_current_wei ?? m?.cro_start_wei ?? "0"));
      const chainCro = BigInt(String(__lastChainCroWei || "0"));
      const merged   = { ...m };
      if (chainCro > apiCro) merged.cro_current_wei = chainCro.toString();
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

  const safe = applyChainNoRegress(mission);
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

      // base before-join count (array if present, or last chain truth)
      const beforeJoined = Math.max(
        Array.isArray(mission?.enrollments) ? mission.enrollments.length : 0,
        Number(__lastChainPlayers || 0)
      );

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
      rehydratePillsFromChain(mission, "joined:post-tx").catch(()=>{});
    } catch (e) {
      console.warn("[CTA/JOIN] optimistic paint failed:", e?.message || e);
    }

    // Let indexer catch up, then reconcile (no await, slight delay)
    setTimeout(() => { refreshOpenStageFromServer(2).catch(()=>{}); }, 1200);

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
      const data = await apiMission(mission.mission_address);
      const m2 = enrichMissionFromApi(data);
      renderStageCtaForStatus(m2);
    } catch {
      renderStageCtaForStatus(mission);
    }
  }

}

async function  handleBankItClick(mission){
  const signer = getSigner?.();
  if (!signer) { showAlert("Connect your wallet first.", "error"); return; }

  try {
    const c = new ethers.Contract(mission.mission_address, MISSION_ABI, signer);
    // No callStatic probe here → immediate wallet popup
    const tx = await c.callRound();
    await tx.wait();

    // Phase 1: immediate feedback
    showAlert("Round called. Waiting for result…", "info");

    // Phase 2: if *you* win, show the amount as soon as the push arrives
    try {
      const me = (await signer.getAddress()).toLowerCase();
      const missionLc = String(mission.mission_address || "").toLowerCase();
      const res = await waitForMyRoundWin(me, missionLc, 25000);
      const cro = weiToCro(String(res.amountWei));
      showAlert(`Congratulations!<br/>You banked ${cro} CRO in round ${res.round}!`, "success");
    } catch {
      // no win / no push within the window → keep the generic info alert
    }

    // keep the regular reconcile
    await refreshOpenStageFromServer(2);

  } catch (err) {
    if (err?.code === 4001 || err?.code === "ACTION_REJECTED") {
      showAlert("Banking canceled.", "warning");
    } else {
      const custom = missionCustomErrorMessage(err);
      const msg = custom || `Bank it failed: ${decodeError(err)}`;
      showAlert(msg, custom ? "warning" : "error");

      // Fallback: if the revert is Cooldown, flip UI to Paused now.
      // This covers the case where the button still showed BANK IT.
      if (stageCurrentStatus === 3 && (isCooldownError(err) || /Cooldown/i.test(custom || ""))) {
        await flipStageToPausedOptimistic(mission);
      }
    }
  }
}

async function  refreshStageCtaIfOpen(){
  const gameMain = document.getElementById('gameMain');
  if (!gameMain || !gameMain.classList.contains('stage-mode')) return;
  if (!currentMissionAddr) {
        return;
      }
  try {
    const data = await apiMission(currentMissionAddr);
    const m = enrichMissionFromApi(data);
    renderStageCtaForStatus(m);
  } catch {}
}

// CTA router:

async function  renderStageCtaForStatus (mission) {
  const host = document.getElementById("stageCtaGroup");
  if (!host) return;

  const st = uiStatusFor(mission);

  // Ignore stale snapshots that would repaint an older CTA (prevents JOIN flicker)
  if (typeof stageCurrentStatus === "number") {
    const toggle34 = (st === 3 && stageCurrentStatus === 4) || (st === 4 && stageCurrentStatus === 3);
    if (st < stageCurrentStatus && !toggle34) {
      console.debug("[CTA] stale snapshot ignored", { st, stageCurrentStatus });
      return;
    }
  }

  host.innerHTML = "";

  if (st === 1) return renderCtaEnrolling (host, mission);  // JOIN
  if (st === 2) return renderCtaArming    (host, mission);  // ENROLLMENT / CLOSED (disabled)
  if (st === 3) return renderCtaActive    (host, mission);  // Active → BANK IT!
  if (st === 4) return renderCtaPaused    (host, mission);  // Paused → Cooldown
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
  let hasSpots = true, canEnrollSoft = true;

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
    // (C) Chain probe → source of truth
    const ro = getReadProvider();
    const mc = new ethers.Contract(mission.mission_address, MISSION_ABI, ro);
    const md = await mc.getMissionData();
    const tuple = md?.[0] || md;
    const players = (tuple?.players || []).map(a => String(a).toLowerCase());

    const alreadyByChain = !!(me && players.includes(me));
    already = already || alreadyByChain;

    const maxP = Number(mission.enrollment_max_players ?? 0);
    hasSpots = maxP ? (players.length < maxP) : true;

    if (me && FACTORY_ADDRESS) {
      const fac = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, ro);
      canEnrollSoft = await fac.canEnroll(me);
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
  else if (!canEnrollSoft)          { disabled = true; note = "Weekly/monthly limit reached"; }
  if (ctaBusy)                      { disabled = true; note = "Joining…"; }

  console.debug("[CTA/JOIN] gating", {
    me, inWin, alreadyByCache, alreadyByApi, already, hasSpots, canEnrollSoft
  });


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
  const me = (walletAddress || "").toLowerCase();

  // joined?
  const joined = !!(me && (mission?.enrollments || []).some(e => {
    const p = String(e?.player_address || e?.address || e?.player || "").toLowerCase();
    return p === me;
  }));

  // already won any round?
  const alreadyWon = !!(me && (mission?.rounds || []).some(r => {
    const w = String(r?.winner_address || "").toLowerCase();
    return w === me;
  }));

  let blockReason = "";
  if (!walletAddress)        blockReason = "Connect your wallet to bank";
  else if (!joined)          blockReason = "You did not join this mission";
  else if (alreadyWon)       blockReason = "View only. You already won a round";

  if (blockReason) {
    // Centered message in the CTA area, no button rendered
    const msg = document.createElementNS(SVG_NS, "text");
    msg.setAttribute("x", String(xCenter));
    msg.setAttribute("y", String(y + Math.round(btnH / 2) + 7));
    msg.setAttribute("text-anchor", "middle");
    msg.setAttribute("class", "cta-note");
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
  line.setAttribute("y", String(y + btnH + 20));
  line.setAttribute("text-anchor", "middle");
  line.setAttribute("class", "cta-note");

  const tLabel = document.createElementNS(SVG_NS, "tspan");
  tLabel.textContent = "Ends in ";

  const tVal = document.createElementNS(SVG_NS, "tspan");
  const endTs = Number(mission?.mission_end || 0);
  if (endTs > 0) {
    tVal.setAttribute("data-countdown", String(endTs));
    tVal.textContent = formatCountdown(endTs);
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
  bank.setAttribute("y", String(y + btnH + 36));
  bank.setAttribute("text-anchor", "middle");
  bank.setAttribute("class", "cta-note");
  bank.setAttribute("data-bank-now", "1");

  // decide label based on viewer eligibility
  const eligible  = !!walletAddress && joined && !alreadyWon;

  const lastTs = getLastBankTs(mission, mission?.rounds);
  const weiNow = computeBankNowWei(mission, lastTs);
  const croNow = weiToCro(String(weiNow), 2);
  const label  = eligible ? "Bank this round to claim:" : "Current round prize pool:";
  bank.textContent = `${label} ${croNow} CRO`;
  host.appendChild(bank);
}

function        renderCtaPaused         (host, mission)   {
  const { xCenter, topY } = CTA_LAYOUT;
  const { bg, btnW, btnH } = CTA_ACTIVE; // reuse the wide button art
  const x = xCenter - Math.round(btnW / 2);
  const y = topY;

  const g = document.createElementNS(SVG_NS, "g");
  g.setAttribute("class", "cta-btn cta-disabled");
  g.setAttribute("transform", `translate(${x},${y})`);

  // background only (no "BANK IT!" text)
  g.appendChild(svgImage(bg, null, null, btnW, btnH));

  // Centered dynamic cooldown label inside the button
  const cool = document.createElementNS(SVG_NS, "text");
  cool.setAttribute("x", String(Math.round(btnW / 2)));
  cool.setAttribute("y", String(Math.round(btnH / 2) + 5));
  cool.setAttribute("text-anchor", "middle");
  cool.setAttribute("font-family", "system-ui, Segoe UI, Arial");
  cool.setAttribute("font-size", "14");
  cool.setAttribute("fill", "#ffffff");

  const tLabel = document.createElementNS(SVG_NS, "tspan");
  tLabel.textContent = "Cooldown: ";

  const tVal = document.createElementNS(SVG_NS, "tspan");
  const info = cooldownInfo(mission);
  if (info.pauseEnd) {
    tVal.setAttribute("data-cooldown-end", String(info.pauseEnd));
    tVal.textContent = formatMMSS(info.secsLeft);
  } else {
    tVal.textContent = "00:00";
  }
  tVal.style.fontWeight = "700";
  cool.appendChild(tLabel);
  cool.appendChild(tVal);
  g.appendChild(cool);

  host.appendChild(g);

  // Under-button: "Ends in …"
  const line = document.createElementNS(SVG_NS, "text");
  line.setAttribute("x", String(xCenter));
  line.setAttribute("y", String(y + btnH + 20));
  line.setAttribute("text-anchor", "middle");
  line.setAttribute("class", "cta-note");

  const eLabel = document.createElementNS(SVG_NS, "tspan");
  eLabel.textContent = "Ends in ";

  const eVal = document.createElementNS(SVG_NS, "tspan");
  const endTs = Number(mission?.mission_end || 0);
  if (endTs > 0) {
    eVal.setAttribute("data-countdown", String(endTs));
    eVal.textContent = formatCountdown(endTs);
  } else {
    eVal.textContent = "—";
  }
  eVal.style.fontWeight = "700";
  line.appendChild(eLabel);
  line.appendChild(eVal);
  host.appendChild(line);

  // New line: live “Accumulating: … CRO” (ticks via [data-bank-now])
  const bank = document.createElementNS(SVG_NS, "text");
  bank.setAttribute("x", String(xCenter));
  bank.setAttribute("y", String(y + btnH + 36));
  bank.setAttribute("text-anchor", "middle");
  bank.setAttribute("class", "cta-note");
  bank.setAttribute("data-bank-now", "1");
  const lastTs = getLastBankTs(mission, mission?.rounds);
  const weiNow = computeBankNowWei(mission, lastTs);
  const croNow = weiToCro(String(weiNow), 2);
  bank.textContent = `Accumulating: ${croNow} CRO`;
  host.appendChild(bank);
}

async function  flipStageToPausedOptimistic(mission){ // Flip the open stage to Paused immediately; reconcile from API afterwards
  const gameMain = document.getElementById('gameMain');
  if (!gameMain || !gameMain.classList.contains('stage-mode')) return;

  const m2 = { ...mission, status: 4 };     // Paused now (UI only)

  setStageStatusImage(statusSlug(4));
  buildStageLowerHudForStatus(m2);
  await bindRingToMission(m2);
  await bindCenterTimerToMission(m2);
  renderStageCtaForStatus(m2);
  await renderStageEndedPanelIfNeeded(m2);
  stageCurrentStatus = 4;

  // Pick up real pause_timestamp on the next API tick
  refreshOpenStageFromServer(2);
}

// Round notice:

function        renderRoundBankedNotice (roundNo, winner, amountWei) {
  const host = document.getElementById("stageNoticeGroup");
  if (!host) return;

  // Clear any previous notice
  host.innerHTML = "";

  // Capsule positioned between the prize line and the top HUD pills
  // (just above the first pill row)
  const { xCenter } = CTA_LAYOUT;
  const { yFirst, rectH, gapY }  = HUD;
  const W = 360;
  const H = 26;
  const x = xCenter - Math.round(W/2);
  const y = Math.max(0, yFirst + (2 * (rectH + gapY)) - H - 12);     // <-- new position (above 2 lines of pills)

  const g = document.createElementNS(SVG_NS, "g");
  g.setAttribute("fill", stageTextFill());

  // Background capsule
  const bg = document.createElementNS(SVG_NS, "rect");
  bg.setAttribute("x", String(x));
  bg.setAttribute("y", String(y));
  bg.setAttribute("width",  String(W));
  bg.setAttribute("height", String(H));
  bg.setAttribute("rx", "12");
  bg.setAttribute("ry", "12");
  bg.setAttribute("fill", "rgba(6,29,45,85)");
  bg.setAttribute("stroke", "rgba(72,221,255,35)");
  g.appendChild(bg);

  // Main text: "Round N banked by 0x1234...1234 for ## CRO!"
  const cro = weiToCro(String(amountWei || "0"), 2);
  const txt = document.createElementNS(SVG_NS, "text");
  txt.setAttribute("x", String(x + 12));
  txt.setAttribute("y", String(y + 17));
  txt.setAttribute("class", "notice-text");
  const short = winner ? shorten(winner) : "";
  txt.textContent = `Round ${roundNo} banked by ${short} for ${cro} CRO!`;
  g.appendChild(txt);

  // Clickable icons (copy address + open on Cronoscan)
  if (winner){
    // Copy icon (inline vector)
    const icoCopy = document.createElementNS(SVG_NS, "g");
    icoCopy.setAttribute("transform", `translate(${x + W - 56},${y + 6})`);
    icoCopy.setAttribute("role", "button");
    icoCopy.setAttribute("tabindex", "0");
    icoCopy.style.cursor = "pointer";
    // two overlapped rectangles → "copy" glyph
    const r1 = document.createElementNS(SVG_NS, "rect");
    r1.setAttribute("x", "3"); r1.setAttribute("y", "3");
    r1.setAttribute("width", "10"); r1.setAttribute("height", "10");
    r1.setAttribute("fill", "none"); r1.setAttribute("stroke", "#bfefff"); r1.setAttribute("stroke-width", "1.2");
    const r2 = document.createElementNS(SVG_NS, "rect");
    r2.setAttribute("x", "7"); r2.setAttribute("y", "7");
    r2.setAttribute("width", "10"); r2.setAttribute("height", "10");
    r2.setAttribute("fill", "none"); r2.setAttribute("stroke", "#bfefff"); r2.setAttribute("stroke-width", "1.2");
    icoCopy.appendChild(r1); icoCopy.appendChild(r2);
    icoCopy.addEventListener("click", async () => { try { await navigator.clipboard.writeText(winner); } catch {} });
    g.appendChild(icoCopy);

    // External link icon (re-use addrLinkIcon from core.js)
    const icoLink = svgImage(addrLinkIcon(winner), x + W - 36, y + 5, 14, 14);
    icoLink.setAttribute("role", "button");
    icoLink.setAttribute("tabindex", "0");
    icoLink.style.cursor = "pointer";
    icoLink.addEventListener("click", () => {
      try { window.open(`https://cronoscan.com/address/${winner}`, "_blank", "noopener"); } catch {}
    });
    g.appendChild(icoLink);
  }

  // Close ×
  const tClose = document.createElementNS(SVG_NS, "text");
  tClose.setAttribute("x", String(x + W - 16));
  tClose.setAttribute("y", String(y + 17));
  tClose.setAttribute("class", "notice-text");
  tClose.setAttribute("role", "button");
  tClose.setAttribute("tabindex", "0");
  tClose.style.cursor = "pointer";
  tClose.textContent = "×";
  tClose.addEventListener("click", () => { host.innerHTML = ""; });
  g.appendChild(tClose);

  host.appendChild(g);
}

async function  maybeShowMissionEndPopup(mission){
  try{
    if (!mission) return;
    const st = Number(mission.status ?? -1);
    if (st < 5 || st === 7) return; // only Success/Partly Success

    const addr = String(mission.mission_address || currentMissionAddr || "").toLowerCase();
    if (!addr || __endPopupShown.has(addr)) return;
    __endPopupShown.add(addr);

    // Ensure we have enrollments + rounds for role detection
    let enrollments = Array.isArray(mission.enrollments) ? mission.enrollments : null;
    let rounds      = Array.isArray(mission.rounds)      ? mission.rounds      : null;
    if (!enrollments || !rounds){
      try{
        const data = await apiMission(addr);
        enrollments = data?.enrollments || [];
        rounds      = data?.rounds      || [];
      } catch {}
    } else {
      // Keep as-is
    }

    const me = (walletAddress || "").toLowerCase();
    const joined = !!(me && (enrollments || []).some(e => {
      const a = String(e.player_address || e.address || e.player || "").toLowerCase();
      return a === me;
    }));

    const isAllBanked = (st === 6);
    const reasonText  = isAllBanked ? "All rounds were banked." : "The mission timer ran out.";

    // If the viewer won any round, show their latest win
    let myWins = [];
    if (me && Array.isArray(rounds)){
      myWins = rounds.filter(r => String(r.winner_address || "").toLowerCase() === me)
                     .sort((a,b) => Number(a.round_no || a.round || 0) - Number(b.round_no || b.round || 0));
    }

    if (myWins.length){
      const last = myWins[myWins.length - 1];
      const roundNo = Number(last.round_no || last.round || 0);
      const cro = weiToCro(String(last.payout_wei || last.amountWei || "0"), 2);
      // Winners: celebratory copy + brief reason line
      showAlert(`🎉 Congratulations!<br/>You won round ${roundNo} and claimed <b>${cro} CRO</b>.<br/><small>${reasonText}</small>`, "success");
      return;
    }

    if (joined){
      // Non-winning player: supportive copy, reason-tailored
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

// Ended panel:

async function  renderStageEndedPanelIfNeeded(mission){
  const host = document.getElementById("stageEndedGroup");
  if (!host) return;
  host.innerHTML = "";

  const st = Number(mission?.status ?? -1);
  if (st < 5) return; // only ended bucket

  // NEW — Failed (status 7): render reason + refund line and exit
  if (st === 7) {
    const x = 500, y = 585;
    const g = document.createElementNS("http://www.w3.org/2000/svg","g");

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
    const data = await apiMission(mission.mission_address);
    enrollments = data?.enrollments || [];
    rounds      = data?.rounds       || [];
  } catch { /* keep empty */ }

  const winners = topWinners(enrollments, rounds, 3);

  const x = 500, y = 585, lineH = 16;
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
    sub.textContent = (st === 6) ? "All rounds were banked." : "Mission time ended.";
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
    await openMission(mission.mission_address);
    els.missionDetail.classList.add("overlay");
    els.missionDetail.style.display = "block";
    lockScroll();
  });

  g.appendChild(link);
  host.appendChild(g);
}

// #endregion





// #region Lists & detail screens

function        renderAllMissions       (missions = []) {
  const ul = document.getElementById("allMissionsList");
  const empty = document.getElementById("allMissionsEmpty");
  if (!ul || !empty) return;
  ul.classList.add("card-grid");
  ul.innerHTML = "";
  if (!missions.length) {
    empty.style.display = "";
    return;
  }
  empty.style.display = "none";

  // newest first (factory returns oldest→newest; we also reversed at fetch time, but
  // keep this guard so we never regress)
  const list = missions;

  for (const m of list) {
    const li = document.createElement("li");
    li.className = "mission-card";

    // live status (same concurrent approach you used already)
    const stNum   = Number(m.status ?? 0);
    const stText  = statusText(stNum);
    const stClass = statusColorClass(stNum);

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
      timeKey1 = "";
    }

    // Fallbacks keep it resilient if you ever reuse this with raw mission objects
    const maxPlayers = Number(m.max_players ?? m.enrollment_max_players ?? 0);
    const minPlayers = Number(m.min_players ?? m.enrollment_min_players ?? 0);
    const curPlayers = Number(m.current_players ?? 0);
    const rounds     = Number(m.rounds ?? m.round_count ?? 0);
    const maxRounds  = Number(m.max_rounds ?? m.mission_rounds_total ?? rounds ?? 0);

    const duration   = Number(m.mission_duration ?? ((m.mission_start && m.mission_end) ? (m.mission_end - m.mission_start) : 0));
    const feeWei     = (m.mission_fee ?? m.enrollment_amount_wei ?? 0);
    const feeCro     = weiToCro(String(feeWei));

    const playersPct = maxPlayers > 0 ? Math.min(100, Math.round((curPlayers / maxPlayers) * 100)) : 0;

    li.className = "mission-card";
    li.dataset.addr = (m.mission_address || "").toLowerCase();

    li.innerHTML = `
      <div class="mission-head d-flex justify-content-between align-items-center">
        <div class="mission-title">
          <span class="title-text">${m.name || m.mission_address}</span>
        </div>
        <span class="status-pill ${stClass}">${stText}</span>
      </div>

      <div class="mini-row">
        <div class="label">Duration:</div>
        <div class="value">${formatDurationShort(duration)}</div>
        <div class="ms-auto fw-bold">Rounds ${rounds}/${maxRounds}</div>
      </div>

      <div class="mini-row">
        <div class="label">Mission Fee:</div>
        <div class="value">${feeCro} CRO</div>
      </div>

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

      <div class="players-line">
        <div class="label me-2">Players</div>
        <div class="players-count">
          <span class="current" data-current="${curPlayers}" data-min="${minPlayers}">${curPlayers}</span>/<span class="max">${maxPlayers}</span>
        </div>
      </div>
      <div class="progress-slim"><i style="--w:${playersPct}%"></i></div>
    `;

    li.querySelector('.mission-title .title-text').title = m.name || m.mission_address;

    // clicking opens detail
    li.addEventListener("click", () => openMission(m.mission_address));

    ul.appendChild(li);
  }

  // re-use the global ticker to keep countdowns + players color live
  startJoinableTicker();
}

function        renderJoinable          (items){
  els.joinableList.innerHTML = "";
  els.joinableEmpty.style.display = items?.length ? "none" : "";
  els.joinableList?.classList.add('card-grid');

  const list = (items || []).slice().reverse(); // newest on top
  for (const m of list){
    const title   = m.name || copyableAddr(m.mission_address);
    const rounds  = m.mission_rounds_total ?? m.missionRounds ?? "";
    const startTs = m.mission_start ?? 0;
    const enrollEndTs = m.enrollment_end ?? 0;

    const feeCro  = weiToCro(m.enrollment_amount_wei);
    const joined  = Array.isArray(m.enrollments) ? m.enrollments.length : 0;
    const max     = Number(m.enrollment_max_players ?? 0);
    const minReq  = Number(m.enrollment_min_players ?? 0);
    const pct     = max > 0 ? Math.min(100, Math.round((joined / max) * 100)) : 0;

    // duration label placeholder (we’ll refine the mapping later)
    const durationTxt = (m.mission_start && m.mission_end)
      ? formatDurationShort(m.mission_end - m.mission_start)
      : "—";

    const li = document.createElement("li");
    li.className = "mission-card mb-3";
    li.setAttribute("role", "button");
    li.tabIndex = 0;

    li.innerHTML = `
      <div class="mission-head">
        <div class="mission-title">
          <span class="title-text">${title}</span>
        </div>
        <div class="mission-rounds">${rounds ? `${rounds} Rounds` : ""}</div>
      </div>

      <div class="mini-row">
        <span class="label">Join until:</span>
        <span class="value">${enrollEndTs ? formatLocalDateTime(enrollEndTs) : "—"}</span>
      </div>

      <div class="mini-row">
        <span class="label">Starts in:</span>
        <span class="value" data-start="${startTs}">${startTs ? formatCountdown(startTs) : "—"}</span>
      </div>

      <div class="fact-row">
         <div>
          <div class="label">Duration:</div>
          <div class="value">${durationTxt}</div>
        </div>       
        <div>
          <div class="label">Mission Fee:</div>
          <div class="value">${feeCro} CRO</div>
        </div>

      </div>

      <div class="players-line">
        <div class="label">Players</div>
        <div class="players-count">
          <span class="current" data-current="${joined}" data-min="${minReq}">${joined}</span>/<span>${max || "—"}</span>
        </div>
      </div>
      <div class="progress-slim"><i style="--w:${pct}%;"></i></div>
    `;

    li.querySelector('.mission-title .title-text').title = title;

    const open = () => openMission(m.mission_address);
    li.addEventListener("click", open);
    li.addEventListener("keypress", e => { if (e.key === "Enter") open(); });

    els.joinableList.appendChild(li);
  }
}

function        renderMyMissions        (items){
  els.myMissionsList.innerHTML = "";
  els.myMissionsEmpty.style.display = items?.length ? "none" : "";
  els.myMissionsList?.classList.add('card-grid');

  const list = (items || []).slice().reverse(); // newest on top
  for (const m of list){
    const title   = m.name || copyableAddr(m.mission_address);
    const rounds  = `${m.round_count}/${m.mission_rounds_total}`;

    // decide which live label to show
    const isUpcoming = (m.status === 0 || m.status === 1 || m.status === 2);   // Pending/Enrolling/Arming
    const isActive   = (m.status === 3);
    const liveLabel  = isUpcoming ? "Starts in:" : (isActive ? "Ends in:" : "");
    const liveAttr   = isUpcoming ? `data-start="${m.mission_start || 0}"` 
                                  : (isActive ? `data-end="${m.mission_end || 0}"` : "");
    const liveValue  =
      isUpcoming ? (m.mission_start ? formatCountdown(m.mission_start) : "—") :
      isActive   ? (m.mission_end   ? formatCountdown(m.mission_end)   : "—") :
                   ""; // ended states: no live countdown line

    const li = document.createElement("li");
    li.className = "mission-card mb-3";
    li.setAttribute("role", "button");
    li.tabIndex = 0;

    li.innerHTML = `
      <div class="mission-head">
        <div class="mission-title">
          <span class="title-text">${title}</span>
        </div>
        <div class="mission-rounds">${rounds} Rounds</div>
      </div>

      <div class="mini-row">
        <span class="label">Status:</span>
        <span class="value">
          <span class="status-pill status-${m.status}">${statusText(m.status)}</span>
          ${m.status === 7 && m.failure_reason
            ? `<span class="small text-muted ms-2">${m.failure_reason}</span>`
            : ""}
        </span>
      </div>

      ${liveLabel ? `
      <div class="mini-row">
        <span class="label">${liveLabel}</span>
        <span class="value" ${liveAttr}>${liveValue}</span>
      </div>` : ""}

      <div class="mini-row">
        <span class="label">Start:</span>
        <span class="value">${m.mission_start ? formatLocalDateTime(m.mission_start) : "—"}</span>
      </div>

      <div class="mini-row">
        <span class="label">End:</span>
        <span class="value">${m.mission_end ? formatLocalDateTime(m.mission_end) : "—"}</span>
      </div>

      ${m.refunded ? `
        <div class="mini-row">
          <span class="label">Refund:</span>
          <span class="value text-warning">
            Refunded ${txLinkIcon(m.refund_tx_hash)}
          </span>
        </div>` : ""}
    `;

    li.querySelector('.mission-title .title-text').title = title;

    const open = () => openMission(m.mission_address);
    li.addEventListener("click", (e) => {
      // don’t open the card if the click is on a link or a button inside the card
      if (e.target.closest('a,button')) return;
      open();
    });

    li.addEventListener("keypress", (e) => {
      if (e.key !== "Enter") return;
      if (e.target.closest('a,button')) return;
      open();
    });

    els.myMissionsList.appendChild(li);
  }
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
  const showEnrollStartCountdown = st <= 0; // <= Pending
  const showEnrollEndCountdown   = st <= 1; // <= Enrolling
  const showMissionStartCountdown= st <= 2; // <= Arming
  const showMissionEndCountdown  = st <= 4; // <= Paused

  const statusCls = statusColorClass(mission.status);

  // No-regress: prefer chain-truth for Enrolling/Arming
  const safe = applyChainNoRegress(mission);
  const listJoined = Array.isArray(enrollments) ? enrollments.length : 0;
  const joinedPlayers = Math.max(
    listJoined,
    Number(__lastChainPlayers || 0),
    Number(optimisticGuard?.players || 0)
  );

  const minP = Number(safe?.enrollment_min_players ?? mission.enrollment_min_players ?? 0);
  const maxP = Number(safe?.enrollment_max_players ?? mission.enrollment_max_players ?? 0);
  const joinedCls   = (joinedPlayers >= minP) ? "text-success" : "text-error";

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
          <div class="value"><span class="${statusCls}">${statusText(mission.status)}</span></div>

          <div class="label">Rounds played</div>
          <div class="value">${mission.round_count}/${mission.mission_rounds_total}</div>

          <div class="label">Mission Fee</div>
          <div class="value">${weiToCro(mission.enrollment_amount_wei)} CRO</div>

          <div class="label">Prize pool</div>
          <div class="value">${weiToCro(mission.cro_start_wei)} CRO</div>

          <div class="label">Enrollment Start</div>
          <div class="value">
            ${showEnrollStartCountdown
              ? `<span id="enrollStartCountdown">${formatCountdown(mission.enrollment_start)}</span>`
              : `${formatLocalDateTime(mission.enrollment_start)}`}
          </div>

          <div class="label">Enrollment End</div>
          <div class="value">
            ${showEnrollEndCountdown
              ? `<span id="enrollEndCountdown">${formatCountdown(mission.enrollment_end)}</span>`
              : `${formatLocalDateTime(mission.enrollment_end)}`}
          </div>

          <div class="label">Mission Start</div>
          <div class="value">
            ${showMissionStartCountdown
              ? `<span id="missionStartCountdown">${formatCountdown(mission.mission_start)}</span>`
              : `${formatLocalDateTime(mission.mission_start)}`}
          </div>

          <div class="label">Mission End</div>
          <div class="value">
            ${showMissionEndCountdown
              ? `<span id="missionEndCountdown">${formatCountdown(mission.mission_end)}</span>`
              : `${formatLocalDateTime(mission.mission_end)}`}
          </div>

        <div class="label">Updated</div>
        <div class="value">
          <span id="updatedAtStamp"
                data-updated="${mission.updated_at}">
            ${formatLocalDateTime(mission.updated_at)}
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

    // UPDATED AGE → mark red + exclamation when > 60s
    const stamp = document.getElementById("updatedAtStamp");
    const icon  = document.getElementById("updatedAtIcon");
    if (stamp) {
      const t = Number(stamp.dataset.updated || 0);
      const age = Math.floor(Date.now()/1000) - t;
      const stale = age > 60;
      stamp.classList.toggle("text-error", stale);
      if (icon) icon.style.display = stale ? "inline-block" : "none";

      // One-time popup when it first turns stale
      if (stale && !staleWarningShown) {
        showAlert("Mission data hasn’t updated for over 1 minute. Try reloading or check your connection.", "warning");
        staleWarningShown = true;
      }
      if (!stale){
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
    const wonLabel = won ? `${weiToCro(String(wwei))} CRO` : "";

    const li = document.createElement("li");
    li.className = "view-card mb-2";
    li.innerHTML = `
      <div class="d-flex justify-content-between align-items-center">
        <div class="text-bold">
          ${copyableAddr(addr)} ${addrLinkIcon(addr)}
          <div class="small">
            ${won ? `<i class="fa-solid fa-trophy ms-2 text-warning" title="Winner"></i>
                    <span class="small ms-1">${wonLabel}</span>` : ""}
          </div>
        </div>
        <div class="text-end small">
          ${e.enrolled_at ? formatLocalDateTime(e.enrolled_at) : "—"}
          ${e.refunded ? `<div class="text-warning">Refunded ${txLinkIcon(e.refund_tx_hash)}</div>` : ""}
        </div>
      </div>
    `;
    els.enrollmentsList.appendChild(li);
  }

  document.getElementById("btnTrigger")?.addEventListener("click", () => triggerRoundCurrentMission(mission));

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
  await fetchAndRenderAllMissions(); 
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

function        startJoinableTicker(){
  stopJoinableTicker();

  const tick = () => {
    const nowSec = Math.floor(Date.now() / 1000);
    let needsRefresh = false;

    // update "Starts in"
    document.querySelectorAll('[data-start]').forEach(el => {
      const t = Number(el.getAttribute('data-start') || 0);
      if (t > 0) {
        const left = Math.max(0, t - nowSec);
        el.textContent = formatCountdown(t);
        if (left <= 0) needsRefresh = true;
      } else {
        el.textContent = '—';
      }
    });

    // update "Ends in" for cards that provide it (e.g., My Missions active ones)
    document.querySelectorAll('[data-end]').forEach(el => {
      const t = Number(el.getAttribute('data-end') || 0);
      el.textContent = t > 0 ? formatCountdown(t) : '—';
    });

    // color the current players count by min threshold
    document.querySelectorAll('.players-count .current[data-min][data-current]').forEach(el => {
      const cur = Number(el.getAttribute('data-current') || 0);
      const min = Number(el.getAttribute('data-min') || 0);
      el.classList.remove('ok', 'low');
      el.classList.add(cur >= min ? 'ok' : 'low');
      // keep the text updated in case API changed
      el.textContent = String(cur);
    });

    // if any mission just reached start, refresh the list once
    if (needsRefresh) {
      // small debounce: stop ticker, refetch, re-render, restart
      stopJoinableTicker();
      apiJoinable()
        .then(items => { renderJoinable(items); startJoinableTicker(); })
        .catch(() => { /* ignore transient */ });
    }
  };

  tick(); // immediate paint
  joinableTimer = setInterval(tick, 1000);
}

function        stopJoinableTicker(){
  if (joinableTimer) { clearInterval(joinableTimer); joinableTimer = null; }
}

// Realtime status hydrator for cards:

async function  hydrateAllMissionsRealtime(listEl){
  if (!listEl) return;
  const cards = [...listEl.querySelectorAll("li.mission-card")];
  if (!cards.length) return;

  // Use batch provider so parallel calls coalesce; also slow down a bit
  const provider    = new ethers.providers.JsonRpcBatchProvider(READ_ONLY_RPC);
  const N           = 1;    // was 4 — keep low to avoid 403 bursts
  const SPACING_MS  = 150;  // small gap per request
  const sleep       = ms => new Promise(r => setTimeout(r, ms));
  let i = 0;

  async function worker(){
    while (i < cards.length){
      const idx  = i++;
      const li   = cards[idx];
      const addr = li.dataset.addr;
      if (!addr) continue;

      try {
        const c  = new ethers.Contract(addr, MISSION_ABI, provider);
        const rt = Number(await c.getRealtimeStatus());
        const pill = li.querySelector(".status-pill");
        if (pill){
          pill.textContent = statusText(rt);
          pill.className   = `status-pill ${statusColorClass(rt)}`;
        }
      } catch (err) {
        console.warn("realtime status failed:", addr, err?.message || err);
      }

      // jitter to avoid synchronized bursts (helps with WAF/rate limit)
      await sleep(SPACING_MS + Math.floor(Math.random() * 100));
    }
  }

  await Promise.all([...Array(Math.min(N, cards.length))].map(() => worker()));

}

// #endregion





// #region Interactions

async function  openMission(addr){
  try {
    currentMissionAddr = addr.toLowerCase();
    await subscribeToMission(currentMissionAddr);
    const data = await apiMission(currentMissionAddr);
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
      const data = await apiMission(addr);
      const m = enrichMissionFromApi(data);
      await showGameStage(m);
      await renderStageEndedPanelIfNeeded(m);
      stageReturnTo = null;
      return;
    } catch { /* fall through */ }
  }

  stageReturnTo = null;
  currentMissionAddr = null;
  showOnlySection(lastListShownId);
}

async function  subscribeToMission(addr){
  if (!hubConnection) return;

  const H = signalR.HubConnectionState;
  const targetLc = String(addr||"").toLowerCase();
  let targetCk = null;
  try { targetCk = ethers.utils.getAddress(targetLc); } catch { /* keep null */ }

  // If the hub isn't connected yet, remember what we want to subscribe to
  if (hubConnection.state !== H.Connected) {
    subscribedAddr = targetLc;
    subscribedGroups = new Set([targetLc, targetCk].filter(Boolean));
    dbg("hub not connected; will subscribe later:", Array.from(subscribedGroups));
    return;
  }

  // IDEMPOTENCE: already on the right groups? do nothing.
  const targetSet = new Set([targetLc, targetCk].filter(Boolean));
  const same = (subscribedGroups.size === targetSet.size) &&
               [...targetSet].every(g => subscribedGroups.has(g));
  if (same) {
    dbg("Already subscribed to target groups:", Array.from(subscribedGroups));
    subscribedAddr = targetLc;
    return;
  }

  // Unsubscribe all previous groups
  for (const g of Array.from(subscribedGroups)) {
    try { await hubConnection.invoke("UnsubscribeMission", g); dbg("Unsubscribed group:", g); }
    catch (e) { dbg("Unsubscribe failed:", g, e?.message||e); }
  }
  subscribedGroups.clear();

  // Subscribe to both forms that may be used server-side
  for (const g of [targetLc, targetCk]) {
    if (!g) continue;
    try {
      await hubConnection.invoke("SubscribeMission", g);
      subscribedGroups.add(g);
      dbg("Subscribed group:", g);
    } catch (e) {
      console.error("SubscribeMission failed:", g, e);
    }
  }

  subscribedAddr = targetLc;
}

async function  triggerRoundCurrentMission(mission){
  try {
    const signer = getSigner();
    if (!signer) { showAlert("Connect your wallet first.", "error"); return; }

    const c = new ethers.Contract(mission.mission_address, MISSION_ABI, signer);

    // Optional probe to avoid a revert dialog:
    try { await c.callStatic.callRound(); } catch { 
      showAlert("Round not available yet (cooldown or status).", "warning");
      return;
    }

    setBtnLoading(document.getElementById("btnTrigger"), true, "Triggering…", true);
    const tx = await c.callRound();
    await tx.wait();

    showAlert("Round triggered!", "success");
    const data = await apiMission(mission.mission_address);
    renderMissionDetail(data);
  } catch (err) {
    console.error(err);
    showAlert(`Trigger failed: ${decodeError(err)}`, "error");
  } finally {
    setBtnLoading(document.getElementById("btnTrigger"), false, "Trigger Round", false);
  }
}

// #endregion





// #region Init & boot

function        updateConnectText(){
  const span = document.getElementById("connectBtnText");
  if (span) span.textContent = walletAddress ? shorten(walletAddress) : "Connect Wallet";
}

async function  init(){
  // 0) show list immediately
  showOnlySection("allMissionsSection");

  // 1) wire buttons BEFORE any awaited network work
  els.refreshJoinableBtn?.addEventListener("click", async () => {
    try {
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
      await withTimeout(fetchAndRenderAllMissions(), 12000);
    } catch (e) {
      console.warn("Refresh All Missions timed out/failed:", e);
      showAlert("Loading All Missions timed out. Please try again.", "warning");
    } finally {
      allLoadBusy = false;
    }
  });

  // 2) other existing listeners (unchanged)
  els.closeMissionBtn?.addEventListener("click", closeMission);
  els.reloadMissionBtn?.addEventListener("click", async () => {
    if (!currentMissionAddr) return;
    try {
      const data = await apiMission(currentMissionAddr);
      renderMissionDetail(data);
    } catch (e) {
      showAlert("Reload failed. Please check your connection.", "error");
    }
  });

  // 3) kick off first load with a timeout so init never “hangs”
  try {
    await withTimeout(fetchAndRenderAllMissions(), 12000);
  } catch (e) {
    console.warn("Initial All Missions load timed out/failed:", e);
    showAlert("Loading All Missions timed out. Tap Refresh to retry.", "warning");
  }

  // 4) Manual reload button
  els.reloadMissionBtn?.addEventListener("click", async () => {
    if (!currentMissionAddr) {
        return;
      }
    try {
      // icon-only button → keep original innerHTML, no text label
      const data = await apiMission(currentMissionAddr);
      renderMissionDetail(data);
    } catch (e) {
      showAlert("Reload failed. Please check your connection.", "error");
    } 
  });

  buildAllFiltersUI();
  
  // event-based (if we add wallet events in walletConnect.js, see 1D)
  window.addEventListener("wallet:connected",               fetchAndRenderAllMissions);
  window.addEventListener("wallet:changed",                 fetchAndRenderAllMissions);
  window.addEventListener("wallet:disconnected", () =>      renderAllMissions([]));

  window.addEventListener("wallet:connected",               refreshStageCtaIfOpen);
  window.addEventListener("wallet:changed",                 refreshStageCtaIfOpen);
  window.addEventListener("wallet:disconnected",            refreshStageCtaIfOpen);

 // Fallback if those custom events aren’t emitted:
  if (window.ethereum) {
    window.ethereum.on("accountsChanged", () => setTimeout( refreshStageCtaIfOpen, 0));
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

if (document.readyState === "loading"){
  document.addEventListener("DOMContentLoaded", () => {
    connectWallet();
    startHub();
    init();
  }, { once:true });
} else {
    connectWallet();
    startHub();
    init();
}

// #endregion
