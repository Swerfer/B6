/**********************************************************************
 game.js – home page bootstrap, re-uses core.js & walletConnect.js
**********************************************************************/
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
} from "./core.js";

// ─────────────────────────────────────────────
// Mission custom errors (from Mission.sol)
// ─────────────────────────────────────────────
const MISSION_ERROR_ABI = [
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

const __missionErrIface = new ethers.utils.Interface(MISSION_ERROR_ABI);

// map error names → friendly text (some include decoded args)
function missionErrorToText(name, args = []) {
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

// extract revert data from common provider error shapes
function __revertHex(err) {
  return (
    err?.error?.data ||
    err?.data?.originalError?.data ||
    err?.data ||
    null
  );
}

// decode a Mission custom error if present; otherwise return null
function missionCustomErrorMessage(err) {
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

// #region Variables
const connectBtn          = document.getElementById("connectWalletBtn");
const sectionBoxes        = document.querySelectorAll(".section-box");

let   lastListShownId       = "joinableSection";
let   hubConnection         = null;
let   hubStartPromise       = null;   // prevent concurrent starts
let   currentMissionAddr    = null;
let   subscribedAddr        = null;
let   detailRefreshTimer    = null;
let   detailBackoffMs       = 15000;
let   detailFailures        = 0;
let   staleWarningShown     = false;
let   ringTimer             = null;
let   stageTicker           = null;
let   stageCurrentStatus    = null;
let   ctaBusy               = false;
// #endregion

connectBtn.addEventListener("click", () => {
  if (walletAddress){
    showConfirm("Disconnect current wallet?", disconnectWallet);
  } else {
    connectWallet(); 
  }
});

// ------------------- helpers ----------------------

function showOnlySection(sectionId) {
  sectionBoxes.forEach(sec => {
    sec.style.display = (sec.id === sectionId) ? "" : "none";
  });

  document.getElementById('gameMain').classList.toggle('stage-mode', sectionId === 'gameStage');

  // Remember the last list view the user opened (now includes All Missions)
  if (["joinableSection","myMissionsSection","allMissionsSection"].includes(sectionId)) {
    lastListShownId = sectionId;
  }
}

async function cleanupMissionDetail(){
  stopCountdown();
  stopStageTimer();
  unbindRing();                   // NEW: stop ring timer & leave last drawn state
  clearDetailRefresh();
  staleWarningShown = false;
  // Optional: unsubscribe from hub for the last mission to cut noise
  if (hubConnection && subscribedAddr){
    try { await hubConnection.invoke("UnsubscribeMission", subscribedAddr); } catch {}
    subscribedAddr = null;
  }
  currentMissionAddr = null;
}

function statusSlug(s){
  switch (Number(s)) {
    case 0: return "pending";
    case 1: return "enrolling";
    case 2: return "arming";
    case 3: return "active";
    case 4: return "paused";
    case 5: return "partly-success";
    case 6: return "success";
    case 7: return "failed";
    default: return "ended";
  }
}

// MissionCreated → creation timestamp (for Pending phaseStart)
const __mcIface = new ethers.utils.Interface([
  "event MissionCreated(address indexed mission,string name,uint8 missionType,uint256 enrollmentStart,uint256 enrollmentEnd,uint8 minPlayers,uint8 maxPlayers,uint256 enrollmentAmount,uint256 missionStart,uint256 missionEnd,uint8 missionRounds)"
]);

// MissionCreated → creation timestamp (for Pending phaseStart)
// Paged over ≤2000 blocks to satisfy Cronos RPC
const __missionCreatedCache = new Map();

async function getMissionCreationTs(mission){
  if (mission.missionCreated) return mission.missionCreated;
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

/* Load + size the status word image and center it under the title */
function setStageStatusImage(slug){
  if (!stageStatusImgSvg || !slug) return;
  stageStatusImgSvg.setAttribute("href", `assets/images/statuses/${slug}.png`);
}

// ---- center timer (short form) ----
function stopStageTimer(){ if (stageTicker){ clearInterval(stageTicker); stageTicker = null; } }

function formatStageShort(leftSec){
  const s = Math.max(0, Math.floor(leftSec));
  if (s > 36*3600) return Math.round(s/86400) + "D";   // > 36h → days
  if (s > 90*60)   return Math.round(s/3600)  + "H";   // > 90m → hours
  if (s > 90)      return Math.round(s/60)    + "M";   // > 90s  → minutes
  return s + "S";                                      // ≤ 90s → seconds
}

// Unit classifier used by the center text and the ring "reset" windows
function stageUnitFor(leftSec){
  const s = Math.max(0, Math.floor(leftSec));
  if (s > 36*3600) return "d";
  if (s > 90*60)   return "h";
  if (s > 90)      return "m";
  return "s";
}

function ringWindowForUnit(unit, phaseStart, endTs){
  if (!endTs) return [0,0];

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
}

function startStageTimer(endTs, phaseStartTs = 0, missionObj){
  stopStageTimer();
  const node = document.getElementById("vaultTimerText");
  if (!node || !endTs) { if (node) node.textContent = ""; return; }

  let zeroFired = false;
  let currentUnit = null;

  const paint = () => {
    const now  = Math.floor(Date.now()/1000);
    const left = Math.max(0, endTs - now);

    // center text only formats; does not drive windows
    node.textContent = formatStageShort(left);

    // NEW: tick lower-HUD countdown pills (full d hh:mm:ss)
    document.querySelectorAll('#stageLowerHud [data-countdown], #stageCtaGroup [data-countdown]').forEach(el => {
      const ts = Number(el.getAttribute('data-countdown') || 0);
      el.textContent = ts ? formatCountdown(ts) : "—";
    });

    // Rebind ring when the unit changes...
    const unit = stageUnitFor(left);
    if (unit !== currentUnit) {
      const [S, E] = ringWindowForUnit(unit, phaseStartTs, endTs);
      if (S && E && E > S) bindRingToWindow(S, E); else setRingProgress(0);
      currentUnit = unit;
    }

    if (left <= 0) {
      stopStageTimer();
      if (!zeroFired) {
        zeroFired = true;

        // (1) Instant front-end flip using immutable times
        if (missionObj) {
          const next = statusByClock(missionObj, now);
          if (typeof next === "number" && next !== Number(missionObj.status)) {
            const m2 = { ...missionObj, status: next };
            setStageStatusImage(statusSlug(m2.status));
            buildStageLowerHudForStatus(m2);
            bindRingToMission(m2);
            bindCenterTimerToMission(m2);
            renderStageCtaForStatus(m2);
            stageCurrentStatus = next;
          }
        }

        // (2) One server refresh (sync with backend / ended subtype)
        refreshOpenStageFromServer(1);
      }
    }
  };

  paint();
  stageTicker = setInterval(paint, 1000);
}

// Choose the deadline shown in the vault center per status
function nextDeadlineFor(m){
  if (!m) return 0;
  const st = Number(m.status);
  if (st === 0) return Number(m.enrollment_start || m.mission_start || 0); // Pending
  if (st === 1) return Number(m.enrollment_end   || 0);                    // Enrolling
  if (st === 2) return Number(m.mission_start    || 0);                    // Arming
  if (st === 3 || st === 4) return Number(m.mission_end || 0);             // Active / Paused
  return 0; // Ended variants – no countdown in center
}

// NEW — compute the status purely from immutable times (front-end flip)
function statusByClock(m, now = Math.floor(Date.now()/1000)) {
  const es = Number(m.enrollment_start || 0);
  const ee = Number(m.enrollment_end   || 0);
  const ms = Number(m.mission_start    || 0);
  const me = Number(m.mission_end      || 0);

  const cur = Number(m.enrolled_players ?? 0);
  const min = Number(m.enrollment_min_players ?? 0);

  if (now < es) return 0;                                              // Pending
  if (now < ee) return 1;                                              // Enrolling
  if (now < ms) return (cur >= min ? 2 : 7);                           // Arming OR Failed if min not met
  if (now < me) return (m.pause_timestamp ? 4 : 3);                    // Paused vs Active
  return (m.status >= 5 ? m.status : 6);                               // Ended bucket (keep subtype if present)
}

async function bindCenterTimerToMission(mission){
  const endTs = nextDeadlineFor(mission);
  const st    = Number(mission?.status);

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
  startStageTimer(endTs, startTs, mission);
}

/* ---------- Ring overlay: bind to time window ---------- */

function unbindRing(){
  if (ringTimer){ clearInterval(ringTimer); ringTimer = null; }
}

function setRingProgress(pct){
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

function bindRingToWindow(startSec, endSec){
  unbindRing();
  const cover = document.getElementById("ringCover");
  if (!cover) return;
  if (!startSec || !endSec || endSec <= startSec) return;

  const tick = () => {
    const now = Math.floor(Date.now()/1000);
    let pct; // revealed percent 0..100
    if (now <= startSec) pct = 0;                         // not started → all covered
    else if (now >= endSec) pct = 100;                    // finished → fully revealed
    else pct = ((now - startSec) / (endSec - startSec)) * 100;
    setRingProgress(pct);
  };

  tick();                             // draw immediately
  ringTimer = setInterval(tick, 1000);
}

/* Map mission.status to the correct time window */
async function bindRingToMission(m){
  const st = Number(m?.status ?? -1);
  let S = 0, E = 0;

  if (st === 0) {                         // Pending: MissionCreated → enrollment_start
    E = Number(m.enrollment_start || 0);

    // MissionCreated event timestamp (cached). Fallback: updated_at.
    S = m?.mission_address ? await getMissionCreationTs(m) : 0;
    console.log(E + " - " + S);
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
}

/* ----- page scroll lock (for gameplay stage) ----- */
function lockScroll(){
  const y = window.scrollY || document.documentElement.scrollTop || 0;
  document.documentElement.classList.add("scroll-lock");
  document.body.classList.add("scroll-lock");
  document.body.style.setProperty("--lock-top", `-${y}px`);
}

function unlockScroll(){
  const top = parseInt(getComputedStyle(document.body).getPropertyValue("--lock-top")) || 0;
  document.documentElement.classList.remove("scroll-lock");
  document.body.classList.remove("scroll-lock");
  document.body.style.removeProperty("--lock-top");
  // restore previous scroll position
  window.scrollTo(0, -top);
}

/* ---------- SignalR: connect to /hub/game ---------- */

function stateName(s){
  const H = signalR.HubConnectionState;
  return s === H.Connected ? "Connected"
    : s === H.Disconnected ? "Disconnected"
    : s === H.Connecting ? "Connecting"
    : s === H.Reconnecting ? "Reconnecting"
    : String(s);
}

async function startHub() { // SignalR HUB
  if (!window.signalR) { showAlert("SignalR client script not found.", "error"); return; }

  if (!hubConnection) {
    hubConnection = new signalR.HubConnectionBuilder()
      .withUrl("/api/hub/game")
      .withAutomaticReconnect()
      .build();

      hubConnection.on("ServerPing", (msg) => {
        showAlert(`Server ping:<br>${msg}`, "info");
      });

      hubConnection.on("RoundResult", (addr, round, winner, amountWei) => {
        showAlert(`Round ${round} – ${winner}<br/>Amount (wei): ${amountWei}<br/>Mission: ${addr}`, "success");
        if (currentMissionAddr && addr?.toLowerCase() === currentMissionAddr) {
          apiMission(currentMissionAddr).then(renderMissionDetail).catch(()=>{});
        }
      });

    hubConnection.on("StatusChanged", async (addr, newStatus) => {
      if (currentMissionAddr && addr?.toLowerCase() === currentMissionAddr) {
        const gameMain = document.getElementById('gameMain');
        try {
          const data = await apiMission(currentMissionAddr);
          const m = data.mission || data;
          if (gameMain && gameMain.classList.contains('stage-mode')) {
            if (Number(m.status) === stageCurrentStatus) return;
            setStageStatusImage(statusSlug(m.status));
            buildStageLowerHudForStatus(m);
            await bindRingToMission(m);
            await bindCenterTimerToMission(m);
            renderStageCtaForStatus(m);
            stageCurrentStatus = Number(m.status);
          } else {
            renderMissionDetail(data);
            showAlert(`Mission status changed:<br>${addr}<br>Status: ${newStatus}`, "warning");
          }
        } catch {}
      }
    });

      // on reconnect, re-subscribe to the open mission (if any)
      hubConnection.onreconnected(async () => {
        if (currentMissionAddr) await subscribeToMission(currentMissionAddr);
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

async function safeSubscribe(){
  const H = signalR.HubConnectionState;
  if (hubConnection?.state !== H.Connected) return;
  if (!currentMissionAddr) return; // nothing open yet
  try {
    await hubConnection.invoke("SubscribeMission", currentMissionAddr);
    subscribedAddr = currentMissionAddr;
  } catch (err) {
    console.error("SubscribeMission failed:", err);
  }
}

function clearDetailRefresh(){          
  if (detailRefreshTimer) { clearTimeout(detailRefreshTimer); detailRefreshTimer = null; }
}

function scheduleDetailRefresh(reset=false){ 
  if (els.missionDetail.style.display === "none" || !currentMissionAddr) return;

  if (reset) { detailBackoffMs = 15000; detailFailures = 0; }
  clearDetailRefresh();
  detailRefreshTimer = setTimeout(async () => {
    if (els.missionDetail.style.display === "none" || !currentMissionAddr) return;
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

/* ---------- API wrappers (ALL under /api) ---------- */
async function fetchAndRenderAllMissions(){
  try {
    // 1) Factory call (chain): get the full mission index
    const provider = getReadProvider();
    const factory  = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, provider);
    const [addrs, statuses, names] = await factory.getAllMissions();

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

    renderAllMissions(missions);

    startJoinableTicker();                       // update any data-start/data-end timers
    hydrateAllMissionsRealtime(els.allMissionsList);  // NEW: live status (concurrency 4)
  } catch (e) {
    console.error(e);
    showAlert("Failed to load All Missions.", "error");
    renderAllMissions([]);                       // show empty state
  }
}

async function apiJoinable(){
  const r = await fetch("/api/missions/joinable", { cache: "no-store" });
  if (!r.ok) throw new Error("/api/missions/joinable failed");
  return r.json();
}

async function apiPlayerMissions(addr){
  const r = await fetch(`/api/missions/player/${addr}`, { cache: "no-store" });
  if (!r.ok) throw new Error("/api/missions/player failed");
  return r.json();
}

async function apiMission(addr){
  const r = await fetch(`/api/missions/mission/${addr}`, { cache: "no-store" });
  if (!r.ok) throw new Error("/api/missions/mission failed");
  return r.json();
}

async function refreshOpenStageFromServer(retries = 3) {
  const gameMain = document.getElementById('gameMain');
  if (!gameMain || !gameMain.classList.contains('stage-mode')) return;
  if (!currentMissionAddr) return;

  try {
    const data = await apiMission(currentMissionAddr);
    const m = data.mission || data;

    // If status changed, or just to be safe, rebuild the stage pieces
    const newStatus = Number(m.status);
    if (newStatus !== stageCurrentStatus) {
      setStageStatusImage(statusSlug(m.status));                         // image under title
      buildStageLowerHudForStatus(m);                             // pills for this status
      await bindRingToMission(m);                                       // ring window for this status
      await bindCenterTimerToMission(m);                                // center countdown for this status
      renderStageCtaForStatus(m);
      stageCurrentStatus = newStatus;
    } else if (retries > 0) {
      // backend might flip a second later; try a couple of times
      setTimeout(() => refreshOpenStageFromServer(retries - 1), 1500);
    }
  } catch { /* ignore transient errors */ }
}

/* ---------- DOM refs ---------- */
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

// #region dom elements
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

async function showGameStage(mission){
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

  // Scale image + overlay, then place everything from the vault center
  layoutStage();
  
  // Build lower HUD pills for THIS status & fill values
  buildStageLowerHudForStatus(mission);

  await bindRingToMission(mission);

  // Center timer bound to this mission's next deadline
  await bindCenterTimerToMission(mission);

  // CTA (status 1 → JOIN MISSION)
  renderStageCtaForStatus(mission);
}

// Use "Active" HUD when simulating during Enrolling
function hudStatusFor(mission){
  const st = Number(mission?.status ?? -1);
  return st;
}

window.addEventListener("resize", layoutStage);

const IMG_W = 2000, IMG_H = 2000;
const visibleRectangle  = { x:566, y:420, w:914, h:1238 }; // visibleRectangle was the rectangle from the phone header to footer space and phone 
                                                           // screen width on the vault bg image. This rectangle is always visible on every screen

function layoutStage(){
  if (!stage || !stageViewport || !stageImg) return;
  const availW = stageViewport.clientWidth;
  const availH = stageViewport.clientHeight;

  const scale = Math.min(availH / visibleRectangle .h, availW / visibleRectangle .w);

  const w = Math.round(IMG_W * scale);
  const h = Math.round(IMG_H * scale);
  stageImg.style.width  = w + "px";
  stageImg.style.height = h + "px";

  if (ringOverlay){
    ringOverlay.style.width  = w + "px";
    ringOverlay.style.height = h + "px";
  }

}

// --- Lower HUD (pills) builder --------------------------------------------
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

// Single source of truth for pill behaviors/labels
const PILL_LIBRARY = {
  missionType:    { label: "Mission Type",     value: m => missionTypeName[Number(m?.mission_type ?? 0)] },
  opensAt:        { label: "Opens At",         value: m => m?.enrollment_start ? formatLocalDateTime(m.enrollment_start) : "—" },
  missionStartAt: { label: "Start At",         value: m => m?.mission_start    ? formatLocalDateTime(m.mission_start)    : "—" },
  duration:       { label: "Duration",         value: m => (m?.mission_start && m?.mission_end)
                                                 ? formatDurationShort(Number(m.mission_end) - Number(m.mission_start)) : "—" },
  fee:            { label: "Mission Fee",      value: m => (m && m.enrollment_amount_wei != null) ? `${weiToCro(m.enrollment_amount_wei)} CRO` : "—" },
  poolStart:      { label: "Pool (start)",     value: m => (m && m.cro_start_wei    != null)      ? `${weiToCro(m.cro_start_wei)} CRO`    : "—" },
  poolCurrent:    { label: "Pool (current)",   value: m => (m && m.cro_current_wei  != null)      ? `${weiToCro(m.cro_current_wei)} CRO`  : "—" },
  playersCap:     { label: "Players cap",      value: m => (m?.enrollment_max_players ?? "—") },
  players:        { label: "Players",          value: m => Number(m?.enrolled_players ?? 0) },      
  rounds:         { label: "Rounds",           value: m => Number(m?.mission_rounds_total ?? 0) },
  roundsOff:      { label: "RoundsOff",        value: m => `${Number(m?.round_count ?? 0)}/${Number(m?.mission_rounds_total ?? 0)}` },
  playersAllStats:{ label: "Players",          value: m => `${m?.enrollment_min_players ?? "—"}/${m?.enrolled_players ?? 0}/${m?.enrollment_max_players ?? "—"}`},
  closesIn:       { label: "Closes In",        countdown: m => Number(m?.enrollment_end  || 0) },
  startsIn:       { label: "Starts In",        countdown: m => Number(m?.mission_start   || 0) },
  endsIn:         { label: "Ends In",          countdown: m => Number(m?.mission_end     || 0) },
};

// Which pills to show per status (0..7) — only using fields that exist in your payloads :contentReference[oaicite:0]{index=0}
const PILL_SETS = {
  0:        ["opensAt","missionStartAt","duration","fee","poolStart","playersCap","rounds"],                        // Pending
  1:        ["missionType","rounds","fee","poolCurrent","playersAllStats","duration","closesIn","missionStartAt"],  // Enrolling
  2:        ["poolStart","players","rounds","startsIn","duration"],                                                 // Arming
  3:        ["poolCurrent","players","roundsOff","endsIn"],                                                         // Active
  4:        ["poolCurrent","players","roundsOff","endsIn"],                                                         // Paused
  default:  ["poolCurrent","players","roundsOff"],                                                                  // Ended variants
};

// Build (and fill) the pills for the current mission/status
function buildStageLowerHudForStatus(mission){
  const host = document.getElementById("stageLowerHud");
  if (!host) return;
  while (host.firstChild) host.removeChild(host.firstChild);

  const keys = PILL_SETS[hudStatusFor(mission)] ?? PILL_SETS.default;
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
      const ts = def.countdown(mission);
      if (ts > 0) {
        val.setAttribute("data-countdown", String(ts));
        val.textContent = formatCountdown(ts);
      } else {
        val.textContent = "—";
      }
    } else if (p.key === "playersAllStats") {
      // min / joined / max with colored "joined"
      const min = Number(mission?.enrollment_min_players ?? 0);
      const cur = Number(mission?.enrolled_players ?? 0);
      const maxRaw = mission?.enrollment_max_players;
      const max = (maxRaw == null) ? "—" : String(maxRaw);

      const tMin = document.createElementNS(SVG_NS, "tspan");
      tMin.textContent = ` (Min ${min}`;

      const tCur = document.createElementNS(SVG_NS, "tspan");
      tCur.textContent = String(cur);
      // color by threshold (uses your CSS vars from core.css)
      const color = cur >= min ? "var(--success)" : "var(--error)";
      tCur.setAttribute("style", `fill:${color}`);   // use CSS var
      // tCur.setAttribute("fill", color);           // (extra-safe duplicate if you want)

      const tMax = document.createElementNS(SVG_NS, "tspan");
      tMax.textContent = `/Max ${max})`;

      // build the value as tspans
      val.textContent = "";
      val.appendChild(tCur);
      val.appendChild(tMin);
      val.appendChild(tMax);
    } else {
      val.textContent = def.value ? def.value(mission) : "—";
    }

    g.appendChild(val);
    host.appendChild(g);
  }
}

function svgImage(href, x, y, w, h){
  const el = document.createElementNS(SVG_NS, "image");
  if (x != null) el.setAttribute("x", String(x));
  if (y != null) el.setAttribute("y", String(y));
  el.setAttribute("width",  String(w));
  el.setAttribute("height", String(h));
  el.setAttribute("href", href);
  el.setAttributeNS("http://www.w3.org/1999/xlink", "href", href);
  return el;
}

// Use "Active" (3) when simulating during Enrolling (1)
function uiStatusFor(mission){
  const st = Number(mission?.status ?? -1);
  return st;
}

// ── CTA assets & layout (single source of truth) ─────────────────────────
const CTA_LAYOUT = {
  xCenter: 500,       // viewBox center
  topY:    555,       // same top Y for all CTAs (matches Join)
};

// JOIN (status 1)
const CTA_JOIN = {
  bg:    "assets/images/buttons/Button extra wide.png",
  text:  "assets/images/buttons/Join Mission text.png",
  btnW:  213, btnH: 50,
  txtW:  158, txtH: 23,
  txtDy: -1,           // vertical nudge (shadow compensation)
};

// ARMING (status 2) — disabled 2-line button: ENROLLMENT / CLOSED
const CTA_ARMING = {
  bg:     "assets/images/buttons/Button 2 lines wide.png",// ← replace with 2-line bg if you have one
  line1:  "assets/images/buttons/Enrollment text.png",    // ← set to your uploaded filename
  line2:  "assets/images/buttons/Closed text.png",        // ← set to your uploaded filename
  btnW:   213, btnH: 50,
  l1W:    120, l1H: 12,     // ← update to your actual text image sizes
  l2W:    90,  l2H: 12,     // ← update to your actual text image sizes
  gap:    4,                // vertical gap between lines
};

// ACTIVE (status 3) — BANK IT!
const CTA_ACTIVE = {
  bg:   "assets/images/buttons/Button extra wide.png",  // provided
  text: "assets/images/buttons/Bank it text.png", // provided
  btnW: 213, btnH: 50,     // consistent with other CTAs; PNG scales down nicely
  txtW: 160, txtH: 30,     // tweak if you want tighter fit
  txtDy: -1,
};

async function  renderStageCtaForStatus(mission){
  const host = document.getElementById("stageCtaGroup");
  if (!host) return;
  host.innerHTML = "";

  const st = uiStatusFor(mission);
  if (st === 1) return renderCtaEnrolling(host, mission); // JOIN
  if (st === 2) return renderCtaArming(host, mission);    // ENROLLMENT / CLOSED (disabled)
  if (st === 3) return renderCtaActive(host, mission);    // Active → BANK IT!

  // no CTA for other statuses (4,5,6,7) for now
}

async function  renderCtaEnrolling(host, mission){
  const { xCenter, topY } = CTA_LAYOUT;
  const { bg, text, btnW, btnH, txtW, txtH, txtDy } = CTA_JOIN;

  // center under vault
  const x = xCenter - Math.round(btnW / 2);
  const y = topY;

  // Gating (wallet, window, already, spots, optional canEnroll)
  const now   = Math.floor(Date.now()/1000);
  const inWin = now < Number(mission.enrollment_end || 0);

  const me = (walletAddress || "").toLowerCase();
  let already = false, hasSpots = true, canEnrollSoft = true;

  try {
    const ro = getReadProvider();
    const mc = new ethers.Contract(mission.mission_address, MISSION_ABI, ro);
    const md = await mc.getMissionData();
    const tuple = md?.[0] || md;
    const players = (tuple?.players || []).map(a => String(a).toLowerCase());

    already = !!(me && players.includes(me));

    const maxP = Number(mission.enrollment_max_players ?? 0);
    hasSpots = maxP ? (players.length < maxP) : true;

    if (me && FACTORY_ADDRESS) {
      const fac = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, ro);
      canEnrollSoft = await fac.canEnroll(me);
    }
  } catch { /* leave permissive defaults */ }

  let disabled = false, note = "";
  if (!walletAddress) { disabled = true; note = "Connect your wallet to join"; }
  else if (!inWin)    { disabled = true; note = "Enrollment closed"; }
  else if (already)   { disabled = true; note = "You already joined"; }
  else if (!hasSpots) { disabled = true; note = "No spots left"; }
  else if (!canEnrollSoft) { disabled = true; note = "Weekly/monthly limit reached"; }
  if (ctaBusy) { disabled = true; note = "Joining…"; }

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

function        renderCtaArming(host, mission){
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
  tLabel.textContent = "Starts in ";

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

function        renderCtaActive(host, mission){
  const { xCenter, topY } = CTA_LAYOUT;
  const { bg, text, btnW, btnH, txtW, txtH, txtDy } = CTA_ACTIVE;

  const x = xCenter - Math.round(btnW / 2);
  const y = topY;

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

  // click → wallet popup (you can cancel)
  g.addEventListener("click", () => handleBankItClick(mission));
  g.addEventListener("mousedown", () => g.setAttribute("transform", `translate(${x},${y+1})`));
  const reset = () => g.setAttribute("transform", `translate(${x},${y})`);
  g.addEventListener("mouseup", reset);
  g.addEventListener("mouseleave", reset);
  g.addEventListener("keydown", (e) => {
    if (e.key === "Enter" || e.key === " ") { e.preventDefault(); handleBankItClick(mission); }
  });

  host.appendChild(g);

  // One-line: "Ends in 00:12:34"
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
}

async function  handleEnrollClick(mission){
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

    showAlert("You joined the mission!", "success");

    // Refresh everything on the open stage
    await refreshOpenStageFromServer(2);
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
      renderStageCtaForStatus(data?.mission || mission);
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
    // No callStatic probe here → we want the wallet popup immediately
    const tx = await c.callRound();
    await tx.wait();

    showAlert("Round called. Good luck!", "success");
    await refreshOpenStageFromServer(2);
  } catch (err) {
    if (err?.code === 4001 || err?.code === "ACTION_REJECTED") {
      showAlert("Banking canceled.", "warning");
    } else {
      const custom = missionCustomErrorMessage(err);
      const msg = custom || `Bank it failed: ${decodeError(err)}`;
      showAlert(msg, custom ? "warning" : "error");
    }
  }
}

async function  refreshStageCtaIfOpen(){
  const gameMain = document.getElementById('gameMain');
  if (!gameMain || !gameMain.classList.contains('stage-mode')) return;
  if (!currentMissionAddr) return;
  try {
    const data = await apiMission(currentMissionAddr);
    const m = data.mission || data;
    renderStageCtaForStatus(m);
  } catch {}
}

// click handlers
btnAllMissions?.addEventListener("click", async () => {
  await cleanupMissionDetail();
  showOnlySection("allMissionsSection");
  await fetchAndRenderAllMissions(); 
});

btnJoinable?.addEventListener("click", async () => {
  await cleanupMissionDetail();
  showOnlySection("joinableSection");
});

btnMyMissions?.addEventListener("click", async () => {
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

let countdownTimer = null;
function stopCountdown(){ if (countdownTimer) { clearInterval(countdownTimer); countdownTimer = null; }}

// Live ticker for Joinable list
let joinableTimer = null;
function stopJoinableTicker(){
  if (joinableTimer) { clearInterval(joinableTimer); joinableTimer = null; }
}

function startJoinableTicker(){
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

/* ---------- renderers ---------- */
function renderAllMissions(missions = []) {
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
  const list = missions.slice().reverse();

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
          <i class="fa-regular fa-calendar me-2 text-info"></i>
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

    // clicking opens detail
    li.addEventListener("click", async () => {
      await cleanupMissionDetail();
      currentMissionAddr = (m.mission_address || "").toLowerCase();
      showOnlySection("missionDetailSection");
      const data = await apiMission(currentMissionAddr).catch(() => null);
      if (data?.mission) renderMissionDetail(data);
    });

    ul.appendChild(li);
  }

  // re-use the global ticker to keep countdowns + players color live
  startJoinableTicker();
}

function renderJoinable(items){
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
    const joined  = Number(m.enrolled_players ?? 0);
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
          <i class="fa-regular fa-calendar text-cyan"></i>
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

    const open = () => openMission(m.mission_address);
    li.addEventListener("click", open);
    li.addEventListener("keypress", e => { if (e.key === "Enter") open(); });

    els.joinableList.appendChild(li);
  }
}

function renderMyMissions(items){
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
          <i class="fa-regular fa-calendar text-cyan"></i>
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

async function renderMissionDetail({ mission, enrollments, rounds }){
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
      await cleanupMissionDetail();                         // stop timers, unsub, etc.
      currentMissionAddr = String(mission.mission_address).toLowerCase();
      await startHub();                                     // ensures connection + safeSubscribe()
      await subscribeToMission(currentMissionAddr);         // idempotent if already subscribed
      lockScroll();
      await showGameStage(mission);                               
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
  const joinedPlayers = Array.isArray(enrollments) ? enrollments.length : 0;
  const minP = Number(mission.enrollment_min_players ?? 0);
  const maxP = Number(mission.enrollment_max_players ?? 0);
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

async function hydrateAllMissionsRealtime(listEl){
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

/* ---------- interactions ---------- */
async function openMission(addr){
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

async function closeMission(){
  els.missionDetail.classList.remove("overlay");
  els.missionDetail.style.display = "none";
  unlockScroll();
  await cleanupMissionDetail();
  stopCountdown();
  clearDetailRefresh();
  currentMissionAddr = null;
  // restore the previous list (joinable by default until we add footer nav)
  showOnlySection(lastListShownId);
}

async function subscribeToMission(addr){
  if (!hubConnection) return;
  const H = signalR.HubConnectionState;

  // If not connected yet, remember target; onreconnected will handle it
  if (hubConnection.state !== H.Connected) {
    subscribedAddr = addr;
    return;
  }

  if (subscribedAddr && subscribedAddr !== addr) {
    try { await hubConnection.invoke("UnsubscribeMission", subscribedAddr); } catch {}
  }
  try {
    await hubConnection.invoke("SubscribeMission", addr);
    subscribedAddr = addr;
  } catch {}
}

async function triggerRoundCurrentMission(mission){
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

/* ---------- page init ---------- */
async function init(){
  // 0) show only the Joinable list on first load
  showOnlySection("joinableSection");
  // 1) initial joins
  try {
    const joinable = await apiJoinable();
    renderJoinable(joinable);
    startJoinableTicker();
  } catch(e){ console.error(e); }

  // 2) refresh button
  els.refreshJoinableBtn?.addEventListener("click", async () => {
    try {
      const joinable = await apiJoinable();
      renderJoinable(joinable);
      startJoinableTicker();
    } catch(e){ console.error(e); }
  });

  els.refreshAllBtn?.addEventListener("click", async () => {
    // stay on the same section; just refetch
    await fetchAndRenderAllMissions();
  });

  // 3) close mission detail
  els.closeMissionBtn?.addEventListener("click", closeMission);

  // 4) Manual reload button
  els.reloadMissionBtn?.addEventListener("click", async () => {
    if (!currentMissionAddr) return;
    try {
      // icon-only button → keep original innerHTML, no text label
      const data = await apiMission(currentMissionAddr);
      renderMissionDetail(data);
    } catch (e) {
      showAlert("Reload failed. Please check your connection.", "error");
    } 
  });

  // 5) auto-load My Missions when wallet connects (event-based if available, else fallback poll)
  const loadMy = async () => {
    if (!walletAddress) return;
    try {
      const mine = await apiPlayerMissions(walletAddress.toLowerCase());
      renderMyMissions(mine);
    } catch(e){ console.error(e); }
  };

  // event-based (if we add wallet events in walletConnect.js, see 1D)
  window.addEventListener("wallet:connected", loadMy);
  window.addEventListener("wallet:changed", loadMy);
  window.addEventListener("wallet:disconnected", () => renderMyMissions([]));

  // fallback: small poll in case events are not wired yet
  let tried = 0;
  const t = setInterval(() => {
    if (walletAddress){ loadMy(); clearInterval(t); }
    if (++tried > 20) clearInterval(t);
  }, 500);

  window.addEventListener("wallet:connected",    refreshStageCtaIfOpen);
  window.addEventListener("wallet:changed",      refreshStageCtaIfOpen);
  window.addEventListener("wallet:disconnected", refreshStageCtaIfOpen);

 // Fallback if those custom events aren’t emitted:
  if (window.ethereum) {
    window.ethereum.on("accountsChanged", () => setTimeout(refreshStageCtaIfOpen, 0));
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
