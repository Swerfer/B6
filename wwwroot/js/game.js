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
  shorten, 
  copyableAddr, 
  formatLocalDateTime, 
  formatCountdown,
  statusText,
  formatDurationShort,
  statusColorClass,
  extLinkIcon,
  txLinkIcon,
  addrLinkIcon,
  missionTypeName,
  weiToCro, 
  MISSION_ABI, 
  setBtnLoading, 
  decodeError, 
} from "./core.js";

/* ---------- button ---------- */
const connectBtn          = document.getElementById("connectWalletBtn");
const sectionBoxes        = document.querySelectorAll(".section-box");

let lastListShownId       = "joinableSection";
let hubConnection         = null;
let hubStartPromise       = null;   // prevent concurrent starts
let currentMissionAddr    = null;
let subscribedAddr        = null;

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
  if (sectionId === "joinableSection" || sectionId === "myMissionsSection") {
    lastListShownId = sectionId;
  }
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

async function startHub() {
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

      hubConnection.on("StatusChanged", (addr, newStatus) => {
        showAlert(`Mission status changed:<br>${addr}<br>Status: ${newStatus}`, "warning");
        if (currentMissionAddr && addr?.toLowerCase() === currentMissionAddr) {
          apiMission(currentMissionAddr).then(renderMissionDetail).catch(()=>{});
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

/* ---------- API wrappers (ALL under /api) ---------- */
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

/* ---------- DOM refs ---------- */
const els = {
  joinableList:        document.getElementById("joinableList"),
  joinableEmpty:       document.getElementById("joinableEmpty"),
  refreshJoinableBtn:  document.getElementById("refreshJoinableBtn"),

  myMissionsList:      document.getElementById("myMissionsList"),
  myMissionsEmpty:     document.getElementById("myMissionsEmpty"),

  missionDetail:       document.getElementById("missionDetailSection"),
  missionTitle:        document.getElementById("missionTitle"),
  missionCore:         document.getElementById("missionCore"),
  enrollmentsList:     document.getElementById("enrollmentsList"),
  enrollmentsEmpty:    document.getElementById("enrollmentsEmpty"),
  closeMissionBtn:     document.getElementById("closeMissionBtn"),
};

const btnJoinable   = document.getElementById("btnJoinable");
const btnMyMissions = document.getElementById("btnMyMissions");

// click handlers
btnJoinable?.addEventListener("click", () => {
  showOnlySection("joinableSection");
});

btnMyMissions?.addEventListener("click", async () => {
  showOnlySection("myMissionsSection");
  if (walletAddress) {
    try {
      const mine = await apiPlayerMissions(walletAddress.toLowerCase());
      renderMyMissions(mine);
    } catch (e) { console.error(e); }
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

function renderMissionDetail({ mission, enrollments, rounds }){
  const me   = (walletAddress || "").toLowerCase();
  const now  = Math.floor(Date.now()/1000);
  const isEnrolling = mission.status === 1 && now < mission.enrollment_end;
  const alreadyEnrolled = enrollments?.some(e => (e.player_address || "").toLowerCase() === me);
  const hasSpots = mission.enrollment_max_players > (enrollments?.length || 0);
  const canEnroll = isEnrolling && !alreadyEnrolled && hasSpots && walletAddress;

  const isActive   = mission.status === 3; // Status enum: 0 Pending, 1 Enrolling, 2 Arming, 3 Active, ...
  const roundsLeft = mission.round_count < mission.mission_rounds_total;

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
    btn.addEventListener("click", () => {
      showAlert("Mission gameplay view coming next.", "info");
    });
  }

  if (canEnroll) {
    const btn = document.createElement("button");
    btn.className = "btn btn-cyan";
    btn.id = "btnEnroll";
    btn.textContent = `Enroll (${weiToCro(mission.enrollment_amount_wei)} CRO)`;
    actions.appendChild(btn);
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
          ${formatLocalDateTime(mission.updated_at)}
        </div>

        <div class="col-md-3">
          <h4>Players</h4>
          <div class="fw-bold">
            Minimum ${minP} | Joined <span class="${joinedCls}">${joinedPlayers}</span> | Maximum ${maxP || "—"}
          </div>
        </div>

      </div>
    </div>
  </div>
`;

  // countdowns (only update spans that exist)
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

  document.getElementById("btnEnroll")?.addEventListener("click", () => enrollCurrentMission(mission));
  document.getElementById("btnTrigger")?.addEventListener("click", () => triggerRoundCurrentMission(mission));

}

/* ---------- interactions ---------- */
async function openMission(addr){
  try {
    currentMissionAddr = addr.toLowerCase(); 
    await subscribeToMission(currentMissionAddr);
    const data = await apiMission(currentMissionAddr);
    renderMissionDetail(data);
    window.scrollTo({ top: els.missionDetail.offsetTop - 20, behavior: "smooth" });
  } catch (e) {
    showAlert("Failed to load mission details.", "error");
    console.error(e);
  }
}

function closeMission(){
  els.missionDetail.classList.remove("overlay");
  els.missionDetail.style.display = "none";
  stopCountdown();
  currentMissionAddr = null;
  // restore the previous list (joinable by default until we add footer nav)
  showOnlySection(lastListShownId);
}

async function enrollCurrentMission(mission){
  try {
    const signer = getSigner();
    if (!signer) { showAlert("Connect your wallet first.", "error"); return; }

    const c = new ethers.Contract(mission.mission_address, MISSION_ABI, signer);
    const value = ethers.BigNumber.from(mission.enrollment_amount_wei);
    setBtnLoading(document.getElementById("btnEnroll"), true, "Enrolling…", true);

    // send tx
    const tx = await c.enrollPlayer({ value });
    await tx.wait(); // 1 confirmation is fine here

    showAlert("Enrollment confirmed!", "success");
    // refresh detail + “My Missions”
    const data = await apiMission(mission.mission_address);
    renderMissionDetail(data);
    if (walletAddress) {
      const mine = await apiPlayerMissions(walletAddress.toLowerCase());
      renderMyMissions(mine);
    }
  } catch (err) {
    console.error(err);
    showAlert(`Enroll failed: ${decodeError(err)}`, "error");
  } finally {
    setBtnLoading(document.getElementById("btnEnroll"), false, `Enroll (${weiToCro(mission.enrollment_amount_wei)} CRO)`, false);
  }
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

  // 3) close mission detail
  els.closeMissionBtn?.addEventListener("click", closeMission);

  // 4) auto-load My Missions when wallet connects (event-based if available, else fallback poll)
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

  // 5) soft detail refresh if a mission is open
  setInterval(() => {
    if (els.missionDetail.style.display !== "none" && currentMissionAddr){
      apiMission(currentMissionAddr)
        .then(renderMissionDetail)
        .catch(()=>{});
    }
  }, 15000);

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
