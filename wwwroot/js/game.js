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
  weiToCro, 
  MISSION_ABI, 
  setBtnLoading, 
  decodeError, 
} from "./core.js";

/* ---------- button ---------- */
const connectBtn = document.getElementById("connectWalletBtn");

connectBtn.addEventListener("click", () => {
  if (walletAddress){
    showConfirm("Disconnect current wallet?", disconnectWallet);
  } else {
    connectWallet(); 
  }
});

/* ---------- SignalR: connect to /hub/game ---------- */
// at module scope
let   hubConnection       = null;
let   hubStartPromise     = null;   // prevent concurrent starts
let   currentMissionAddr  = null;
let   subscribedAddr      = null;

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
  roundsList:          document.getElementById("roundsList"),
  roundsEmpty:         document.getElementById("roundsEmpty"),
  enrollmentsList:     document.getElementById("enrollmentsList"),
  enrollmentsEmpty:    document.getElementById("enrollmentsEmpty"),
  closeMissionBtn:     document.getElementById("closeMissionBtn"),
};

let countdownTimer = null;
function stopCountdown(){ if (countdownTimer) { clearInterval(countdownTimer); countdownTimer = null; }}

/* ---------- renderers ---------- */
function renderJoinable(items){
  els.joinableList.innerHTML = "";
  els.joinableEmpty.style.display = items?.length ? "none" : "";
  for (const m of (items || [])){
    const li = document.createElement("li");
    li.className = "view-card mb-3";
    li.innerHTML = `
      <div class="d-flex justify-content-between align-items-start flex-wrap gap-2">
        <div>
          <div class="fw-bold">${m.name || copyableAddr(m.mission_address)}</div>
          <div class="small text-muted">Type ${m.mission_type} · Fee ${weiToCro(m.enrollment_amount_wei)} CRO</div>
          <div class="small text-muted">Players ${m.enrolled_players}/${m.enrollment_max_players}</div>
          <div class="small">Enroll ends: <span data-enroll="${m.enrollment_end}">${formatLocalDateTime(m.enrollment_end)}</span></div>
        </div>
        <div class="d-flex gap-2">
          <button class="btn btn-sm btn-outline-info" data-view="${m.mission_address}">View</button>
        </div>
      </div>
    `;
    li.querySelector("[data-view]").addEventListener("click", () => openMission(m.mission_address));
    els.joinableList.appendChild(li);
  }
}

function renderMyMissions(items){
  els.myMissionsList.innerHTML = "";
  els.myMissionsEmpty.style.display = items?.length ? "none" : "";
  for (const m of (items || [])){
    const li = document.createElement("li");
    li.className = "view-card mb-3";
    const rounds = `${m.round_count}/${m.mission_rounds_total}`;
    li.innerHTML = `
      <div class="d-flex justify-content-between align-items-start flex-wrap gap-2">
        <div>
          <div class="fw-bold">${m.name || copyableAddr(m.mission_address)}</div>
          <div class="small text-muted">Status ${m.status} · Rounds ${rounds}</div>
          <div class="small text-muted">Start: ${formatLocalDateTime(m.mission_start)} · End: ${formatLocalDateTime(m.mission_end)}</div>
          ${m.refunded ? `<div class="small text-warning">Refunded ${m.refund_tx_hash ? '('+shorten(m.refund_tx_hash)+')' : ''}</div>` : ""}
        </div>
        <div class="d-flex gap-2">
          <button class="btn btn-sm btn-outline-info" data-view="${m.mission_address}">View</button>
        </div>
      </div>
    `;
    li.querySelector("[data-view]").addEventListener("click", () => openMission(m.mission_address));
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

  if (canEnroll) {
    const btn = document.createElement("button");
    btn.className = "btn btn-cyan";
    btn.id = "btnEnroll";
    btn.textContent = `Enroll (${weiToCro(mission.enrollment_amount_wei)} CRO)`;
    actions.appendChild(btn);
  }

  if (isActive && roundsLeft && walletAddress) {
    const btn = document.createElement("button");
    btn.className = "btn btn-outline-cyan";
    btn.id = "btnTrigger";
    btn.textContent = "Trigger Round";
    actions.appendChild(btn);
  }

  els.missionDetail.style.display = "";
  els.missionTitle.textContent = mission.name || mission.mission_address;

  els.missionCore.innerHTML = `
    <div class="row g-3">
      <div class="col-md-3"><div class="small text-muted">Address</div><div class="fw-bold">${mission.mission_address}</div></div>
      <div class="col-md-3"><div class="small text-muted">Type</div><div class="fw-bold">${mission.mission_type}</div></div>
      <div class="col-md-3"><div class="small text-muted">Status</div><div class="fw-bold">${mission.status}</div></div>
      <div class="col-md-3"><div class="small text-muted">Rounds</div><div class="fw-bold">${mission.round_count}/${mission.mission_rounds_total}</div></div>
      <div class="col-md-3"><div class="small text-muted">Enroll Fee</div><div class="fw-bold">${weiToCro(mission.enrollment_amount_wei)} CRO</div></div>
      <div class="col-md-3"><div class="small text-muted">CRO Start</div><div class="fw-bold">${weiToCro(mission.cro_start_wei)} CRO</div></div>
      <div class="col-md-3"><div class="small text-muted">CRO Current</div><div class="fw-bold">${weiToCro(mission.cro_current_wei)} CRO</div></div>
      <div class="col-md-3"><div class="small text-muted">Pause</div><div class="fw-bold">${mission.pause_timestamp ?? "—"}</div></div>
      <div class="col-md-3"><div class="small text-muted">Enroll End</div><div class="fw-bold"><span id="enrollCountdown">${formatCountdown(mission.enrollment_end)}</span></div></div>
      <div class="col-md-3"><div class="small text-muted">Mission End</div><div class="fw-bold"><span id="missionCountdown">${formatCountdown(mission.mission_end)}</span></div></div>
      <div class="col-md-3"><div class="small text-muted">Updated</div><div class="fw-bold">${formatLocalDateTime(mission.updated_at)}</div></div>
    </div>
  `;

  // countdowns
  stopCountdown();
  countdownTimer = setInterval(() => {
    const en = document.getElementById("enrollCountdown");
    const ms = document.getElementById("missionCountdown");
    if (en) en.textContent = formatCountdown(mission.enrollment_end);
    if (ms) ms.textContent = formatCountdown(mission.mission_end);
  }, 1000);

  // rounds
  els.roundsList.innerHTML = "";
  els.roundsEmpty.style.display = rounds?.length ? "none" : "";
  for (const r of (rounds || [])){
    const li = document.createElement("li");
    li.className = "view-card mb-2";
    li.innerHTML = `
      <div class="d-flex justify-content-between align-items-center">
        <div>
          <div class="fw-bold">Round ${r.round_number}</div>
          <div class="small text-muted">${formatLocalDateTime(r.created_at)} · Block ${r.block_number ?? "—"}</div>
        </div>
        <div class="text-end">
          <div class="small">Winner</div>
          <div class="fw-bold">${shorten(r.winner_address)}</div>
          <div class="small">${weiToCro(r.payout_wei)} CRO</div>
        </div>
      </div>
    `;
    els.roundsList.appendChild(li);
  }

  // enrollments
  els.enrollmentsList.innerHTML = "";
  els.enrollmentsEmpty.style.display = enrollments?.length ? "none" : "";
  for (const e of (enrollments || [])){
    const li = document.createElement("li");
    li.className = "view-card mb-2";
    li.innerHTML = `
      <div class="d-flex justify-content-between align-items-center">
        <div class="fw-bold">${shorten(e.player_address)}</div>
        <div class="text-end small">
          ${e.enrolled_at ? formatLocalDateTime(e.enrolled_at) : "—"}
          ${e.refunded ? `<div class="text-warning">Refunded ${e.refund_tx_hash ? '('+shorten(e.refund_tx_hash)+')' : ''}</div>` : ""}
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
  els.missionDetail.style.display = "none";
  stopCountdown();
  currentMissionAddr = null;
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
  // 1) initial joins
  try {
    const joinable = await apiJoinable();
    renderJoinable(joinable);
  } catch(e){ console.error(e); }

  // 2) refresh button
  els.refreshJoinableBtn?.addEventListener("click", async () => {
    try {
      const joinable = await apiJoinable();
      renderJoinable(joinable);
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
