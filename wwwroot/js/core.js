/**********************************************************************
 core.js  – shared UI utilities (modals, shortener, global caches)
**********************************************************************/

// Defaults (fallbacks if /api/config is unavailable)
export let READ_ONLY_RPC;
export let FACTORY_ADDRESS;

// Load runtime config once. Because this file is loaded as type="module",
// top-level await is supported in modern browsers.
try {
  const res = await fetch('/api/config', { cache: 'no-store' });
  if (res.ok) {
    const cfg = await res.json();
    const rpc     = cfg?.cronos?.rpc        || cfg?.rpc;
    const factory = cfg?.contracts?.factory || cfg?.factory;
    if (rpc)     READ_ONLY_RPC   = rpc;
    if (factory) FACTORY_ADDRESS = factory;
    console.log('[core] /api/config loaded');
  } else {
    console.warn('[core] /api/config failed', res.status);
  }
} catch (err) {
  console.warn('[core] /api/config error', err);
}

export const FACTORY_ABI = [
  // --------- Factory methods ----------
  "function createMission(uint8,uint256,uint256,uint256,uint8,uint8,uint256,uint256,uint8,string) payable returns(address)",
  // --------- Get missions views ----------
  "function getAllMissions() view returns(address[] missions, uint8[] statuses, string[] names)",
  "function getMissionsByStatus(uint8 status) view returns(address[] missions, uint8[] statuses, string[] names)",
  "function getMissionsEnded() view returns(address[] missions, uint8[] statuses, string[] names)",
  "function getMissionsNotEnded() view returns(address[] missions, uint8[] statuses, string[] names)",
  "function getLatestMissions(uint256) view returns(address[] missions, uint8[] statuses, string[] names)",
  // ------------ global views ------------
  "function getFactorySummary() view returns (address,address,address,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256[])",
  "function owner() view returns(address)",
  "function weeklyLimit() view returns(uint256)",
  "function monthlyLimit() view returns(uint256)",
  "function totalMissionFunds() view returns(uint256)",
  "function totalOwnerEarnedFunds() view returns(uint256)",
  "function totalMissionSuccesses() view returns(uint256)",
  "function totalMissionFailures() view returns(uint256)",
  "function missionImplementation() view returns(address)",
  "function getTotalMissions() view returns(uint256)",
  "function getFundsByType(uint8) view returns(uint256)",
  "function getOwnershipProposal() view returns(address newOwner, address proposer, uint256 timestamp, uint256 timeLeft)",
  // ------------ global writes -----------
  "function setEnrollmentLimits(uint256 newWeeklyLimit, uint256 newMonthlyLimit)",
  "function addAuthorizedAddress(address addr)",
  "function removeAuthorizedAddress(address addr)",
  "function proposeOwnershipTransfer(address newOwner)",
  "function confirmOwnershipTransfer()",
  "function withdrawFunds(uint256 amount)",
  // ---------- address-specific ----------
  "function authorized(address) view returns(bool)",
  "function getPlayerLimits(address) view returns(uint8, uint8, uint8, uint8, uint256, uint256)",
  "function canEnroll(address) view returns(bool)",
  "function secondsTillWeeklySlot(address) view returns(uint256)",
  "function secondsTillMonthlySlot(address) view returns(uint256)",
  "function isMission(address) view returns(bool)",
  "function getPlayerParticipation(address) view returns(address[] joined, uint8[] statuses, string[] names)",
];

export const MISSION_ABI = [
  "function getMissionData() view returns (\
      tuple(\
        address[] players,\
        uint8 missionType,\
        uint256 enrollmentStart,\
        uint256 enrollmentEnd,\
        uint256 enrollmentAmount,\
        uint8 enrollmentMinPlayers,\
        uint8 enrollmentMaxPlayers,\
        uint256 missionStart,\
        uint256 missionEnd,\
        uint8 missionRounds,\
        uint8 roundCount,\
        uint256 ethStart,\
        uint256 ethCurrent,\
        (address,uint256)[] playersWon,\
        uint256 pauseTimestamp,\
        address[] refundedPlayers\
      )\
  )",
  "function getRealtimeStatus() view returns (uint8)",
  "function refundPlayers()",
  "function forceFinalizeMission()",
  "function owner() view returns (address)",
  "function enrollPlayer() payable",
  "function callRound()",
];

/* --------------------- Helpers --------------------- */
export const shorten = addr =>
  addr ? `${addr.slice(0, 6)}…${addr.slice(-4)}` : "";

export function weiToCro(weiStr, decimals = 6) {
  if (!weiStr) return "0";
  try {
    const wei  = BigInt(weiStr);
    const base = 10n ** 18n;
    const i    = wei / base;
    const fraw = (wei % base).toString().padStart(18, "0");
    const f    = fraw.slice(0, decimals).replace(/0+$/, "");
    return f ? `${i}.${f}` : `${i}`;
  } catch {
    return String(weiStr);
  }
}

export function copyableAddr(addr){
  if(!addr) return "";
  return `
    <span class="copy-wrap" data-copy="${addr}">
      ${shorten(addr)}
      <i class="fa-regular fa-copy ms-1 copy-icon"></i>
    </span>`;
}

export function extLinkIcon(url, title = "Open in new tab"){
  if (!url) return "";
  return `
    <a class="ms-2" href="${url}" target="_blank" rel="noopener" title="${title}">
      <i class="fa-solid fa-arrow-up-right-from-square"></i>
    </a>`;
}

export function txLinkIcon(txHash){
  return txHash ? extLinkIcon(`https://cronoscan.com/tx/${txHash}`, "Open on Cronoscan") : "";
}

export function addrLinkIcon(addr){
  return addr ? extLinkIcon(`https://cronoscan.com/address/${addr}`, "Open on Cronoscan") : "";
}

export function statusColorClass(s) {
  // 0 Pending, 1 Enrolling, 2 Arming → info
  // 3 Active → info (or success if you prefer)
  // 4 Paused → warning
  // 5 PartlySuccess, 6 Success → success
  // 7 Failed → error
  if (s === 7) return "text-error";
  if (s === 6 || s === 5) return "text-success";
  if (s === 4) return "text-warning";
  return "text-info";
}

export const unixToDate = (sec) => new Date((Number(sec) || 0) * 1000);

export const formatLocalDateTime = (sec) => {
  if (sec == null) return "";
  return unixToDate(sec).toLocaleString(navigator.language, {
      dateStyle: 'short',
      timeStyle: 'short'
    });
};

export const formatCountdown = (targetSec) => {
  if (!targetSec) return "";
  const nowSec = Math.floor(Date.now() / 1000);
  let left = Math.max(0, targetSec - nowSec);

  const d = Math.floor(left / 86400); left -= d * 86400;
  const h = Math.floor(left / 3600);  left -= h * 3600;
  const m = Math.floor(left / 60);    left -= m * 60;
  const s = left;

  const pad = (n) => String(n).padStart(2, "0");
  return `${d}d ${pad(h)}:${pad(m)}:${pad(s)}`;
};

export const formatDurationShort = (seconds) => {
  let s = Math.max(0, Number(seconds) || 0);
  const d = Math.floor(s / 86400); s -= d * 86400;
  const h = Math.floor(s / 3600);  s -= h * 3600;
  const m = Math.floor(s / 60);

  if (d >= 30) return "1 mo";        // coarse label for very long
  if (d >= 7)  return `${Math.round(d/7)} w`;
  if (d > 0 && h > 0) return `${d} d ${h} h`;
  if (d > 0)  return `${d} d`;
  if (h > 0 && m > 0) return `${h} h ${m} m`;
  if (h > 0)  return `${h} h`;
  if (m > 0)  return `${m} m`;
  return "0 m";
};

document.addEventListener("click", e=>{
  const tgt = e.target.closest("[data-copy]");
  if(!tgt) return;
  navigator.clipboard.writeText(tgt.dataset.copy)
    .then(()=>showAlert("Address copied ✓","success"))
    .catch(()=>showAlert("Copy failed","error"));
});

export const missionTypeName = {
  0:  "Custom",         // Custom mission type
  1:  "Hourly",         // Hourly missions
  2:  "Quarter-Daily",  // Quarter-Daily missions
  3:  "Bi-Daily",       // Bi-Daily missions
  4:  "Daily",          // Daily missions
  5:  "Weekly",         // Weekly missions
  6:  "Monthly"         // Monthly missions
};

export const Status = {
  0:  "Pending",        // Mission is created but not yet enrolling
  1:  "Enrolling",      // Mission is open for enrollment, waiting for players to join
  2:  "Arming",         // Mission is armed and ready to start
  3:  "Active",         // Mission is currently active and players can participate
  4:  "Paused",         // Mission is paused, no further actions can be taken
  5:  "PartlySuccess",  // Mission has ended with some players winning, but not all rounds were claimed
  6:  "Success",        // Mission has ended successfully, all rounds were claimed
  7:  "Failed",         // Mission has failed, no players won or not enough players enrolled
};

export const limit = {
  0: "None",            // No limit
  1: "Weekly",          // Weekly limit
  2: "Monthly",         // Monthly limit  
}

export const statusText = code => Status[code] ?? `Unknown(${code})`;

export function setBtnLoading(btn, state = true, label = "", restore = true) {
  if (!btn) return;

  /* ---------- START LOADING ---------- */
  if (state) {
    if (btn.dataset.loading) return;                 // already loading

    btn.dataset.loading      = btn.innerHTML;        // remember markup
    btn.dataset.loadingWidth = btn.offsetWidth;
    btn.style.width = `${btn.dataset.loadingWidth}px`;

    btn.classList.add("btn-loading");
    const isConnect = btn.id === "connectWalletBtn";
    const labelSpan = (restore || !isConnect)
      ? `<span class="label-loading">${label}</span>`
      : `<span id="connectBtnText" class="label-loading">${label}</span>`;

    btn.innerHTML = `
      <span class="spinner fade-spinner"></span>
      ${labelSpan}`;

    const spinner = btn.querySelector(".fade-spinner");
    void spinner.offsetWidth;                        // force reflow
    spinner.classList.add("show");                   // fade-in 0 → 1
    return;
  }

  /* ---------- STOP  LOADING ---------- */
  if (!btn.dataset.loading) return;                  // not in loading state

  const restoreMarkup = () => {
    if (restore) {
      btn.innerHTML = btn.dataset.loading;
    } else {
      const isConnect = btn.id === "connectWalletBtn";
      btn.innerHTML = isConnect
        ? `<span id="connectBtnText" class="label-loading">${label}</span>`
        : `<span class="label-loading">${label}</span>`;
    }
  };
  const cleanup = () => {
    spinner?.removeEventListener("transitionend", cleanup);
    btn.classList.remove("btn-loading");
    restoreMarkup();
    btn.style.width = "";
    delete btn.dataset.loading;
    delete btn.dataset.loadingWidth;
  };

  const spinner = btn.querySelector(".fade-spinner");
  if (spinner) {
    requestAnimationFrame(() => spinner.classList.remove("show"));  // 1 → 0
    spinner.addEventListener("transitionend", cleanup, { once:true });
    setTimeout(cleanup, 600);                    // ← unchanged fallback
  } else {                                       // ← unchanged “no-spinner” path
    cleanup();
  }
}

/* ---------- DOM caches ---------- */
const modalOverlay = document.getElementById("modalOverlay");
const confirmModal = document.getElementById("confirmModal");
const modalMsg     = document.getElementById("modalMessage");
const modalConfirm = document.getElementById("modalConfirm");
const modalCancel  = document.getElementById("modalCancel");
const alertModal   = document.getElementById("alertModal");
const alertTitle   = document.getElementById("alertModalTitle");
const alertText    = document.getElementById("alertModalText");
const alertClose   = document.getElementById("alertModalCloseBtn");

export function showConfirm(message, onYes) {
  alertModal.classList.add("hidden");
  confirmModal.classList.remove("hidden");

  modalMsg.innerHTML =
    `<i class="fa-solid fa-circle-question fa-lg text-cyan me-2"></i>${message}`;

  modalOverlay.classList.add("active");

  function close() {
    modalOverlay.classList.remove("active");
  }

  modalConfirm.onclick = async () => {
    close();
    if (typeof onYes === "function") {
      try {
        await onYes();
      } catch (err) {
        console.error("Confirm action failed:", err);
        showAlert("An error occurred while confirming.", "error");
      }
    }
  };

  modalCancel.onclick = close;

  modalOverlay.onclick = e => {
    if (e.target === modalOverlay) close();
  };
}

export function showAlert(message, type = "info", onClose = null){
  confirmModal.classList.add("hidden");
  alertModal.className = `modal-box ${type}`;
  alertTitle.innerHTML =
    `<i class="fa-solid ${
      {info:"fa-circle-info", success:"fa-circle-check",
       warning:"fa-triangle-exclamation", error:"fa-circle-xmark"}[type]
    } fa-lg me-2"></i>${type[0].toUpperCase() + type.slice(1)}`;
  alertText.innerHTML = message;
  alertModal.classList.remove("hidden");
  modalOverlay.classList.add("active");

  const close = () => { modalOverlay.classList.remove("active"); onClose?.(); };
  alertClose.onclick  = close;
  modalOverlay.onclick = e => { if(e.target === modalOverlay) close(); };
}

export const clearSelection = () => {
  const sel = window.getSelection?.();
  if(sel && sel.removeAllRanges){ sel.removeAllRanges(); }
};

export function fadeSpinner(el, show) {
  if (!el) return;

  el.style.transition = `opacity 500ms ease`;

  if (show) {
    el.classList.remove("hidden");
    void el.offsetWidth;
    el.classList.add("show");
  } else {
    el.style.opacity = "0";
    setTimeout(() => el.classList.add("hidden"), 500);
  }
}

export function decodeError(err) {
  // 1 ▸ Try common paths
  let msg =
         err?.data?.message
      || err?.error?.data?.message
      || err?.reason
      || err?.error?.message;

  // 2 ▸ Fallback: decode Error(string)
  if (!msg) {
    const hexData = err?.data?.originalError?.data
                 || err?.data
                 || err?.error?.data;
    if (hexData && hexData.startsWith("0x08c379a0")) {
      try {
        const iface = new ethers.utils.Interface(["function Error(string)"]);
        msg = iface.decodeFunctionData("Error", hexData)[0];
      } catch {/* ignore */}
    }
  }

  return msg || err?.message || "Transaction failed";
}


