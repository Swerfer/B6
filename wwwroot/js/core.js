/**********************************************************************
 core.js  – shared UI utilities (modals, shortener, global caches)
**********************************************************************/
export const FACTORY_ADDRESS = "0x9309f012C08Df9Af3a70b1187040A0d18ED18eed";
export const READ_ONLY_RPC   = "https://evm.cronos.org";

export const FACTORY_ABI = [
  "function owner() view returns(address)",
  "function authorized(address) view returns(bool)",
  "function createMission(uint8,uint256,uint256,uint256,uint8,uint8,uint256,uint256,uint8) payable returns(address)",
  "function getAllMissions() view returns(address[] missions, uint8[] statuses)",
];

/* ABI for a single Mission contract */
/* ABI for a single Mission contract – matches getMissionData() tuple (15 fields) */
/* ABI that mirrors struct MissionData exactly */
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
  "function owner() view returns (address)",
];


export const shorten = addr =>
  addr ? `${addr.slice(0, 6)}…${addr.slice(-4)}` : "";

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
export const statusText = code => Status[code] ?? `Unknown(${code})`;

/* ---------- reusable spinner helpers ---------- */
export function setBtnLoading(btn, state = true, label = ""){
  if(!btn) return;

  if(state){
    /* already active? do nothing */
    if(btn.dataset.loading) return;

    /* store original content + width so the button never shrinks */
    btn.dataset.loading       = btn.innerHTML;
    btn.dataset.loadingWidth  = btn.offsetWidth;      // px value
    btn.style.width           = `${btn.dataset.loadingWidth}px`;

    /* swap in spinner & label */
    btn.classList.add("btn-loading");
    btn.innerHTML = `<span class="spinner"></span><span>${label}</span>`;
  }else{
    /* nothing to stop */
    if(!btn.dataset.loading) return;

    /* restore */
    btn.classList.remove("btn-loading");
    btn.innerHTML = btn.dataset.loading;
    btn.style.width = "";                           // drop fixed width
    delete btn.dataset.loading;
    delete btn.dataset.loadingWidth;
  }
}

/* ---------- DOM caches (present on both pages) ---------- */
const modalOverlay = document.getElementById("modalOverlay");
const confirmModal = document.getElementById("confirmModal");
const modalMsg     = document.getElementById("modalMessage");
const modalConfirm = document.getElementById("modalConfirm");
const modalCancel  = document.getElementById("modalCancel");
const alertModal   = document.getElementById("alertModal");
const alertTitle   = document.getElementById("alertModalTitle");
const alertText    = document.getElementById("alertModalText");
const alertClose   = document.getElementById("alertModalCloseBtn");

/* ---------- Confirm dialog ---------- */
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

/* ---------- Alert dialog ---------- */
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