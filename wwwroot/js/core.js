/**********************************************************************
 core.js  – shared UI utilities (modals, shortener, global caches)
**********************************************************************/
export const FACTORY_ADDRESS = "0xe1A460cD14c6A02E0BE4AC620DfC7CD3fFD63B7a";
export const READ_ONLY_RPC   = "https://evm.cronos.org";

export const FACTORY_ABI = [
  "function owner() view returns(address)",
  "function authorized(address) view returns(bool)",
  "function createMission(uint8,uint256,uint256,uint256,uint8,uint8,uint256,uint256,uint8) payable returns(address)"
];

export const shorten = addr =>
  addr ? `${addr.slice(0, 6)}…${addr.slice(-4)}` : "";

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
export function showConfirm(message, onYes){
  alertModal.classList.add("hidden");
  confirmModal.classList.remove("hidden");
  modalMsg.innerHTML =
    `<i class="fa-solid fa-circle-question fa-lg text-cyan me-2"></i>${message}`;
  modalOverlay.classList.add("active");

  const close = () => modalOverlay.classList.remove("active");
  modalConfirm.onclick = () => { close(); onYes(); };
  modalCancel .onclick = close;
  modalOverlay.onclick  = e => { if(e.target === modalOverlay) close(); };
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
