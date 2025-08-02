/**********************************************************************
 core.js  – shared UI utilities (modals, shortener, global caches)
**********************************************************************/
export const FACTORY_ADDRESS = "0x9309f012C08Df9Af3a70b1187040A0d18ED18eed";
export const READ_ONLY_RPC   = "https://evm.cronos.org";

export const FACTORY_ABI = [
  "function owner() view returns(address)",
  "function authorized(address) view returns(bool)",
  "function createMission(uint8,uint256,uint256,uint256,uint8,uint8,uint256,uint256,uint8) payable returns(address)"
];

export const shorten = addr =>
  addr ? `${addr.slice(0, 6)}…${addr.slice(-4)}` : "";

/* ---------- reusable spinner helpers ---------- */
export function setBtnLoading(btn, state = true, label = "Creating&nbsp;Mission"){
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

export const clearSelection = () => {
  const sel = window.getSelection?.();
  if(sel && sel.removeAllRanges){ sel.removeAllRanges(); }
};