/*--------------------------------------------------------------------
  main.js – boots the app, holds the UI helpers, imports wallet logic
--------------------------------------------------------------------*/
import {
  connectWallet,
  disconnectWallet,
  autoConnectOnLoad,
  walletAddress            // live binding – always up-to-date
} from "./walletConnect.js";

/* ---------- DOM CACHES ---------- */
const connectBtn          = document.getElementById("connectWalletBtn");
const modalOverlay        = document.getElementById("modalOverlay");

/* Confirm modal */
const confirmModal        = document.getElementById("confirmModal");
const modalMsg            = document.getElementById("modalMessage");
const modalConfirmBtn     = document.getElementById("modalConfirm");
const modalCancelBtn      = document.getElementById("modalCancel");

/* Alert modal */
const alertModal          = document.getElementById("alertModal");
const alertTitle          = document.getElementById("alertModalTitle");
const alertText           = document.getElementById("alertModalText");
const alertCloseBtn       = document.getElementById("alertModalCloseBtn");

/* Confirm dialog */
function showConfirm(message, onConfirm){
  alertModal.classList.add("hidden");
  confirmModal.classList.remove("hidden");

  modalMsg.innerHTML =
    `<i class="fa-solid fa-circle-question fa-lg text-cyan me-2"></i>${message}`;
  modalOverlay.classList.add("active");

  const close = () => modalOverlay.classList.remove("active");
  modalConfirmBtn.onclick = () => { close(); onConfirm(); };
  modalCancelBtn .onclick = close;
  modalOverlay.onclick = e => { if(e.target===modalOverlay) close(); };
}
/* Make showConfirm global so walletConnect.js can call it if needed */
window.showConfirm = showConfirm;

/* Alert dialog */
function showAlert(message, type="info", onClose=null){
  confirmModal.classList.add("hidden");
  alertModal.className = `modal-box ${type}`;
  alertTitle.innerHTML =
    `<i class="fa-solid ${
        {info:"fa-circle-info",success:"fa-circle-check",
         warning:"fa-triangle-exclamation",error:"fa-circle-xmark"}[type] || "fa-circle-info"
     } me-2"></i>${type[0].toUpperCase()+type.slice(1)}`;

  alertText.innerHTML = message;
  alertModal.classList.remove("hidden");
  modalOverlay.classList.add("active");

  const close = () => { modalOverlay.classList.remove("active"); if(onClose) onClose(); };
  alertCloseBtn.onclick = close;
  modalOverlay.onclick  = e => { if(e.target===modalOverlay) close(); };
}
window.showAlert = showAlert;   // expose for walletConnect.js

/* ---------- BUTTON CLICK ---------- */
connectBtn.addEventListener("click", () => {
  if (walletAddress){
    showConfirm("Disconnect current wallet?", disconnectWallet);
  } else {
    connectWallet();
  }
});

/* ------------------------------------------------------------------
   AUTO-CONNECT ON EVERY PAGE VISIT
   - waits for the DOM so the button text node exists,
   - then calls connectWallet() exactly once.
-------------------------------------------------------------------*/
(() => {
  const start = () => connectWallet();          // always run, no checks

  if (document.readyState === "loading") {      // HTML still parsing?
    document.addEventListener("DOMContentLoaded", start, { once: true });
  } else {                                      // DOM already built
    start();
  }
})();
