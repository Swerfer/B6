/**********************************************************************
 game.js – home page bootstrap, re-uses core.js & walletConnect.js
**********************************************************************/
import { connectWallet, disconnectWallet, walletAddress } from "./walletConnect.js";
import { showAlert, showConfirm } from "./core.js";

/* ---------- button ---------- */
const connectBtn     = document.getElementById("connectWalletBtn");
const connectBtnText = document.getElementById("connectBtnText");

connectBtn.addEventListener("click", () => {
  if (walletAddress){
    showConfirm("Disconnect current wallet?", disconnectWallet);
  } else {
    connectWallet();
  }
});

/* ---------- auto-connect on DOM ready ---------- */
(document.readyState === "loading"
  ? document.addEventListener("DOMContentLoaded", connectWallet, { once:true })
  : connectWallet());
