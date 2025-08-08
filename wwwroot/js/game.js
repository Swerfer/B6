/**********************************************************************
 game.js – home page bootstrap, re-uses core.js & walletConnect.js
**********************************************************************/
import { connectWallet, disconnectWallet, walletAddress } from "./walletConnect.js";
import { showAlert, showConfirm, shorten } from "./core.js";

/* ---------- button ---------- */
const connectBtn     = document.getElementById("connectWalletBtn");

connectBtn.addEventListener("click", () => {
  if (walletAddress){
    showConfirm("Disconnect current wallet?", disconnectWallet);
  } else {
    connectWallet(); 
  }
});

(document.readyState === "loading"
  ? document.addEventListener("DOMContentLoaded", connectWallet, { once:true })
  : connectWallet());
