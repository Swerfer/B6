/**********************************************************************
 game.js – home page bootstrap, re-uses core.js & walletConnect.js
**********************************************************************/
import { connectWallet, disconnectWallet, walletAddress } from "./walletConnect.js";
import { showAlert, showConfirm, shorten } from "./core.js";

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
let hubConnection = null;

async function startHub(){
  if (!window.signalR) {
    showAlert("SignalR client script not found.", "error");
    return;
  }
  if (hubConnection?.state === "Connected") return;

  hubConnection = new signalR.HubConnectionBuilder()
    .withUrl("/hub/game")
    .withAutomaticReconnect()
    .build();

  // --- incoming events (we’ll expand these later) ---
  hubConnection.on("ServerPing", (msg) => {
    showAlert(`Server ping:<br>${msg}`, "info");
  });

  hubConnection.on("RoundResult", (addr, round, winner, amountWei) => {
    showAlert(
      `Round ${round} – ${winner}<br/>Amount (wei): ${amountWei}<br/>Mission: ${addr}`,
      "success"
    );
  });

  hubConnection.on("StatusChanged", (addr, newStatus) => {
    showAlert(`Mission status changed:<br>${addr}<br>Status: ${newStatus}`, "warning");
  });

  try{
    await hubConnection.start();
    // subscribe to a demo group so /demo/ping/demo hits us
    await hubConnection.invoke("SubscribeMission", "demo".toLowerCase());
  }catch(err){
    console.error("Hub start failed:", err);
    showAlert("Real-time channel failed to connect.", "error");
  }
}

if (document.readyState === "loading"){
  document.addEventListener("DOMContentLoaded", () => {
    connectWallet();
    startHub();
  }, { once:true });
} else {
  connectWallet();
  startHub();
}
