/**************************************************************************
 *  Be Brave Be Bold Be Banked – wallet-connect & modal helpers
 **************************************************************************/

let web3Modal, provider, signer, walletAddress;

/* ---------- DOM CACHES ---------- */
const connectBtn          = document.getElementById("connectWalletBtn");
const connectBtnText      = document.getElementById("connectBtnText");
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

/* ---------- HELPERS ---------- */
const shortenAddress = a => a ? `${a.slice(0,6)}…${a.slice(-4)}` : "";

/*   Confirm dialog  */
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

/*   Alert dialog  */
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

/* ---------- WALLET FLOW ---------- */
async function afterWalletConnect(instance){
  provider      = new ethers.providers.Web3Provider(instance);
  signer        = provider.getSigner();
  walletAddress = (await signer.getAddress()).toLowerCase();
  connectBtnText.textContent = shortenAddress(walletAddress);

  provider.provider.on("accountsChanged", accts=>{
    if(!accts.length){ disconnectWallet(); }
    else{
      walletAddress = accts[0].toLowerCase();
      connectBtnText.textContent = shortenAddress(walletAddress);
    }
  });
}

async function connectWallet(){
  if(walletAddress) return;
  connectBtnText.textContent = "Connecting…";

  try{
    const instance = await retryWeb3ConnectWithTimeout(10, 1000);
    if(!instance){
      showAlert("Wallet connection timed out.<br>Please try again.","error");
      connectBtnText.textContent = "Connect Wallet";
      return;
    }
    await afterWalletConnect(instance);
  }catch(err){
    console.error(err);
    showAlert("Wallet connection failed.<br>Please try again.","error");
    connectBtnText.textContent = "Connect Wallet";
  }
}

/* retryWalletConnect helper – returns the instance or null after N attempts */
async function retryWeb3ConnectWithTimeout(maxRetries = 10, timeoutMs = 1000) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const instance = await Promise.race([
        web3Modal.connect(),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error("Connect timed out")), timeoutMs)
        )
      ]);
      return instance;
    } catch (err) {
      if (attempt === maxRetries) return null;
      await new Promise(res => setTimeout(res, timeoutMs));
    }
  }
}

function disconnectWallet(){
  if(provider?.provider?.disconnect) provider.provider.disconnect();
  walletAddress = null;
  connectBtnText.textContent = "Connect Wallet";
}

/* ---------- BOOTSTRAP BTN CLICK ---------- */
connectBtn.addEventListener("click", ()=>{
  if(walletAddress){
    showConfirm("Disconnect current wallet?", disconnectWallet);
  }else{
    connectWallet();
  }
});

/* ---------- INIT ---------- */
web3Modal = new window.Web3Modal.default({
  cacheProvider:true,
  providerOptions:{},
  disableInjectedProvider:false
});

window.addEventListener("load", () => {
  if (web3Modal.cachedProvider) connectWallet();
});
