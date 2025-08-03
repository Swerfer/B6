/*--------------------------------------------------------------------
  walletConnect.js – all wallet / Web3Modal logic
--------------------------------------------------------------------*/
// NEW ------------- walletConnect.js (imports + constants)
import { showAlert, setBtnLoading, shorten } from "./core.js";
export let walletAddress = null;
let web3Modal, provider, signer;

const connectBtn     = document.getElementById("connectWalletBtn");

/* -------- init Web3Modal once -------- */
function initWeb3Modal(){
  web3Modal = new window.Web3Modal.default({
    cacheProvider: true,
    providerOptions: {},
    disableInjectedProvider: false
  });
}

/* -------- retry helper with timeout -------- */
async function retryWeb3ConnectWithTimeout(maxRetries = 10, timeoutMs = 1000){
  for (let attempt = 1; attempt <= maxRetries; attempt++){
    try{
      const instance = await Promise.race([
        web3Modal.connect(),
        new Promise((_, reject)=>
          setTimeout(()=>reject(new Error("Connect timed out")), timeoutMs))
      ]);
      return instance;                      // success
    }catch(err){
      if (attempt === maxRetries) return null;
      await new Promise(r => setTimeout(r, timeoutMs));
    }
  }
}

/* -------- after connect -------- */
async function afterWalletConnect(instance){
  provider      = new ethers.providers.Web3Provider(instance);
  signer        = provider.getSigner();
  walletAddress = (await signer.getAddress()).toLowerCase();
  await new Promise(res => {
    connectBtn.addEventListener("transitionend", res, { once:true });
    setBtnLoading(connectBtn, false, shorten(walletAddress), false);       // fade spinner out
    setTimeout(res, 600);                   // safety-net
  });

  setConnectText(shorten(walletAddress));

  provider.provider.on("accountsChanged", accts=>{
    if (!accts.length){
      disconnectWallet();
    } else {
      walletAddress = accts[0].toLowerCase();
      setConnectText(shorten(walletAddress)); 
    }
  });
}

/* -------- public: connect -------- */
export async function connectWallet(){
  if (walletAddress) return;

  if (connectBtn) {
    setBtnLoading(connectBtn, true, "Connecting");
  }
  else            setConnectText("Connecting…");

  try{
    const instance = await retryWeb3ConnectWithTimeout(10, 1000);
    if (!instance){
      showAlert("Wallet connection timed out.<br>Please try again.","error");
      resetBtn();
      return;
    }
    await afterWalletConnect(instance);
  }catch(err){
    console.error(err);
    showAlert("Wallet connection failed.<br>Please try again.","error");
    resetBtn();
  }
}

/* -------- public: disconnect -------- */
export function disconnectWallet(){
  if (provider?.provider?.disconnect) provider.provider.disconnect();
  web3Modal.clearCachedProvider && web3Modal.clearCachedProvider();
  walletAddress = null;
  resetBtn();
}

function resetBtn(){
  if (connectBtn) setBtnLoading(connectBtn, false);
  setConnectText("Connect Wallet");
}

// NEW — add just below the constants
const setConnectText = (txt = "Connect Wallet") => {
  const span = document.getElementById("connectBtnText");
  if (span) span.textContent = txt;
};

/* run immediately */
initWeb3Modal();
