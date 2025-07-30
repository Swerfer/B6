/*--------------------------------------------------------------------
  walletConnect.js – all wallet / Web3Modal logic
--------------------------------------------------------------------*/
import { showAlert } from "./core.js";
export let walletAddress = null;        // live export
let web3Modal, provider, signer;

const connectBtnText      = document.getElementById("connectBtnText");

const shortenAddress = a => a ? `${a.slice(0,6)}…${a.slice(-4)}` : "";

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
  connectBtnText.textContent = shortenAddress(walletAddress);

  /* account change handler */
  provider.provider.on("accountsChanged", accts=>{
    if(!accts.length){
      disconnectWallet();
    }else{
      walletAddress = accts[0].toLowerCase();
      connectBtnText.textContent = shortenAddress(walletAddress);
    }
  });
}

/* -------- public: connect -------- */
export async function connectWallet(){
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

/* -------- public: disconnect -------- */
export function disconnectWallet(){
  if(provider?.provider?.disconnect) provider.provider.disconnect();
  web3Modal.clearCachedProvider && web3Modal.clearCachedProvider();
  walletAddress = null;
  connectBtnText.textContent = "Connect Wallet";
}

/* run immediately */
initWeb3Modal();
