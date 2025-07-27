// b6.js v142

// UMD-provider van @walletconnect/ethereum-provider
const { EthereumProvider } = window["@walletconnect/ethereum-provider"];

// Jouw WalletConnect Cloud projectId
const projectId  = '2dab128ebcbc81670b98e793466c3e1d';
// Cronos Mainnet
const chainId    = 25;
const chainIdHex = '0x19';

const connectBtn    = document.getElementById('connectBtn');
const disconnectBtn = document.getElementById('disconnectBtn');
const walletInfoDiv = document.getElementById('walletInfo');
const addressElem   = document.getElementById('walletAddress');
const balanceElem   = document.getElementById('walletBalance');

let provider, ethersProvider, signer;

// Reset de UI naar de “vaak-voor-verbinden” status
function resetUI() {
  walletInfoDiv.style.display   = 'none';
  disconnectBtn.style.display   = 'none';
  addressElem.textContent       = '';
  balanceElem.textContent       = '';
}

// Disconnect-knop handler
async function disconnectWallet() {
  try {
    // Sluit WalletConnect sessie af als dat van toepassing is
    if (provider && typeof provider.disconnect === 'function') {
      await provider.disconnect();
    }
  } catch (err) {
    console.warn('Fout bij disconnect:', err);
  } finally {
    // Clear state & UI
    provider      = null;
    ethersProvider = null;
    signer        = null;
    resetUI();
  }
}

disconnectBtn.addEventListener('click', disconnectWallet);

async function connectWallet() {
  try {
    // === 1) Probeer injected wallet (desktop / in-app) ===
    if (typeof window.ethereum !== 'undefined') {
      await window.ethereum.request({ method: 'eth_requestAccounts' });
      ethersProvider = new ethers.BrowserProvider(window.ethereum);
      signer         = await ethersProvider.getSigner();
    } else {
      // === 2) Fallback: WalletConnect (QR + deep link mobiele wallets) ===
      if (!provider) {
        provider = await EthereumProvider.init({
          projectId,
          chains: [chainId],
          showQrModal: true,  // laat de modal zien met keuze en QR
          metadata: {
            name: "B6 Missions",
            description: "Connect met je wallet",
            url: window.location.origin,
            icons: []
          }
        });
      }
      // opent de standaard WalletConnect modal
      await provider.enable();
      ethersProvider = new ethers.BrowserProvider(provider);
      signer         = await ethersProvider.getSigner();
    }

    // === 3) Toon adres + disconnect-knop ===
    const address = await signer.getAddress();
    addressElem.textContent     = address;
    walletInfoDiv.style.display = 'block';
    disconnectBtn.style.display = 'inline-block';

    // === 4) Switch/voeg Cronos Mainnet toe indien nodig ===
    const rpcTarget   = provider || window.ethereum;
    const currentChain = await rpcTarget.request({ method: 'eth_chainId' });
    if (currentChain !== chainIdHex) {
      try {
        await rpcTarget.request({
          method: 'wallet_switchEthereumChain',
          params: [{ chainId: chainIdHex }],
        });
      } catch (switchError) {
        if (switchError.code === 4902) {
          await rpcTarget.request({
            method: 'wallet_addEthereumChain',
            params: [{
              chainId: chainIdHex,
              chainName: 'Cronos Mainnet',
              rpcUrls: ['https://evm.cronos.org'],
              nativeCurrency: { name: 'Cronos', symbol: 'CRO', decimals: 18 },
              blockExplorerUrls: ['https://cronoscan.com']
            }],
          });
        } else {
          console.error(switchError);
        }
      }
    }

    // === 5) Haal CRO-saldo op ===
    const balanceWei = await ethersProvider.getBalance(address);
    const balance    = ethers.formatEther(balanceWei);
    balanceElem.textContent = `${balance} CRO`;

  } catch (err) {
    console.error('Connectie mislukt:', err);
  }
}

connectBtn.addEventListener('click', connectWallet);

// Bij laden: reset UI (voor het geval er nog iets staat)
resetUI();
