const hre  = require("hardhat");
const fs   = require("fs");
const path = require("path");

async function main() {
  console.log("🚀 Starting full project deployment…");

  const [deployer] = await hre.ethers.getSigners();
  console.log("👤 Deployer:", deployer.address);
  const balance = await hre.ethers.provider.getBalance(deployer.address);
  console.log("💰 Balance:", hre.ethers.formatEther(balance), "CRO");

  /* ──────────────────────────────────────────────────────────
     1. Deploy Mission implementation (once)
  ────────────────────────────────────────────────────────── */
  const Mission = await hre.ethers.getContractFactory("Mission");
  const missionImpl = await Mission.deploy({ gasLimit: 12_000_000 });
  await missionImpl.waitForDeployment();
  const missionImplAddress = await missionImpl.getAddress();
  console.log("🧩 Mission implementation deployed at:", missionImplAddress);

  // give the RPC a moment to update its nonce cache
  await new Promise(r => setTimeout(r, 3000));

  /* ──────────────────────────────────────────────────────────
     2. Deploy MissionFactory, passing the impl address
  ────────────────────────────────────────────────────────── */
  const nextNonce = await hre.ethers.provider.getTransactionCount(
    deployer.address,
    "latest"                       // guarantees we skip any pending nonce
  );

  const Factory = await hre.ethers.getContractFactory("MissionFactory");
  const missionFactory = await Factory.deploy(
    missionImplAddress,
    { gasLimit: 4_000_000, nonce: nextNonce }
  );
  await missionFactory.waitForDeployment();
  const missionFactoryAddress = await missionFactory.getAddress();
  console.log("🏗️  MissionFactory deployed at:", missionFactoryAddress);

  /* ──────────────────────────────────────────────────────────
     3. Sanity-check the public getter
  ────────────────────────────────────────────────────────── */
  const getterValue = await missionFactory.missionImplementation();
  console.log("🔍 missionImplementation() returns:", getterValue);

  /* ──────────────────────────────────────────────────────────
     4. Fund the factory with 10 CRO
  ────────────────────────────────────────────────────────── */
  console.log("💸 Funding MissionFactory with 10 CRO…");
  const fundTx = await deployer.sendTransaction({
    to: missionFactoryAddress,
    value: hre.ethers.parseEther("10")
  });
  await fundTx.wait();
  console.log("✅ Funding complete!");

  /* ──────────────────────────────────────────────────────────
     5. Persist deployment info
  ────────────────────────────────────────────────────────── */
  const deploymentInfo = {
    network:          hre.network.name,
    deployer:         deployer.address,
    MissionFactory:   missionFactoryAddress,
    MissionImpl:      missionImplAddress,
    funded:           "10 CRO",
    timestamp:        new Date().toISOString()
  };

  const filePath = path.join(__dirname, `../deployments/${hre.network.name}.json`);
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(deploymentInfo, null, 2));
  console.log("📂 Deployment info saved to", filePath);

  console.log("🎉 All done!");
}

main().catch(err => {
  console.error(err);
  process.exitCode = 1;
});
