// scripts/deploy.js
const hre  = require("hardhat");
const fs   = require("fs");
const path = require("path");

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function verifyWithRetry(address, constructorArguments = [], label = "", tries = 5) {
  for (let i = 1; i <= tries; i++) {
    try {
      console.log(`🔎 Verifying ${label || address} (attempt ${i}/${tries})…`);
      await hre.run("verify:verify", { address, constructorArguments });
      console.log(`✅ Verified ${label || address}`);
      return;
    } catch (e) {
      const msg = String(e?.message || e);
      if (msg.match(/Already Verified/i)) {
        console.log(`✅ Already verified: ${label || address}`);
        return;
      }
      if (i === tries) throw e;
      console.log(`⏳ Explorer not ready yet. Waiting before retry…`);
      await sleep(15_000);
    }
  }
}

async function main() {
  console.log("🚀 Starting full project deployment…");

  const [deployer] = await hre.ethers.getSigners();
  console.log("👤 Deployer:", deployer.address);
  const balance = await hre.ethers.provider.getBalance(deployer.address);
  console.log("💰 Balance:", hre.ethers.formatEther(balance), "CRO");
  if (hre.ethers.formatEther(balance) < 10) {
    console.log("Keep at least 10 CRO in the wallet");
    return;
  }

  /* 1) Deploy Mission implementation (once) */
  const Mission = await hre.ethers.getContractFactory("Mission");
  const missionImpl = await Mission.deploy({ gasLimit: 12_000_000 });
  await missionImpl.waitForDeployment();
  const missionImplAddress = await missionImpl.getAddress();
  console.log("🧩 Mission implementation deployed at:", missionImplAddress);

  // wait a few confirmations so the explorer indexes it
  await missionImpl.deploymentTransaction().wait(5);

  /* 2) Deploy MissionFactory, passing the impl address */
  const nextNonce = await hre.ethers.provider.getTransactionCount(deployer.address, "latest");
  const Factory = await hre.ethers.getContractFactory("MissionFactory");
  const missionFactory = await Factory.deploy(
    missionImplAddress,
    { gasLimit: 4_000_000, nonce: nextNonce }
  );
  await missionFactory.waitForDeployment();
  const missionFactoryAddress = await missionFactory.getAddress();
  console.log("🏗️  MissionFactory deployed at:", missionFactoryAddress);

  await missionFactory.deploymentTransaction().wait(5);

  /* 3) Sanity-check the public getter */
  const getterValue = await missionFactory.missionImplementation();
  console.log("🔍 missionImplementation() returns:", getterValue);

  /* 4) (Optional) Fund the factory with 10 CRO */
  //console.log("💸 Funding MissionFactory with 10 CRO…");
  //const fundTx = await deployer.sendTransaction({
  //  to: missionFactoryAddress,
  //  value: hre.ethers.parseEther("10")
  //});
  //await fundTx.wait();
  //console.log("✅ Funding complete!");

  /* 5) Persist deployment info */
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

  /* 6) Verify on Cronoscan via hardhat-etherscan */
  // tiny grace period for explorer indexing
  await sleep(10_000);

  await verifyWithRetry(missionImplAddress, [], "Mission implementation");
  await verifyWithRetry(missionFactoryAddress, [missionImplAddress], "MissionFactory");

  console.log("🎉 All done!");
}

main().catch(err => {
  console.error(err);
  process.exitCode = 1;
});
