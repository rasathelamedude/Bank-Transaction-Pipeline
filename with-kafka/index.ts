import { spawn } from "child_process";
import path from "path";

const services = path.join(__dirname, "services");
const venvPython = path.join(
  services,
  "fraud-detector",
  "venv",
  "Scripts",
  "python.exe",
);

function spawnService(label: string, cmd: string, args: string[], cwd: string) {
  const proc = spawn(cmd, args, { cwd, stdio: "pipe" });

  proc.stdout.on("data", (data) => {
    const lines = data.toString().trim().split("\n");
    for (const line of lines) {
      if (line.includes('"level"') || line.includes('"logger":"kafkajs"'))
        continue;
      if (line.trim()) console.log(`[${label}] ${line.trim()}`);
    }
  });

  proc.stderr.on("data", (data) => {
    const lines = data.toString().trim().split("\n");
    for (const line of lines) {
      // Suppress kafkajs noise
      if (
        line.includes("KafkaJS") ||
        line.includes("TimeoutNegative") ||
        line.includes("kafkajs") ||
        line.includes("correlationId")
      )
        continue;
      if (line.trim()) console.error(`[${label}] ${line.trim()}`);
    }
  });

  proc.on("exit", (code) => {
    if (code !== 0 && code !== null) {
      console.error(`[${label}] exited with code ${code}`);
    }
  });

  return proc;
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  console.log("Starting WireFlow...\n");

  console.log("Starting consumers...");

  spawnService(
    "BALANCE-UPDATER",
    "bun",
    ["run", path.join(services, "balance-updater", "index.ts")],
    __dirname,
  );

  spawnService(
    "NOTIFIER",
    "bun",
    ["run", path.join(services, "notifier", "index.ts")],
    __dirname,
  );

  spawnService(
    "AUDIT-LOGGER",
    "bun",
    ["run", path.join(services, "audit-logger", "index.ts")],
    __dirname,
  );

  spawnService(
    "FRAUD-DETECTOR",
    venvPython,
    ["main.py"],
    path.join(services, "fraud-detector"),
  );

  // Give consumers time to connect to Kafka before producer fires
  console.log("Waiting for consumers to connect...\n");
  await sleep(3000);

  console.log("Publishing transactions...\n");

  spawnService(
    "PRODUCER",
    "bun",
    ["run", path.join(services, "producer", "index.ts")],
    __dirname,
  );
}

main();
