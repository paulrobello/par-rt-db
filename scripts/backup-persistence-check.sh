#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

# Render Compose with a blank env file so local deployment secrets cannot leak
# into output or influence this canonical configuration check.
RTDB_BACKUP_ENABLED= RTDB_BACKUP_DIR= RTDB_BACKUP_RETENTION= \
POSTGRES_PASSWORD=check RTDB_ADMIN_KEY=check \
  docker compose --file docker-compose.yml --env-file /dev/null config --format json |
  node --input-type=commonjs -e '
    let input = "";
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", (chunk) => { input += chunk; });
    process.stdin.on("end", () => {
      const config = JSON.parse(input);
      const server = config.services?.server;
      const volumes = server?.volumes ?? [];
      const mount = volumes.find((volume) => volume.target === "/backups");
      const tmpfs = server?.tmpfs ?? [];
      const tmpfsPaths = tmpfs.map((entry) =>
        typeof entry === "string" ? entry.split(":", 1)[0] : entry.target ?? entry.path,
      );
      const fail = (message) => {
        console.error(`backup-persistence-check: ${message}`);
        process.exitCode = 1;
      };
      if (!mount || mount.type !== "volume" || mount.source !== "rtdb-backups")
        fail("server /backups must use the rtdb-backups named volume");
      if (!Object.hasOwn(config.volumes ?? {}, "rtdb-backups"))
        fail("rtdb-backups must be declared as a Compose volume");
      if (server?.environment?.RTDB_BACKUP_DIR !== "/backups")
        fail("Compose RTDB_BACKUP_DIR must default to /backups");
      if (server?.environment?.RTDB_BACKUP_ENABLED !== "false")
        fail("managed backups must remain disabled by default");
      if (server?.environment?.RTDB_BACKUP_RETENTION !== "7")
        fail("managed backup retention must remain seven dumps by default");
      if (tmpfsPaths.includes("/backups"))
        fail("/backups must not be tmpfs");
      if (!tmpfsPaths.includes("/tmp")) fail("/tmp must remain tmpfs");
      if (server?.read_only !== true) fail("server root filesystem must stay read-only");
      if (!server?.cap_drop?.includes("ALL")) fail("server must drop all capabilities");
      if ((config.services?.postgres?.ports ?? []).length !== 0)
        fail("PostgreSQL must not publish host ports");
      const dockerfile = require("node:fs").readFileSync("Dockerfile", "utf8");
      if (!dockerfile.includes("install -d -o rtdb -g rtdb -m 0700 /backups"))
        fail("image must create mode-0700 /backups for the non-root server user");
      if (!process.exitCode) console.log("backup-persistence-check: ok");
    });
  '
