# Running the stack with wslc (WSL Containers)

This file mirrors the docker-compose workflow for environments without Docker
Desktop. Requires `wslc.exe` (v2.9.3+) installed and the Linux kernel set up
for WSL container hosting.

## Prerequisites

- `wslc.exe` on PATH
- The two build contexts next to each other:

```
E:\Repos\
├── arkiv-quickwit-indexer     ← bridge source (build context ..)
│   └── demo-docker-stack/     ← this folder
└── quickwit/                  ← quickwit fork with ipfs backend (context ../quickwit)
```

## Quick start

Run from **PowerShell as administrator** (required for the first network
creation — the network persists after that).

### 1. Set up infrastructure (one-time)

```powershell
wslc network create arkiv-stack
wslc volume create ipfs-data
wslc volume create qw-data
wslc volume create bridge-state
```

### 2. Pull base images (one-time)

```powershell
wslc pull ipfs/kubo:v0.32.1
wslc pull nginx:1.27-alpine
wslc pull curlimages/curl:8.10.1
```

### 3. Build custom images (one-time, or after code changes)

```powershell
wslc build -t arkiv-stack/quickwit:latest  ../quickwit
wslc build -t arkiv-stack/bridge:latest     ..
wslc build -t arkiv-stack/frontend:latest  ./frontend
```

Building Quickwit from source takes 30–60+ minutes. Skip if images already
exist from a previous `docker compose up --build`.

### 4. Start kubo (offline IPFS)

```powershell
wslc run --detach `
  --network arkiv-stack --network-alias kubo `
  --volume ipfs-data:/data/ipfs `
  --env IPFS_PROFILE=test `
  --name arkiv-kubo `
  ipfs/kubo:v0.32.1 daemon --migrate=true --offline

# Wait for it to be healthy:
:loop
wslc exec arkiv-kubo ipfs id >nul 2>&1 && echo kubo is ready || (timeout /t 2 >nul & goto loop)
```

### 5. Start quickwit

```powershell
wslc run --detach `
  --network arkiv-stack --network-alias quickwit `
  --volume E:\Repos\arkiv-quickwit-indexer\demo-docker-stack\quickwit-config\quickwit.yaml:/quickwit/config/quickwit.yaml:ro `
  --volume qw-data:/quickwit/qwdata `
  --env QW_CONFIG=/quickwit/config/quickwit.yaml `
  --name arkiv-quickwit `
  arkiv-stack/quickwit:latest run
```

Wait ~20 seconds for it to start accepting connections:

```powershell
:loop
wslc exec arkiv-quickwit bash -c "exec 3<>/dev/tcp/127.0.0.1/7280 && printf 'GET /health/livez HTTP/1.0\r\n\r\n' >&3 && grep -q 200 <&3" >nul 2>&1 && echo quickwit ready || (timeout /t 2 >nul & goto loop)
```

### 6. Create the arkiv index (one-shot)

```powershell
wslc run --rm `
  --network arkiv-stack `
  --volume E:\Repos\arkiv-quickwit-indexer\demo-docker-stack\quickwit-config\arkiv-index.yaml:/arkiv-index.yaml:ro `
  curlimages/curl:8.10.1 `
  -s -XPOST http://quickwit:7280/api/v1/indexes `
  -H 'content-type: application/yaml' `
  --data-binary `@/arkiv-index.yaml
```

Note the backtick before `@` to prevent PowerShell from interpreting it.

### 7. Start the bridge

```powershell
wslc run --detach `
  --network arkiv-stack --network-alias bridge `
  --volume bridge-state:/var/lib/arkiv-quickwit-bridge `
  --volume E:\Repos\arkiv-quickwit-indexer\demo-docker-stack\bridge-config\config.yaml:/etc/arkiv-quickwit-bridge/config.yaml:ro `
  --env RUST_LOG=info `
  --name arkiv-bridge `
  arkiv-stack/bridge:latest
```

### 8. Start the frontend (search page)

```powershell
wslc run --detach `
  --network arkiv-stack --network-alias frontend `
  -p 127.0.0.1:8080:8080 `
  --name arkiv-frontend `
  arkiv-stack/frontend:latest
```

Open **http://localhost:8080**.

## Stop / restart

```powershell
# Stop a single container:
wslc stop arkiv-frontend

# Remove (to restart fresh):
wslc rm arkiv-frontend

# Check all containers:
wslc container ls

# View logs:
wslc logs arkiv-bridge --tail 20
```

## Notes

- All containers use the `arkiv-stack` wslc network — no docker compose network.
- `wslc run -p` only works as a port publish for the initial `run` call. If the
  container is removed and recreated, you must pass `-p` again.
- PowerShell's `@` character must be escaped with a backtick when passing a
  `--data-binary @file` argument.
- This setup mirrors `docker compose up -d --build` exactly: same images, same
  volumes, same network aliases.
