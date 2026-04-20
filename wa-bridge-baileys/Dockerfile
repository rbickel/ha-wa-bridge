# Build stage – compile native modules (libsignal, etc.) then discard tools
FROM node:22-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 make g++ git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY package*.json ./

# legacy-peer-deps: sharp is an optional peer dep of Baileys; we don't need it
RUN npm install --legacy-peer-deps

# ─────────────────────────────────────────────────────────────────────────────
# Runtime stage – no build toolchain, no Chromium, no extra fonts
FROM node:22-slim

WORKDIR /usr/src/app

COPY --from=builder /usr/src/app/node_modules ./node_modules
COPY package*.json index.js run.sh ./

RUN chmod +x run.sh

EXPOSE 3000

CMD ["./run.sh"]
