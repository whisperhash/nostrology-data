# cashu-redeemer (arm64) - the Cashu redemption sidecar. Reached
# ONLY by the nostrology-mcp container over the compose network (plus
# Prometheus for /metrics); NEVER published to a host port. Build context =
# cashu_redeemer/, as the deployment builds it.
# base-pin: node:22-slim resolved 2026-10-26
# Pinned by OCI index digest, so the box's pull is content-addressed (the Docker daemon
# verifies the registry against its own CA store) and a rebuild is reproducible. The tag
# beside the digest is decorative - Docker resolves the digest and ignores it - so a tag bump
# needs a re-resolve and a new base-pin line in the same commit. The Node 22 line, whose compiled-in root set dropped the
# roots compromised in the 2026-10 eMudhra/emSign incident (Node 20 is end-of-life
# and its set is frozen holding them).
FROM node:22-slim@sha256:de170b39439eb0e5a3444ad7d60b015614a381e10fecae2e30ec9e735122f38d AS build
WORKDIR /app
COPY package.json package-lock.json tsconfig.json ./
RUN npm ci
COPY src ./src
RUN npx tsc

# base-pin: node:22-slim resolved 2026-10-26 (the same digest as the build stage
# above; both stages must be the same image or the runtime runs a different Node
# from the one the build produced against).
FROM node:22-slim@sha256:de170b39439eb0e5a3444ad7d60b015614a381e10fecae2e30ec9e735122f38d
WORKDIR /app
ENV NODE_ENV=production
COPY package.json package-lock.json ./
# Runtime deps only (cashu-ts + the AWS SDK); tsc and the type packages stay
# in the build stage.
RUN npm ci --omit=dev && npm cache clean --force
COPY --from=build /app/dist ./dist
# Non-root: the container holds bearer instruments in memory; no reason for
# uid 0. node:slim ships the `node` user.
USER node
EXPOSE 8081
CMD ["node", "dist/server.js"]
