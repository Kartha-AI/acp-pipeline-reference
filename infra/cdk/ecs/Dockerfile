# ECS Fargate task image for the extractor.
#
# Two-stage build: pnpm install + tsc in builder, then a slim runtime image
# with only the compiled JS and the production node_modules. Built and pushed
# to ECR by CDK's `ContainerImage.fromAsset()` — no manual docker steps.

FROM node:20-bookworm-slim AS builder
ENV PNPM_HOME=/pnpm
ENV PATH=$PNPM_HOME:$PATH
RUN corepack enable

WORKDIR /repo
# Copy the entire monorepo. The builder image is throwaway; the final image
# only carries the bundled output.
COPY . /repo

RUN pnpm install --frozen-lockfile --prefer-offline
RUN pnpm -r build

# Bundle the ECS entry into a single self-contained JS file using esbuild.
# Native modules (pg) stay external and are reinstalled in the runtime stage.
RUN npx --yes esbuild infra/cdk/ecs/extractor.ts \
      --bundle \
      --platform=node \
      --target=node20 \
      --format=esm \
      --outfile=/repo/dist/ecs-extractor.mjs \
      --external:pg \
      --external:pg-cursor \
      --external:pg-copy-streams \
      --external:pino \
      --external:thread-stream \
      --external:@aws-sdk/*

FROM node:20-bookworm-slim AS runtime
WORKDIR /app

# Reinstall just the runtime deps that we kept external.
COPY --from=builder /repo/dist/ecs-extractor.mjs /app/extractor.mjs
RUN echo '{"name":"acp-extractor","version":"0.0.0","type":"module","dependencies":{"pg":"^8.11.5","pg-cursor":"^2.10.5","pg-copy-streams":"^6.0.6","pino":"^9.2.0","@aws-sdk/client-secrets-manager":"^3.658.0","@aws-sdk/client-sqs":"^3.658.0"}}' > /app/package.json \
    && npm install --omit=dev --no-audit --no-fund

USER node
ENV NODE_ENV=production
ENTRYPOINT ["node", "/app/extractor.mjs"]
