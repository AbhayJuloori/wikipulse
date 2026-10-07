FROM node:22.20.0-slim AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM python:3.11.13-slim
WORKDIR /app
COPY pyproject.toml /app/
COPY src /app/src
RUN pip install --no-cache-dir /app
COPY dashboard /app/dashboard
COPY --from=web /web/dist /app/web/dist
ENV WEB_DIST=/app/web/dist LEGACY_DASHBOARD=/app/dashboard/index.html
