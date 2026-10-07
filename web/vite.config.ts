import react from "@vitejs/plugin-react";
import { defineConfig, loadEnv } from "vite";

// GitHub Pages serves the repo under /wikipulse/; FastAPI and `vite dev` serve it at /.
export default defineConfig(({ mode }) => ({
  base: loadEnv(mode, ".", "WIKIPULSE_").WIKIPULSE_BASE || "/",
  plugins: [react()],
  server: { proxy: { "/api": "http://localhost:8000" } },
}));
