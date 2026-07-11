import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import path from 'node:path'
import fs from 'node:fs'

const rootDir = fs.realpathSync.native(process.cwd())

const BACKEND_TARGET = process.env.VITE_BACKEND_URL ?? 'http://localhost:8418'

export default defineConfig({
  root: rootDir,
  plugins: [react(), tailwindcss()],
  build: {
    rollupOptions: {
      input: {
        index: path.resolve(rootDir, 'index.html'),
      },
    },
  },
  resolve: {
    alias: {
      '@': path.resolve(rootDir, '.'),
    },
  },
  server: {
    port: 3010,
    strictPort: true,
    proxy: {
      '/api':     { target: BACKEND_TARGET, changeOrigin: true },
      '/healthz': { target: BACKEND_TARGET, changeOrigin: true },
    },
  },
})
