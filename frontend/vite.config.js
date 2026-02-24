import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';

export default defineConfig(({ mode }) => {
  const rootDir = __dirname;
  const outputDir = path.resolve(rootDir, '../dashboard/static/frontend');

  return {
    plugins: [react()],
    root: rootDir,
    base: '/static/frontend/',
    build: {
      outDir: outputDir,
      emptyOutDir: true,
      manifest: true,
      rollupOptions: {
        input: path.resolve(rootDir, 'index.html')
      }
    },
    server: {
      port: 5173,
      strictPort: true
    }
  };
});
