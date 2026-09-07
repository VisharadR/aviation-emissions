import { cpSync, mkdirSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const stage = resolve(root, '.pages-site');
const api = process.env.NEXT_PUBLIC_LIVE_API_URL;
if (!api || new URL(api).protocol !== 'https:') {
  throw new Error('Set NEXT_PUBLIC_LIVE_API_URL to the public HTTPS live backend origin before deploying.');
}
mkdirSync(resolve(stage, 'app/live'), { recursive: true });
for (const file of ['layout.js', 'globals.css']) cpSync(resolve(root, 'app', file), resolve(stage, 'app', file));
cpSync(resolve(root, 'app/live'), resolve(stage, 'app/live'), { recursive: true });
cpSync(resolve(root, 'public'), resolve(stage, 'public'), { recursive: true });
cpSync(resolve(root, 'postcss.config.mjs'), resolve(stage, 'postcss.config.mjs'));
cpSync(resolve(root, 'package.json'), resolve(stage, 'package.json'));
writeFileSync(resolve(stage, 'app/page.js'), 'export { default, metadata } from "./live/page";\n');
writeFileSync(resolve(stage, 'next.config.mjs'), `export default ${JSON.stringify({output:'export',trailingSlash:true,basePath:process.env.PAGES_BASE_PATH || '',images:{unoptimized:true}})};\n`);
const result = spawnSync(process.execPath, [resolve(root, 'node_modules/next/dist/bin/next'), 'build', stage], {
  cwd: root, stdio: 'inherit', env: { ...process.env, NEXT_PUBLIC_LIVE_ONLY: 'true' },
});
if (result.status !== 0) process.exit(result.status || 1);
writeFileSync(resolve(stage, 'out/.nojekyll'), '');
