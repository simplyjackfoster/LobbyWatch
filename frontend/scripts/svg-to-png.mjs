import { chromium } from 'playwright';
import { fileURLToPath } from 'url';
import path from 'path';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const svgPath = path.resolve(__dirname, '../public/og-image.svg');
const outPath = path.resolve(__dirname, '../public/og-image.png');

const browser = await chromium.launch();
const page = await browser.newPage();
await page.setViewportSize({ width: 1200, height: 630 });
await page.goto(`file://${svgPath}`);
await page.screenshot({ path: outPath, clip: { x: 0, y: 0, width: 1200, height: 630 } });
await browser.close();
console.log('Written:', outPath);
