// Dev-only: regenerate what pageAnchors is pinned to, from VitePress itself.
//
//   make docs-anchor-fixtures    (needs `npm ci` in docs/ first)
//
// Writes, next to this script:
//   headings.golden.json  the heading ids VitePress gives testdata/headings.md, in order,
//                         and every fragment the rendered page answers (id="…" on any
//                         element, name="…" on <a>);
// and, in the package directory:
//   emoji.json            the emoji shortcodes and emoticon shortcuts VitePress's emoji
//                         plugin replaces, which drop out of a heading's anchor.
//
// Run it again whenever docs/package.json moves VitePress, and commit what changes.
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const here = path.dirname(fileURLToPath(import.meta.url))
const docs = path.resolve(process.argv[2] ?? 'docs')
const nodeDir = path.join(docs, 'node_modules', 'vitepress', 'dist', 'node')
const vp = await import(pathToFileURL(path.join(nodeDir, 'index.js')).href)
const md = await vp.createMarkdownRenderer(docs, {}, '/')
const version = JSON.parse(fs.readFileSync(path.join(docs, 'node_modules', 'vitepress', 'package.json'), 'utf8')).version

const page = path.join(here, 'headings.md')
const src = fs.readFileSync(page, 'utf8')
const env = { path: page, relativePath: 'headings.md', cleanUrls: true }
const headings = md
  .parse(src, env)
  .filter((t) => t.type === 'heading_open')
  .map((t) => t.attrGet('id'))
const html = md.render(src, { ...env })
const ids = new Set()
for (const m of html.matchAll(/<[A-Za-z][\w-]*\s[^>]*?\bid="([^"]*)"/g)) ids.add(m[1])
for (const m of html.matchAll(/<[A-Za-z][\w-]*\s[^>]*?\bid='([^']*)'/g)) ids.add(m[1])
for (const m of html.matchAll(/<a\s[^>]*?\bname="([^"]*)"/g)) ids.add(m[1])
fs.writeFileSync(
  path.join(here, 'headings.golden.json'),
  JSON.stringify({ vitepress: version, headings, ids: [...ids].sort() }, null, 2) + '\n',
)

// The emoji plugin's tables are module-private in VitePress's bundle, so read them out of
// the chunk that declares them.
const chunk = fs
  .readdirSync(nodeDir)
  .map((f) => path.join(nodeDir, f))
  .find((f) => f.endsWith('.js') && fs.readFileSync(f, 'utf8').includes('var emojies_defs = {'))
if (!chunk) throw new Error(`no chunk under ${nodeDir} declares emojies_defs`)
const code = fs.readFileSync(chunk, 'utf8')
const object = (name) => {
  const start = code.indexOf(`var ${name} = {`)
  const end = code.indexOf('\n};', start)
  if (start < 0 || end < 0) throw new Error(`cannot find ${name} in ${chunk}`)
  return new Function(`return ${code.slice(start + `var ${name} = `.length, end + 2)}`)()
}
const defs = object('emojies_defs')
const shortcuts = []
for (const [name, aliases] of Object.entries(object('emojies_shortcuts'))) {
  if (!defs[name]) continue
  shortcuts.push(...(Array.isArray(aliases) ? aliases : [aliases]))
}
fs.writeFileSync(
  path.join(here, '..', 'emoji.json'),
  JSON.stringify({ vitepress: version, names: Object.keys(defs).sort(), shortcuts: shortcuts.sort() }) + '\n',
)
console.log(`headings.golden.json: ${headings.length} headings, ${ids.size} ids; emoji.json: ${Object.keys(defs).length} names, ${shortcuts.length} shortcuts`)
