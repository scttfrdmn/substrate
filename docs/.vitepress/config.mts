import { defineConfig } from 'vitepress'

const siteUrl = 'https://scttfrdmn.github.io/substrate/'
const ogImage = siteUrl + 'og-image.png'
const description =
  'Substrate is an event-sourced AWS emulator for deterministic, offline testing ' +
  'of CloudFormation, CDK, Terraform, and any AWS SDK or CLI call — with time-travel ' +
  'debugging and cost visibility before you deploy.'

export default defineConfig({
  title: 'Substrate',
  description,
  lang: 'en-US',

  // Project Pages site at scttfrdmn.github.io/substrate/
  base: '/substrate/',

  // Emit sitemap.xml for search-engine crawling.
  sitemap: { hostname: siteUrl },

  // Use clean URLs (/getting-started instead of /getting-started.html).
  cleanUrls: true,

  head: [
    ['link', { rel: 'preconnect', href: 'https://fonts.googleapis.com' }],
    ['link', { rel: 'preconnect', href: 'https://fonts.gstatic.com', crossorigin: '' }],
    ['link', { href: 'https://fonts.googleapis.com/css2?family=Atkinson+Hyperlegible:ital,wght@0,400;0,700;1,400;1,700&display=swap', rel: 'stylesheet' }],

    ['link', { rel: 'icon', type: 'image/svg+xml', href: '/substrate/favicon.svg' }],

    // SEO + social cards
    ['meta', { name: 'author', content: 'scttfrdmn' }],
    ['meta', { name: 'keywords', content: 'AWS emulator, AWS testing, LocalStack alternative, CloudFormation testing, CDK testing, Terraform testing, infrastructure as code, event sourcing, deterministic testing, Go, boto3, mock AWS' }],
    ['link', { rel: 'canonical', href: siteUrl }],

    ['meta', { property: 'og:type', content: 'website' }],
    ['meta', { property: 'og:site_name', content: 'Substrate' }],
    ['meta', { property: 'og:title', content: 'Substrate — the test harness for AI-generated infrastructure' }],
    ['meta', { property: 'og:description', content: description }],
    ['meta', { property: 'og:url', content: siteUrl }],
    ['meta', { property: 'og:image', content: ogImage }],

    ['meta', { name: 'twitter:card', content: 'summary_large_image' }],
    ['meta', { name: 'twitter:title', content: 'Substrate — the test harness for AI-generated infrastructure' }],
    ['meta', { name: 'twitter:description', content: description }],
    ['meta', { name: 'twitter:image', content: ogImage }],
  ],

  themeConfig: {
    siteTitle: 'Substrate',

    nav: [
      { text: 'Getting Started', link: '/getting-started' },
      { text: 'Services', link: '/services' },
      { text: 'Testing', link: '/testing-guide' },
      { text: 'Scope', link: '/scope' },
      { text: 'GitHub', link: 'https://github.com/scttfrdmn/substrate', target: '_blank' },
    ],

    sidebar: [
      {
        text: 'Introduction',
        collapsed: false,
        items: [
          { text: 'What is Substrate?', link: '/' },
          { text: 'Scope & Philosophy', link: '/scope' },
        ],
      },
      {
        text: 'Using Substrate',
        collapsed: false,
        items: [
          { text: 'Getting Started', link: '/getting-started' },
          { text: 'Endpoint Configuration', link: '/endpoint-configuration' },
          { text: 'Testing Guide', link: '/testing-guide' },
        ],
      },
      {
        text: 'Reference',
        collapsed: false,
        items: [
          { text: 'Service Reference', link: '/services' },
        ],
      },
    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/scttfrdmn/substrate' },
    ],

    editLink: {
      pattern: 'https://github.com/scttfrdmn/substrate/edit/main/docs/:path',
      text: 'Edit this page on GitHub',
    },

    footer: {
      message: 'Released under the <a href="https://github.com/scttfrdmn/substrate/blob/main/LICENSE">Apache 2.0 License</a>.',
      copyright: 'Substrate — event-sourced AWS emulator',
    },

    search: { provider: 'local' },
  },
})
