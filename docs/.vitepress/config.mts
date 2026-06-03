import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'Substrate',
  description: 'Event-sourced AWS emulator for deterministic testing of infrastructure code.',
  lang: 'en-US',

  // Project Pages site at scttfrdmn.github.io/substrate/
  base: '/substrate/',

  head: [
    ['link', { rel: 'preconnect', href: 'https://fonts.googleapis.com' }],
    ['link', { rel: 'preconnect', href: 'https://fonts.gstatic.com', crossorigin: '' }],
    ['link', { href: 'https://fonts.googleapis.com/css2?family=Atkinson+Hyperlegible:ital,wght@0,400;0,700;1,400;1,700&display=swap', rel: 'stylesheet' }],
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
