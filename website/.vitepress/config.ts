import { defineConfig } from 'vitepress'

const REPO = 'https://gitlab.com/lemarco/trembita'
const REPO_TREE = `${REPO}/-/tree/main`

export default defineConfig({
  title: 'trembita',
  description:
    'A distributed Raft + actor framework for Rust — one codebase, N nodes, elastic and self-healing.',
  lang: 'en-US',
  cleanUrls: true,
  lastUpdated: true,
  appearance: 'dark',

  head: [
    ['link', { rel: 'icon', href: '/favicon.svg', type: 'image/svg+xml' }],
    [
      'meta',
      {
        name: 'theme-color',
        content: '#0b0f17',
      },
    ],
    [
      'meta',
      {
        property: 'og:title',
        content: 'trembita — distributed Raft + actors for Rust',
      },
    ],
    [
      'meta',
      {
        property: 'og:description',
        content:
          'Embed consensus, actors, jobs, and durable workflows in your binary. No mandatory Redis or Kubernetes.',
      },
    ],
  ],

  themeConfig: {
    logo: '/logo.svg',
    siteTitle: 'trembita',

    nav: [
      { text: 'Guide', link: '/guide/getting-started', activeMatch: '/guide/' },
      { text: 'Scenarios', link: '/scenarios/', activeMatch: '/scenarios/' },
      { text: 'Examples', link: '/examples' },
      { text: 'Reference', link: '/reference/status', activeMatch: '/reference/' },
      { text: 'Design', link: '/design/deployment-model', activeMatch: '/design/' },
      { text: 'Changelog', link: '/changelog' },
      {
        text: 'crates.io',
        link: 'https://crates.io/crates/trembita',
      },
    ],

    sidebar: {
      '/guide/': [
        {
          text: 'Guide',
          items: [
            { text: 'Getting started', link: '/guide/getting-started' },
          ],
        },
      ],
      '/scenarios/': [
        {
          text: 'Scenarios',
          items: [
            { text: 'Overview', link: '/scenarios/' },
            { text: 'Background jobs', link: '/scenarios/background-jobs' },
            { text: 'Event topics', link: '/scenarios/event-topics' },
            { text: 'Stateful workers', link: '/scenarios/stateful-workers' },
            { text: 'Real-time sessions', link: '/scenarios/realtime-sessions' },
            { text: 'Workflows', link: '/scenarios/workflows' },
            { text: 'State placement', link: '/scenarios/state-placement' },
          ],
        },
      ],
      '/reference/': [
        {
          text: 'Reference',
          items: [
            { text: 'Status', link: '/reference/status' },
            { text: 'Architecture', link: '/reference/architecture' },
            { text: 'Protocol', link: '/reference/protocol' },
            { text: 'Certificates', link: '/reference/certs' },
          ],
        },
      ],
      '/design/': [
        {
          text: 'Design records',
          items: [
            { text: 'Deployment model', link: '/design/deployment-model' },
            { text: 'Product scenarios', link: '/design/product-scenarios' },
            { text: 'Job queue', link: '/design/job-queue' },
            { text: 'Event topics', link: '/design/event-topics' },
            { text: 'Gateway identity', link: '/design/gateway-identity' },
            { text: 'Multi-Raft', link: '/design/multi-raft' },
            { text: 'Upgrade coordinator', link: '/design/upgrade-coordinator' },
            { text: 'Wire protocol', link: '/design/wire-protocol' },
            { text: 'Security', link: '/design/security' },
            { text: 'Library & publishing', link: '/design/library-and-publishing' },
          ],
        },
      ],
      '/changelog': [
        {
          text: 'Changelog',
          items: [{ text: 'All releases', link: '/changelog' }],
        },
      ],
    },

    socialLinks: [
      { icon: 'gitlab', link: REPO },
      { icon: 'github', link: 'https://docs.rs/trembita' },
    ],

    search: {
      provider: 'local',
    },

    outline: {
      level: [2, 3],
    },

    footer: {
      message: 'MIT OR Apache-2.0',
      copyright: 'trembita contributors',
    },

    editLink: {
      pattern: `${REPO}/-/edit/main/website/:path`,
      text: 'Edit this page on GitLab',
    },
  },

  markdown: {
    lineNumbers: true,
  },

  // Synced docs link to GitLab source trees and external ADR cross-refs.
  ignoreDeadLinks: true,
})
