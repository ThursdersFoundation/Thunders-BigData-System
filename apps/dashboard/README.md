# Thunders Dashboard

The web dashboard for the Thunders BigData System, built with Next.js 14,
React 18, TypeScript, Tailwind CSS, and Recharts.

## Features

- Real-time dashboard with WebSocket updates
- Dataset and pipeline management
- Analytics with charts (line, bar, pie, area)
- AI model registry, predictions, and evaluation
- System monitoring (CPU, memory, disk, services, alerts)
- User, role, and permission management
- Light / dark theme
- JWT authentication with refresh tokens

## Getting Started

```bash
cp .env.example .env.local
npm install
npm run dev
```

Open http://localhost:3000.

## Scripts

| Script              | Purpose                          |
| ------------------- | -------------------------------- |
| `npm run dev`       | Start dev server                 |
| `npm run build`     | Production build                 |
| `npm run start`     | Run production build             |
| `npm run lint`      | Lint with ESLint                 |
| `npm run test`      | Run Jest tests                   |
| `npm run type-check`| TypeScript check without emit    |

## Directory Layout

- `src/pages/` - Next.js page routes (file-system based)
- `src/components/` - Reusable UI components, grouped by domain
- `src/layouts/` - Page-level layouts (Dashboard, Auth, Blank)
- `src/hooks/` - Custom React hooks
- `src/services/` - API clients, auth, and websocket services
- `src/utils/` - Formatters, validators, helpers
- `src/styles/` - Global CSS, variables, component styles
- `src/types/` - TypeScript type definitions
- `src/context/` - React contexts (Auth, User, Theme)
- `src/config/` - App configuration (api, env, navigation)
- `src/assets/` - Images, icons, illustrations
- `tests/` - Jest tests for components, pages, hooks, services
- `public/` - Static assets served as-is

## Theming

The app supports light and dark themes via Tailwind's `dark:` variant.
The `ThemeContext` toggles a `dark` class on the `<html>` element.

## Authentication

JWTs are stored in HTTP-only cookies on the server side and refreshed
automatically via the refresh-token endpoint when they are about to
expire.
