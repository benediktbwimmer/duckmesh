# DuckMesh Web Console

Frontend stack:

- React + TypeScript
- Tailwind CSS
- Monaco editor (`@monaco-editor/react`)
- TanStack Query + TanStack Table
- React Router
- zod + react-hook-form

## Development

```bash
npm install
npm run dev
```

## Production build

```bash
npm run build
```

Build output is emitted to `../internal/api/uistatic/app` and served by the Go API.
