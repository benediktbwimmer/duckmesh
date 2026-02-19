import { NavLink, Route, Routes } from 'react-router-dom'
import { useMemo, useState } from 'react'
import { ConsolePage } from './pages/ConsolePage'
import type { ConnectionSettings } from './lib/types'

const defaultSettings: ConnectionSettings = {
  baseUrl: localStorage.getItem('duckmesh.baseUrl') || window.location.origin,
  apiKey: localStorage.getItem('duckmesh.apiKey') || '',
  tenantId: localStorage.getItem('duckmesh.tenantId') || 'tenant-dev',
}

function persistSettings(next: ConnectionSettings) {
  localStorage.setItem('duckmesh.baseUrl', next.baseUrl)
  localStorage.setItem('duckmesh.apiKey', next.apiKey)
  localStorage.setItem('duckmesh.tenantId', next.tenantId)
}

export default function App() {
  const [settings, setSettings] = useState<ConnectionSettings>(defaultSettings)

  const navClass = useMemo(
    () =>
      ({ isActive }: { isActive: boolean }) =>
        `rounded-full px-3 py-1 text-sm font-semibold transition ${
          isActive ? 'bg-ink text-white' : 'bg-white/70 text-slate-700 hover:bg-white'
        }`,
    [],
  )

  return (
    <div className="min-h-screen">
      <div className="mx-auto flex w-full max-w-[1300px] items-center justify-between px-4 py-4 md:px-8">
        <p className="font-mono text-sm uppercase tracking-[0.24em] text-slate-600">DuckMesh UI</p>
        <nav className="flex gap-2 rounded-full border border-white/80 bg-white/65 p-1 shadow-card backdrop-blur">
          <NavLink className={navClass} to="/">
            Console
          </NavLink>
          <NavLink className={navClass} to="/about">
            About
          </NavLink>
        </nav>
      </div>

      <Routes>
        <Route
          element={
            <ConsolePage
              onSettingsChange={(next) => {
                setSettings(next)
                persistSettings(next)
              }}
              settings={settings}
            />
          }
          path="/"
        />
        <Route
          element={
            <div className="mx-auto mt-6 w-full max-w-[1100px] rounded-2xl border border-white/70 bg-white/80 p-6 shadow-card">
              <h1 className="text-2xl font-bold text-ink">About This Console</h1>
              <p className="mt-2 text-slate-700">
                Built with React, TypeScript, Tailwind, Monaco, TanStack Query/Table, and zod forms.
                Natural-language query translation runs on backend to keep model credentials server-side.
              </p>
            </div>
          }
          path="/about"
        />
      </Routes>
    </div>
  )
}
