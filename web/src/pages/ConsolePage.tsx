import { useMemo, useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import { z } from 'zod'
import { zodResolver } from '@hookform/resolvers/zod'
import { useForm } from 'react-hook-form'
import { fetchUISchema, runQuery, translateQuery } from '../lib/api'
import type { ConnectionSettings, QueryResponse } from '../lib/types'
import { QueryEditor } from '../components/QueryEditor'
import { ResultTable } from '../components/ResultTable'

type Props = {
  settings: ConnectionSettings
  onSettingsChange: (settings: ConnectionSettings) => void
}

const settingsSchema = z.object({
  baseUrl: z.string().url(),
  apiKey: z.string(),
  tenantId: z.string().min(1),
})

type SettingsForm = z.infer<typeof settingsSchema>

const starterSQL = `SELECT table_name, COUNT(*) AS files\nFROM (\n  SELECT table_name FROM events\n) t\nGROUP BY table_name\nLIMIT 50`

export function ConsolePage({ settings, onSettingsChange }: Props) {
  const [sql, setSql] = useState(starterSQL)
  const [result, setResult] = useState<QueryResponse | null>(null)

  const schemaQuery = useQuery({
    queryKey: ['ui-schema', settings.baseUrl, settings.tenantId, settings.apiKey],
    queryFn: () => fetchUISchema(settings),
  })

  const executeMutation = useMutation({
    mutationFn: (nextSQL: string) => runQuery(settings, nextSQL),
    onSuccess: (response) => setResult(response),
  })

  const translateMutation = useMutation({
    mutationFn: (prompt: string) => translateQuery(settings, prompt),
    onSuccess: (response) => setSql(response.sql),
  })

  const form = useForm<SettingsForm>({
    resolver: zodResolver(settingsSchema),
    defaultValues: settings,
  })

  const promptForm = useForm<{ prompt: string }>({
    defaultValues: { prompt: '' },
  })

  const schemaTables = schemaQuery.data?.tables ?? []

  const statusLine = useMemo(() => {
    if (executeMutation.isPending) {
      return 'Running query...'
    }
    if (executeMutation.error) {
      return (executeMutation.error as Error).message
    }
    if (!result) {
      return 'No query executed yet.'
    }
    return `snapshot=${result.snapshot_id} files=${result.stats?.scanned_files ?? 0} rows=${result.rows.length}`
  }, [executeMutation.error, executeMutation.isPending, result])

  return (
    <div className="mx-auto w-full max-w-[1300px] animate-fade-rise px-4 py-6 md:px-8">
      <header className="mb-6 grid gap-4 rounded-2xl border border-white/70 bg-white/75 p-5 shadow-card backdrop-blur md:grid-cols-[1.6fr_1fr]">
        <div>
          <p className="text-xs font-mono uppercase tracking-[0.18em] text-sea">DuckMesh Console</p>
          <h1 className="mt-1 text-3xl font-bold text-ink">Query with SQL or Natural Language</h1>
          <p className="mt-2 text-sm text-slate-600">
            Monaco editor with table-aware autocomplete, plus AI-assisted SQL generation for DuckDB.
          </p>
        </div>
        <form
          className="grid gap-2"
          onSubmit={form.handleSubmit((value) => {
            onSettingsChange(value)
          })}
        >
          <input className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm" placeholder="API base URL" {...form.register('baseUrl')} />
          <input className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm" placeholder="Tenant ID" {...form.register('tenantId')} />
          <input className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm" placeholder="API key (optional)" {...form.register('apiKey')} />
          {form.formState.errors.baseUrl && <p className="text-xs text-red-600">{form.formState.errors.baseUrl.message}</p>}
          {form.formState.errors.tenantId && <p className="text-xs text-red-600">{form.formState.errors.tenantId.message}</p>}
          <button className="rounded-lg bg-sea px-3 py-2 text-sm font-semibold text-white transition hover:bg-[#0b6e70]" type="submit">
            Save Connection
          </button>
        </form>
      </header>

      <div className="grid gap-4 lg:grid-cols-[2fr_1fr]">
        <section className="rounded-2xl border border-white/70 bg-white/80 p-4 shadow-card backdrop-blur">
          <div className="mb-3 flex flex-wrap items-center gap-2">
            <button
              className="rounded-lg bg-ink px-3 py-2 text-sm font-semibold text-white transition hover:bg-[#1b2a35]"
              onClick={() => executeMutation.mutate(sql)}
              type="button"
            >
              Run Query
            </button>
            <span className="text-xs font-mono text-slate-500">{statusLine}</span>
          </div>

          <QueryEditor sql={sql} onChange={setSql} tables={schemaTables} />

          <div className="mt-4">
            <ResultTable columns={result?.columns ?? []} rows={result?.rows ?? []} />
          </div>
        </section>

        <aside className="rounded-2xl border border-white/70 bg-white/80 p-4 shadow-card backdrop-blur">
          <h2 className="text-lg font-semibold text-ink">AI SQL Assistant</h2>
          <p className="mt-1 text-sm text-slate-600">
            Ask in natural language. We send schema and sample rows to backend translation.
          </p>

          <form
            className="mt-3 grid gap-3"
            onSubmit={promptForm.handleSubmit((value) => translateMutation.mutate(value.prompt))}
          >
            <textarea
              className="min-h-40 rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm"
              placeholder="Example: Show top 20 users by total purchase amount in the last 7 days"
              {...promptForm.register('prompt', { required: true })}
            />
            <button
              className="rounded-lg bg-ember px-3 py-2 text-sm font-semibold text-white transition hover:bg-[#bf5e36]"
              disabled={translateMutation.isPending}
              type="submit"
            >
              {translateMutation.isPending ? 'Generating...' : 'Generate SQL'}
            </button>
            {translateMutation.error && (
              <p className="text-xs text-red-600">{(translateMutation.error as Error).message}</p>
            )}
            {translateMutation.data && (
              <p className="text-xs text-slate-500">
                Generated by {translateMutation.data.provider} / {translateMutation.data.model}
              </p>
            )}
          </form>

          <div className="mt-6 rounded-xl border border-slate-200 bg-slate-50 p-3">
            <p className="text-xs font-mono uppercase tracking-[0.18em] text-slate-600">Autocomplete Context</p>
            <ul className="mt-2 space-y-1 text-sm text-slate-700">
              {schemaTables.slice(0, 8).map((table) => (
                <li key={table.table_name}>
                  <span className="font-semibold">{table.table_name}</span>{' '}
                  <span className="text-xs text-slate-500">({table.columns.length} columns)</span>
                </li>
              ))}
              {!schemaTables.length && <li className="text-xs text-slate-500">No tables found for tenant.</li>}
            </ul>
          </div>
        </aside>
      </div>
    </div>
  )
}
