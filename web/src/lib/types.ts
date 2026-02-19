export type ConnectionSettings = {
  baseUrl: string
  apiKey: string
  tenantId: string
}

export type UISchemaTable = {
  table_name: string
  columns: string[]
  sample_rows: unknown[][]
}

export type UISchemaResponse = {
  tenant_id: string
  tables: UISchemaTable[]
  snapshot_id?: number
  snapshot_time?: string
  max_visibility_token?: number
}

export type QueryResponse = {
  columns: string[]
  rows: unknown[][]
  snapshot_id: number
  snapshot_time: string
  max_visibility_token: number
  stats?: {
    duration_ms?: number
    scanned_files?: number
    scanned_bytes?: number
  }
}

export type TranslateResponse = {
  sql: string
  provider: string
  model: string
}

export type APIError = {
  error_code: string
  message: string
  retryable: boolean
  context?: Record<string, unknown>
  trace_id?: string
}
