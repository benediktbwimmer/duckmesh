import type {
  APIError,
  ConnectionSettings,
  QueryResponse,
  TranslateResponse,
  UISchemaResponse,
} from './types'

const jsonHeaders = {
  'Content-Type': 'application/json',
} as const

async function request<T>(
  settings: ConnectionSettings,
  path: string,
  init: RequestInit,
): Promise<T> {
  const headers: Record<string, string> = {
    ...(init.headers as Record<string, string> | undefined),
  }
  if (settings.apiKey.trim()) {
    headers['X-API-Key'] = settings.apiKey.trim()
  }
  if (settings.tenantId.trim()) {
    headers['X-Tenant-ID'] = settings.tenantId.trim()
  }

  const response = await fetch(`${settings.baseUrl.replace(/\/$/, '')}${path}`, {
    ...init,
    headers,
  })

  if (!response.ok) {
    const maybeError = (await response.json().catch(() => null)) as APIError | null
    throw new Error(maybeError?.message || `Request failed with status ${response.status}`)
  }
  return (await response.json()) as T
}

export async function fetchUISchema(settings: ConnectionSettings): Promise<UISchemaResponse> {
  return request<UISchemaResponse>(settings, '/v1/ui/schema', {
    method: 'GET',
  })
}

export async function runQuery(settings: ConnectionSettings, sql: string): Promise<QueryResponse> {
  return request<QueryResponse>(settings, '/v1/query', {
    method: 'POST',
    headers: jsonHeaders,
    body: JSON.stringify({ sql, row_limit: 2000 }),
  })
}

export async function translateQuery(
  settings: ConnectionSettings,
  prompt: string,
): Promise<TranslateResponse> {
  return request<TranslateResponse>(settings, '/v1/query/translate', {
    method: 'POST',
    headers: jsonHeaders,
    body: JSON.stringify({ prompt }),
  })
}
