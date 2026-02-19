import { useEffect, useMemo, useRef, useState } from 'react'
import Editor, { type Monaco, type OnMount } from '@monaco-editor/react'
import type { editor, languages } from 'monaco-editor'
import type { UISchemaTable } from '../lib/types'

type Props = {
  sql: string
  onChange: (value: string) => void
  tables: UISchemaTable[]
}

export function QueryEditor({ sql, onChange, tables }: Props) {
  const disposableRef = useRef<{ dispose: () => void } | null>(null)
  const monacoRef = useRef<Monaco | null>(null)
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null)
  const [monacoReady, setMonacoReady] = useState(false)

  type CompletionSeed = {
    label: string
    insertText: string
    detail: string
    kind: 'table' | 'column'
  }

  const completionItems = useMemo<CompletionSeed[]>(() => {
    const items: CompletionSeed[] = []

    for (const table of tables) {
      items.push({
        label: table.table_name,
        insertText: table.table_name,
        detail: 'table',
        kind: 'table',
      })

      for (const column of table.columns) {
        items.push({
          label: `${table.table_name}.${column}`,
          insertText: `"${table.table_name}"."${column}"`,
          detail: `column in ${table.table_name}`,
          kind: 'column',
        })
      }
    }

    return items
  }, [tables])

  const onMount: OnMount = (monacoEditor: editor.IStandaloneCodeEditor, monaco: Monaco) => {
    monacoRef.current = monaco
    editorRef.current = monacoEditor

    if (!monaco.languages.getLanguages().some((lang: languages.ILanguageExtensionPoint) => lang.id === 'ducksql')) {
      monaco.languages.register({ id: 'ducksql' })
    }
    monaco.languages.setMonarchTokensProvider('ducksql', {
      tokenizer: {
        root: [
          [/\b(select|from|where|group|by|order|limit|with|as|join|left|right|inner|outer|on|and|or|count|sum|avg|min|max)\b/i, 'keyword'],
          [/"[^"]+"/, 'string'],
          [/[a-zA-Z_][\w]*/, 'identifier'],
          [/\d+/, 'number'],
        ],
      },
    })

    const model = monacoEditor.getModel()
    if (model) {
      monaco.editor.setModelLanguage(model, 'ducksql')
    }
    monaco.editor.setTheme('vs')
    setMonacoReady(true)
  }

  useEffect(() => {
    if (!monacoReady || !monacoRef.current || !editorRef.current) {
      return
    }
    const monaco = monacoRef.current
    disposableRef.current?.dispose()

    disposableRef.current = monaco.languages.registerCompletionItemProvider('ducksql', {
      triggerCharacters: ['.', '"', ' '],
      provideCompletionItems: (model: editor.ITextModel, position: { lineNumber: number; column: number }) => {
        const word = model.getWordUntilPosition(position)
        const range = {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: word.startColumn,
          endColumn: word.endColumn,
        }

        const suggestions: languages.CompletionItem[] = completionItems.map((item) => ({
          label: item.label,
          insertText: item.insertText,
          detail: item.detail,
          kind: item.kind === 'table' ? monaco.languages.CompletionItemKind.Class : monaco.languages.CompletionItemKind.Field,
          range,
        }))

        return { suggestions }
      },
    })

    return () => {
      disposableRef.current?.dispose()
      disposableRef.current = null
    }
  }, [completionItems, monacoReady])

  return (
    <Editor
      height="45vh"
      defaultLanguage="ducksql"
      value={sql}
      onChange={(value) => onChange(value ?? '')}
      onMount={onMount}
      options={{
        fontFamily: 'IBM Plex Mono, monospace',
        fontSize: 14,
        minimap: { enabled: false },
        padding: { top: 12, bottom: 12 },
        suggestOnTriggerCharacters: true,
        quickSuggestions: true,
        wordBasedSuggestions: 'off',
        scrollBeyondLastLine: false,
      }}
    />
  )
}
