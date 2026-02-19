import { useMemo } from 'react'
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  useReactTable,
} from '@tanstack/react-table'

type Props = {
  columns: string[]
  rows: unknown[][]
}

type RowRecord = Record<string, unknown>

export function ResultTable({ columns, rows }: Props) {
  const data = useMemo<RowRecord[]>(() => {
    return rows.map((row) => {
      const record: RowRecord = {}
      columns.forEach((col, index) => {
        record[col] = row[index]
      })
      return record
    })
  }, [columns, rows])

  const columnHelper = createColumnHelper<RowRecord>()
  const tableColumns = useMemo(
    () =>
      columns.map((name) =>
        columnHelper.accessor(name, {
          id: name,
          header: name,
          cell: (info) => renderCell(info.getValue()),
        }),
      ),
    [columnHelper, columns],
  )

  // eslint-disable-next-line react-hooks/incompatible-library
  const table = useReactTable({
    data,
    columns: tableColumns,
    getCoreRowModel: getCoreRowModel(),
  })

  if (!columns.length) {
    return <div className="rounded-xl border border-slate-200 bg-white/70 p-6 text-slate-500">Run a query to see results.</div>
  }

  return (
    <div className="overflow-x-auto rounded-xl border border-slate-200 bg-white shadow-card">
      <table className="min-w-full text-left text-sm">
        <thead className="bg-slate-100/80 text-slate-700">
          {table.getHeaderGroups().map((headerGroup) => (
            <tr key={headerGroup.id}>
              {headerGroup.headers.map((header) => (
                <th className="px-3 py-2 font-semibold" key={header.id}>
                  {header.isPlaceholder
                    ? null
                    : flexRender(header.column.columnDef.header, header.getContext())}
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody>
          {table.getRowModel().rows.map((row) => (
            <tr className="border-t border-slate-100" key={row.id}>
              {row.getVisibleCells().map((cell) => (
                <td className="px-3 py-2 font-mono text-xs text-slate-800" key={cell.id}>
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function renderCell(value: unknown): string {
  if (value === null || value === undefined) {
    return 'NULL'
  }
  if (typeof value === 'object') {
    return JSON.stringify(value)
  }
  return String(value)
}
