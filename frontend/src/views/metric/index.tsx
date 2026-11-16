import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/metric'
const columns = [
  "采集器编号",
  "作业名称",
  "指标名称",
  "采集间隔",
  "聚合方式",
  "保留天数",
  "最近采集",
  "采集器状态"
]
const actions = [
  "启动采集",
  "暂停采集",
  "关闭采集"
]
const statuses = [
  "草稿",
  "采集中",
  "已暂停",
  "已关闭"
]
const stats = [
  {
    "label": "指标采集器总数",
    "value": 0
  },
  {
    "label": "采集中采集器",
    "value": 0
  },
  {
    "label": "已暂停采集器",
    "value": 0
  }
]
const detailFields = [
  "采集器编号",
  "作业名称",
  "指标名称",
  "采集间隔",
  "聚合方式",
  "保留天数",
  "最近采集",
  "采集器状态",
  "数据集聚合口径",
  "数据集保留期",
  "数据集版本"
]

type Row = Record<string, string | number | null>

type AggregateRow = {
  采集器唯一标识: string
  指标名称: string
  聚合口径: string
  保留期: number
  数据集版本: number
  记录状态: string
}

export default function MetricView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [aggregates, setAggregates] = useState<AggregateRow[]>([])
  const [selectedId, setSelectedId] = useState<number | null>(null)
  const [errorMessage, setErrorMessage] = useState('')
  const [successMessage, setSuccessMessage] = useState('')
  const [recovering, setRecovering] = useState<string | null>(null)
  const [filters, setFilters] = useState<Record<string, string>>({})
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const [listResponse, aggregateResponse] = await Promise.all([
        request(`${ENDPOINT}?${query}`),
        request(`${ENDPOINT}/aggregates`),
      ])
      if (!listResponse.ok || !aggregateResponse.ok) {
        throw new Error('指标采集器列表读取失败')
      }
      const payload = await listResponse.json()
      const aggregatePayload = await aggregateResponse.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
      setAggregates(aggregatePayload.items ?? [])
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '流作业指标采集列表读取失败')
    }
  }, [filters])

  useEffect(() => {
    void reload()
  }, [reload])

  const selectedRow = useMemo(
    () => rows.find((row) => Number(row.id) === selectedId) ?? null,
    [rows, selectedId],
  )

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    setSuccessMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ action }),
      })
      if (!response.ok) {
        throw new Error('流作业指标采集动作未生效，请稍后重试')
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '流作业指标采集操作失败')
    }
  }

  async function recoverCollector(row: Row) {
    setErrorMessage('')
    setSuccessMessage('')
    const collectorId = String(row['采集器编号'] ?? '')
    setRecovering(collectorId)
    try {
      const response = await request(`${ENDPOINT}/recover`, {
        method: 'POST',
        body: JSON.stringify({ values: { 采集器编号: collectorId } }),
      })
      const payload = await response.json()
      if (!response.ok || !payload.ok) {
        throw new Error(payload.message ?? '指标采集器恢复失败')
      }
      setSuccessMessage(payload.message ?? '指标采集器恢复完成')
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '指标采集器恢复失败')
    } finally {
      setRecovering(null)
    }
  }

  return (
    <section className="page" data-module="metric">
      <header className="page-head">
        <div>
          <h2>流作业指标采集管理</h2>
          <p className="page-desc">维护流作业指标的采集间隔、聚合方式与保留策略；恢复时校验指标名称、聚合方式与保留天数并写入聚合指标数据集，为流作业指标采集提供可审计的数据工程流程。</p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('指标采集器登记入口尚未接入审批流')}>
            登记指标采集器
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出流作业指标采集清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        {stats.map((item) => (
          <article key={item.label} className="stat-card">
            <span className="stat-label">{item.label}</span>
            <strong className="stat-value">{item.value}</strong>
          </article>
        ))}
      </div>

      <div className="dataset-section">
        <h3>聚合指标数据集（每个采集器唯一一条有效记录，含唯一标识、聚合口径、保留期）</h3>
        <table className="data-table">
          <thead>
            <tr>
              <th>采集器唯一标识</th>
              <th>指标名称</th>
              <th>聚合口径</th>
              <th>保留期（天）</th>
              <th>数据集版本</th>
              <th>记录状态</th>
            </tr>
          </thead>
          <tbody>
            {aggregates.map((row) => (
              <tr key={`${row.采集器唯一标识}-${row.数据集版本}`}>
                <td>{row.采集器唯一标识}</td>
                <td>{row.指标名称}</td>
                <td>{row.聚合口径}</td>
                <td>{row.保留期}</td>
                <td>v{row.数据集版本}</td>
                <td>{row.记录状态}</td>
              </tr>
            ))}
            {aggregates.length === 0 && (
              <tr>
                <td className="empty-state" colSpan={6}>暂无有效聚合指标记录</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }}>
        {filterFields.map((field) => (
          <label key={field} className="filter-item">
            <span>{field}</span>
            <input
              value={filters[field] ?? ''}
              placeholder={`按${field}检索`}
              onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
            />
          </label>
        ))}
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      <div className="content-layout">
        <div className="content-main">
          <table className="data-table">
            <thead>
              <tr>
                {columns.map((column) => <th key={column}>{column}</th>)}
                <th>可执行动作</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr
                  key={String(row.id)}
                  onClick={() => setSelectedId(Number(row.id))}
                  style={selectedId === Number(row.id) ? { background: '#eff6ff' } : undefined}
                >
                  {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
                  <td className="row-actions">
                    {actions.map((action) => (
                      <button
                        key={action}
                        className="link"
                        type="button"
                        onClick={(event) => {
                          event.stopPropagation()
                          void runAction(action, row)
                        }}
                      >
                        {action}
                      </button>
                    ))}
                    <button
                      className="link"
                      type="button"
                      disabled={recovering === String(row['采集器编号'] ?? '')}
                      onClick={(event) => {
                        event.stopPropagation()
                        void recoverCollector(row)
                      }}
                    >
                      {recovering === String(row['采集器编号'] ?? '') ? '恢复中…' : '恢复采集'}
                    </button>
                  </td>
                </tr>
              ))}
              {rows.length === 0 && (
                <tr>
                  <td className="empty-state" colSpan={columns.length + 1}>暂无流作业指标采集数据，可先登记指标采集器</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <aside className="detail-side" aria-label="采集器详情侧栏">
          <h3>采集器详情</h3>
          {selectedRow === null ? (
            <p className="detail-empty">点击列表中的采集器查看详情，侧栏与列表读取同一条有效记录。</p>
          ) : (
            <div className="detail-list">
              {detailFields.map((field) => (
                <div className="detail-item" key={field}>
                  <span>{field}</span>
                  <strong>{selectedRow[field] === null || selectedRow[field] === undefined || selectedRow[field] === '' ? '—' : String(selectedRow[field])}</strong>
                </div>
              ))}
            </div>
          )}
        </aside>
      </div>

      <footer className="page-foot">
        <span>共 {total} 条流作业指标采集记录</span>
        {successMessage && <span className="success-text">{successMessage}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
