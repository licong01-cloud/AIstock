"use client";

import { useCallback, useEffect, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

// ─── 类型 ────────────────────────────────────────────────────────────────────

interface BlockInfo {
  name: string;
  display_name: string;
  count: number;
  size: number;
}

interface StockInfo {
  code: string;
  market: string;
}

// ─── 页面 ────────────────────────────────────────────────────────────────────

export default function TdxBlocksPage() {
  const [available, setAvailable] = useState(false);
  const [checked, setChecked] = useState(false);
  const [blocks, setBlocks] = useState<BlockInfo[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // 板块详情
  const [selectedBlock, setSelectedBlock] = useState<string | null>(null);
  const [stocks, setStocks] = useState<StockInfo[]>([]);
  const [stocksLoading, setStocksLoading] = useState(false);

  // 创建表单
  const [showCreate, setShowCreate] = useState(false);
  const [newName, setNewName] = useState("");
  const [newDisplayName, setNewDisplayName] = useState("");
  const [newStocks, setNewStocks] = useState("");

  // 添加股票
  const [addStocksInput, setAddStocksInput] = useState("");
  const [addMode, setAddMode] = useState<string | null>(null);

  // ─── 初始化检查 ──────────────────────────────────────────────────────
  useEffect(() => {
    fetch(`${API_BASE}/tdx-blocks/available`)
      .then((r) => r.json())
      .then((d) => {
        setAvailable(d.available);
        setChecked(true);
      })
      .catch(() => {
        setAvailable(false);
        setChecked(true);
      });
  }, []);

  // ─── 加载板块列表 ────────────────────────────────────────────────────
  const loadBlocks = useCallback(() => {
    setLoading(true);
    setError(null);
    fetch(`${API_BASE}/tdx-blocks/list`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((d) => setBlocks(d))
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    if (available) loadBlocks();
  }, [available, loadBlocks]);

  // ─── 加载板块内股票 ──────────────────────────────────────────────────
  const loadStocks = useCallback(
    (name: string) => {
      setStocksLoading(true);
      fetch(`${API_BASE}/tdx-blocks/${name}/stocks`)
        .then((r) => r.json())
        .then((d) => {
          setStocks(d.stocks || []);
          setSelectedBlock(name);
        })
        .catch(() => setStocks([]))
        .finally(() => setStocksLoading(false));
    },
    []
  );

  // ─── 创建板块 ────────────────────────────────────────────────────────
  const handleCreate = async () => {
    if (!newName.trim()) return;
    const stocksArr = newStocks
      .split(/[,，\s]+/)
      .map((s) => s.trim())
      .filter(Boolean);

    try {
      const r = await fetch(`${API_BASE}/tdx-blocks/create`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: newName.trim(),
          display_name: newDisplayName.trim() || newName.trim(),
          stocks: stocksArr,
        }),
      });
      if (!r.ok) {
        const d = await r.json();
        throw new Error(d.detail || `HTTP ${r.status}`);
      }
      setShowCreate(false);
      setNewName("");
      setNewDisplayName("");
      setNewStocks("");
      loadBlocks();
    } catch (e: unknown) {
      alert((e as Error).message);
    }
  };

  // ─── 添加股票 ────────────────────────────────────────────────────────
  const handleAddStocks = async () => {
    if (!addMode || !addStocksInput.trim()) return;
    const stocksArr = addStocksInput
      .split(/[,，\s]+/)
      .map((s) => s.trim())
      .filter(Boolean);

    try {
      const r = await fetch(`${API_BASE}/tdx-blocks/${addMode}/add`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ stocks: stocksArr }),
      });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const d = await r.json();
      alert(`成功添加 ${d.added} 只, 跳过 ${d.skipped} 只已存在`);
      setAddMode(null);
      setAddStocksInput("");
      loadBlocks();
      if (selectedBlock === addMode) loadStocks(addMode);
    } catch (e: unknown) {
      alert((e as Error).message);
    }
  };

  // ─── 移除股票 ────────────────────────────────────────────────────────
  const handleRemoveStock = async (code: string) => {
    if (!selectedBlock) return;
    if (!confirm(`确认从板块 ${selectedBlock} 移除 ${code}?`)) return;

    try {
      await fetch(`${API_BASE}/tdx-blocks/${selectedBlock}/remove`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ stocks: [code] }),
      });
      loadStocks(selectedBlock);
      loadBlocks();
    } catch (e: unknown) {
      alert((e as Error).message);
    }
  };

  // ─── 删除板块 ────────────────────────────────────────────────────────
  const handleDelete = async (name: string) => {
    if (!confirm(`确认删除板块 "${name}"? 此操作不可恢复。`)) return;
    try {
      await fetch(`${API_BASE}/tdx-blocks/${name}`, { method: "DELETE" });
      if (selectedBlock === name) {
        setSelectedBlock(null);
        setStocks([]);
      }
      loadBlocks();
    } catch (e: unknown) {
      alert((e as Error).message);
    }
  };

  // ─── 未配置提示 ──────────────────────────────────────────────────────
  if (!checked) {
    return (
      <div className="p-6">
        <h1 className="text-2xl font-bold mb-4">📡 通达信板块管理</h1>
        <p>检查服务状态...</p>
      </div>
    );
  }

  if (!available) {
    return (
      <div className="p-6">
        <h1 className="text-2xl font-bold mb-4">📡 通达信板块管理</h1>
        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
          <p className="font-semibold text-yellow-800">功能未启用</p>
          <p className="text-yellow-700 text-sm mt-1">
            请在 <code className="bg-yellow-100 px-1 rounded">.env</code>{" "}
            中配置{" "}
            <code className="bg-yellow-100 px-1 rounded">
              TDX_CLIENT_PATH
            </code>{" "}
            指向通达信客户端安装目录，并确认客户端已运行、已登录且 TdxQuant 可连接。
            例如：{" "}
            <code className="bg-yellow-100 px-1 rounded">
              C:\new_tdx64
            </code>{" "}
          </p>
          <pre className="bg-yellow-100 p-2 rounded mt-2 text-sm">
            TDX_CLIENT_PATH=C:\new_tdx64
          </pre>
        </div>
      </div>
    );
  }

  // ─── 主界面 ───────────────────────────────────────────────────────────
  return (
    <div className="p-6 max-w-6xl mx-auto">
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold">📡 通达信板块管理</h1>
        <button
          onClick={() => setShowCreate(true)}
          className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 text-sm"
        >
          + 新建板块
        </button>
      </div>

      {/* 创建面板 */}
      {showCreate && (
        <div className="bg-white border rounded-lg p-4 mb-4 shadow-sm">
          <h3 className="font-semibold mb-3">新建板块</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div>
              <label className="text-sm text-gray-600">
                板块名称（英文大写+数字）
              </label>
              <input
                className="w-full border rounded px-3 py-2 text-sm mt-1"
                placeholder="如 AIstock_SELECT"
                value={newName}
                onChange={(e) => setNewName(e.target.value.toUpperCase())}
              />
            </div>
            <div>
              <label className="text-sm text-gray-600">
                显示名（中文，通达信内显示）
              </label>
              <input
                className="w-full border rounded px-3 py-2 text-sm mt-1"
                placeholder="如 AIstock精选"
                value={newDisplayName}
                onChange={(e) => setNewDisplayName(e.target.value)}
              />
            </div>
          </div>
          <div className="mt-3">
            <label className="text-sm text-gray-600">
              股票代码（逗号或空格分隔，如 600519.SH, 000858.SZ）
            </label>
            <textarea
              className="w-full border rounded px-3 py-2 text-sm mt-1"
              rows={2}
              placeholder="600519.SH, 000858.SZ, 300750.SZ"
              value={newStocks}
              onChange={(e) => setNewStocks(e.target.value)}
            />
          </div>
          <div className="flex gap-2 mt-3">
            <button
              onClick={handleCreate}
              className="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700 text-sm"
            >
              创建
            </button>
            <button
              onClick={() => setShowCreate(false)}
              className="px-4 py-2 bg-gray-200 rounded hover:bg-gray-300 text-sm"
            >
              取消
            </button>
          </div>
        </div>
      )}

      {/* 添加股票面板 */}
      {addMode && (
        <div className="bg-white border rounded-lg p-4 mb-4 shadow-sm">
          <h3 className="font-semibold mb-2">
            添加股票到 [{addMode}]
          </h3>
          <textarea
            className="w-full border rounded px-3 py-2 text-sm"
            rows={2}
            placeholder="600519.SH, 000858.SZ, 300750.SZ"
            value={addStocksInput}
            onChange={(e) => setAddStocksInput(e.target.value)}
          />
          <div className="flex gap-2 mt-2">
            <button
              onClick={handleAddStocks}
              className="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700 text-sm"
            >
              添加
            </button>
            <button
              onClick={() => {
                setAddMode(null);
                setAddStocksInput("");
              }}
              className="px-4 py-2 bg-gray-200 rounded hover:bg-gray-300 text-sm"
            >
              取消
            </button>
          </div>
        </div>
      )}

      {error && (
        <div className="bg-red-50 border border-red-200 rounded p-3 mb-4 text-red-700 text-sm">
          {error}
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* 板块列表 */}
        <div className="lg:col-span-1">
          <h2 className="font-semibold mb-2 text-gray-700">
            板块列表 ({blocks.length})
          </h2>
          {loading ? (
            <p className="text-gray-500 text-sm">加载中...</p>
          ) : blocks.length === 0 ? (
            <p className="text-gray-500 text-sm">暂无板块</p>
          ) : (
            <div className="space-y-1">
              {blocks.map((b) => (
                <div
                  key={b.name}
                  className={`flex items-center justify-between px-3 py-2 rounded cursor-pointer text-sm ${
                    selectedBlock === b.name
                      ? "bg-blue-50 border border-blue-300"
                      : "hover:bg-gray-50 border border-transparent"
                  }`}
                  onClick={() => loadStocks(b.name)}
                >
                  <div>
                    <span className="font-medium">{b.display_name}</span>
                    <span className="text-gray-400 ml-2 text-xs">
                      ({b.count}只)
                    </span>
                  </div>
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      handleDelete(b.name);
                    }}
                    className="text-red-400 hover:text-red-600 text-xs ml-2"
                    title="删除板块"
                  >
                    ✕
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* 板块详情 */}
        <div className="lg:col-span-2">
          {selectedBlock ? (
            <div>
              <div className="flex items-center justify-between mb-3">
                <h2 className="font-semibold text-gray-700">
                  {selectedBlock}{" "}
                  <span className="text-gray-400 text-sm font-normal">
                    ({stocks.length}只)
                  </span>
                </h2>
                <button
                  onClick={() => setAddMode(selectedBlock)}
                  className="px-3 py-1 bg-blue-100 text-blue-700 rounded hover:bg-blue-200 text-sm"
                >
                  + 添加股票
                </button>
              </div>
              {stocksLoading ? (
                <p className="text-gray-500 text-sm">加载中...</p>
              ) : stocks.length === 0 ? (
                <p className="text-gray-500 text-sm">板块为空</p>
              ) : (
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b text-left text-gray-500">
                      <th className="py-2 pr-4">#</th>
                      <th className="py-2 pr-4">代码</th>
                      <th className="py-2 pr-4">市场</th>
                      <th className="py-2 text-right">操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    {stocks.map((s, i) => (
                      <tr
                        key={s.code}
                        className="border-b hover:bg-gray-50"
                      >
                        <td className="py-2 pr-4 text-gray-400">{i + 1}</td>
                        <td className="py-2 pr-4 font-mono">{s.code}</td>
                        <td className="py-2 pr-4 text-gray-600">
                          {s.market}
                        </td>
                        <td className="py-2 text-right">
                          <button
                            onClick={() => handleRemoveStock(s.code)}
                            className="text-red-400 hover:text-red-600 text-xs"
                          >
                            移除
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          ) : (
            <div className="text-gray-500 text-sm flex items-center justify-center h-40 border border-dashed rounded">
              点击左侧板块查看详情
            </div>
          )}
        </div>
      </div>

      <p className="text-xs text-gray-400 mt-6">
        💡 修改后需重启通达信客户端才能看到变化
      </p>
    </div>
  );
}
