"use client";

import { useState, useEffect } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Loader2, ChevronLeft, ChevronRight } from "lucide-react";

interface ChangeLog {
  id: number;
  stage_name: string;
  old_model_display_name: string | null;
  new_model_display_name: string | null;
  old_full_model_id: string | null;
  new_full_model_id: string | null;
  change_reason: string | null;
  is_rollback: boolean;
  created_at: string;
}

interface ChangeLogViewerProps {
  apiBase: string;
}

export default function ChangeLogViewer({ apiBase }: ChangeLogViewerProps) {
  const [loading, setLoading] = useState(true);
  const [logs, setLogs] = useState<ChangeLog[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(5);
  const [showMore, setShowMore] = useState(false);

  useEffect(() => {
    loadLogs();
  }, [page, pageSize]);

  const loadLogs = async () => {
    setLoading(true);
    try {
      const limit = showMore ? 10 : 5;
      const offset = page * limit;
      const response = await fetch(
        `${apiBase}/rdagent/llm-config/change-logs?limit=${limit}&offset=${offset}`
      );
      const data = await response.json();
      setLogs(data.logs || []);
      setTotal(data.total || 0);
      setPageSize(limit);
    } catch (err) {
      console.error("Failed to load change logs:", err);
    } finally {
      setLoading(false);
    }
  };

  const handleShowMore = () => {
    setShowMore(true);
    setPage(0);
  };

  const handleShowLess = () => {
    setShowMore(false);
    setPage(0);
  };

  const totalPages = Math.ceil(total / pageSize);

  if (loading && logs.length === 0) {
    return (
      <Card>
        <CardContent className="flex items-center justify-center h-64">
          <Loader2 className="h-8 w-8 animate-spin text-gray-400" />
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>配置变更记录</CardTitle>
        <CardDescription>
          查看RD-Agent LLM配置的历史变更记录
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {logs.length === 0 ? (
          <div className="text-center py-8 text-gray-500">
            暂无变更记录
          </div>
        ) : (
          <>
            <div className="space-y-3">
              {logs.map((log) => (
                <div key={log.id} className="border rounded-lg p-4 space-y-2">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <Badge variant="outline">{log.stage_name}</Badge>
                      {log.is_rollback && (
                        <Badge variant="destructive" className="text-xs">
                          回滚
                        </Badge>
                      )}
                    </div>
                    <div className="text-sm text-gray-500">
                      {new Date(log.created_at).toLocaleString("zh-CN")}
                    </div>
                  </div>

                  <div className="flex items-center gap-2 text-sm">
                    <span className="text-gray-600">
                      {log.old_model_display_name || "未配置"}
                    </span>
                    <span className="text-gray-400">→</span>
                    <span className="font-medium">
                      {log.new_model_display_name || "未配置"}
                    </span>
                  </div>

                  {log.change_reason && (
                    <div className="text-sm text-gray-600">
                      原因: {log.change_reason}
                    </div>
                  )}

                  <div className="text-xs text-gray-400 font-mono">
                    {log.old_full_model_id || "N/A"} → {log.new_full_model_id || "N/A"}
                  </div>
                </div>
              ))}
            </div>

            {/* 控制按钮 */}
            <div className="flex items-center justify-between pt-4 border-t">
              <div className="text-sm text-gray-500">
                共 {total} 条记录，当前显示 {showMore ? "10" : "5"} 条/页
              </div>
              <div className="flex items-center gap-2">
                {!showMore && total > 5 && (
                  <Button onClick={handleShowMore} variant="outline" size="sm">
                    查看更多
                  </Button>
                )}
                {showMore && (
                  <Button onClick={handleShowLess} variant="outline" size="sm">
                    显示较少
                  </Button>
                )}
              </div>
            </div>

            {/* 分页控制 */}
            {totalPages > 1 && (
              <div className="flex items-center justify-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setPage(Math.max(0, page - 1))}
                  disabled={page === 0 || loading}
                >
                  <ChevronLeft className="h-4 w-4" />
                </Button>
                <span className="text-sm text-gray-600">
                  第 {page + 1} / {totalPages} 页
                </span>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setPage(Math.min(totalPages - 1, page + 1))}
                  disabled={page >= totalPages - 1 || loading}
                >
                  <ChevronRight className="h-4 w-4" />
                </Button>
              </div>
            )}
          </>
        )}
      </CardContent>
    </Card>
  );
}
