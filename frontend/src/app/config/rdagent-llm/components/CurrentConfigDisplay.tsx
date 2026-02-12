"use client";

import { useState, useEffect } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Loader2, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";

interface CurrentConfigDisplayProps {
  apiBase: string;
}

export default function CurrentConfigDisplay({ apiBase }: CurrentConfigDisplayProps) {
  const [loading, setLoading] = useState(true);
  const [config, setConfig] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    loadConfig();
  }, []);

  const loadConfig = async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`${apiBase}/rdagent/llm-config/current-config`);
      if (!response.ok) throw new Error("Failed to load config");
      const data = await response.json();
      setConfig(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load config");
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <Card>
        <CardContent className="flex items-center justify-center h-64">
          <Loader2 className="h-8 w-8 animate-spin text-gray-400" />
        </CardContent>
      </Card>
    );
  }

  if (error) {
    return (
      <Card>
        <CardContent className="flex flex-col items-center justify-center h-64 space-y-4">
          <p className="text-red-600">{error}</p>
          <Button onClick={loadConfig} variant="outline" size="sm">
            <RefreshCw className="h-4 w-4 mr-2" />
            重试
          </Button>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <div>
            <CardTitle>当前RD-Agent配置</CardTitle>
            <CardDescription>
              从.env文件读取的当前配置状态
            </CardDescription>
          </div>
          <Button onClick={loadConfig} variant="outline" size="sm">
            <RefreshCw className="h-4 w-4 mr-2" />
            刷新
          </Button>
        </div>
      </CardHeader>
      <CardContent className="space-y-6">
        {/* 基础配置 */}
        <div className="space-y-3">
          <h3 className="font-semibold text-lg">基础配置</h3>
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-1">
              <div className="text-sm text-gray-500">默认对话模型</div>
              <div className="font-mono text-sm bg-gray-50 p-2 rounded">
                {config?.chat_model || "未配置"}
              </div>
            </div>
            <div className="space-y-1">
              <div className="text-sm text-gray-500">嵌入模型</div>
              <div className="font-mono text-sm bg-gray-50 p-2 rounded">
                {config?.embedding_model || "未配置"}
              </div>
            </div>
          </div>
        </div>

        {/* 阶段模型映射 */}
        <div className="space-y-3">
          <h3 className="font-semibold text-lg">阶段模型映射</h3>
          {config?.stage_mappings && Object.keys(config.stage_mappings).length > 0 ? (
            <div className="space-y-2">
              {Object.entries(config.stage_mappings).map(([stage, mapping]: [string, any]) => (
                <div key={stage} className="flex items-center justify-between p-3 border rounded-lg">
                  <div className="flex items-center gap-3">
                    <Badge variant="outline">{stage}</Badge>
                    <span className="font-mono text-sm">{mapping.model}</span>
                  </div>
                  <div className="flex items-center gap-4 text-sm text-gray-500">
                    {mapping.temperature && (
                      <span>temp: {mapping.temperature}</span>
                    )}
                    {mapping.max_tokens && (
                      <span>max_tokens: {mapping.max_tokens}</span>
                    )}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="text-center py-4 text-gray-500">
              暂无阶段映射配置
            </div>
          )}
        </div>

        {/* 配置时间 */}
        {config?.last_updated && (
          <div className="pt-4 border-t">
            <div className="text-sm text-gray-500">
              最后更新时间: {new Date(config.last_updated).toLocaleString("zh-CN")}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
