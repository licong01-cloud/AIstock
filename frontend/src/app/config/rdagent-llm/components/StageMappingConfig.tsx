"use client";

import { useState, useEffect } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Loader2, Save } from "lucide-react";

interface Model {
  id: number;
  display_name: string;
  full_model_id: string;
  model_type: string;
}

interface StageMapping {
  stage_name: string;
  model_id: string;
  temperature: number | null;
  max_tokens: number | null;
}

interface StageMappingConfigProps {
  models: Model[];
  onConfigUpdated: () => void;
  onError: (message: string) => void;
  apiBase: string;
}

const STAGES = [
  { name: "direct_exp_gen", label: "假设生成+实验设计", description: "生成研究假设和实验方案" },
  { name: "coding", label: "代码生成", description: "生成和修改Factor/Strategy代码" },
  { name: "feedback", label: "反馈分析", description: "分析实验结果并生成反馈" },
  { name: "default", label: "默认兜底", description: "其他未定义阶段的默认配置" },
  { name: "embedding", label: "向量嵌入", description: "文本向量化（RAG/知识库）" },
];

export default function StageMappingConfig({
  models,
  onConfigUpdated,
  onError,
  apiBase,
}: StageMappingConfigProps) {
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [mappings, setMappings] = useState<Record<string, StageMapping>>({});
  const [changeReason, setChangeReason] = useState("");

  useEffect(() => {
    loadMappings();
  }, []);

  const loadMappings = async () => {
    setLoading(true);
    try {
      const response = await fetch(`${apiBase}/rdagent/llm-config/stage-mappings`);
      const data = await response.json();

      const mappingsMap: Record<string, StageMapping> = {};
      data.mappings?.forEach((m: any) => {
        mappingsMap[m.stage_name] = {
          stage_name: m.stage_name,
          model_id: m.full_model_id || "",
          temperature: m.temperature,
          max_tokens: m.max_tokens,
        };
      });

      setMappings(mappingsMap);
    } catch (err) {
      onError("加载阶段映射失败");
    } finally {
      setLoading(false);
    }
  };

  const handleSave = async () => {
    if (!changeReason.trim()) {
      onError("请填写变更原因");
      return;
    }

    setSaving(true);
    try {
      const stageMappings = Object.values(mappings).filter((m) => m.model_id);

      const response = await fetch(`${apiBase}/rdagent/llm-config/update-config`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          stage_mappings: stageMappings,
          change_reason: changeReason,
        }),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || "更新配置失败");
      }

      onConfigUpdated();
      setChangeReason("");
    } catch (err) {
      onError(err instanceof Error ? err.message : "更新配置失败");
    } finally {
      setSaving(false);
    }
  };

  const updateMapping = (stageName: string, field: string, value: any) => {
    setMappings((prev) => ({
      ...prev,
      [stageName]: {
        ...prev[stageName],
        stage_name: stageName,
        [field]: value,
      },
    }));
  };

  const getModelsForStage = (stageName: string) => {
    if (stageName === "embedding") {
      return models.filter((m) => m.model_type === "embedding");
    }
    return models.filter((m) => m.model_type === "chat" || m.model_type === "reasoner");
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

  return (
    <Card>
      <CardHeader>
        <CardTitle>RD-Agent阶段模型配置</CardTitle>
        <CardDescription>
          为RD-Agent的各个执行阶段配置使用的LLM模型
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-6">
        {STAGES.map((stage) => (
          <div key={stage.name} className="border rounded-lg p-4 space-y-3">
            <div>
              <h3 className="font-medium">{stage.label}</h3>
              <p className="text-sm text-gray-500">{stage.description}</p>
            </div>

            <div className="grid grid-cols-3 gap-4">
              <div className="col-span-2 space-y-2">
                <Label>选择模型</Label>
                <Select
                  value={mappings[stage.name]?.model_id || ""}
                  onValueChange={(value) => updateMapping(stage.name, "model_id", value)}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="选择模型" />
                  </SelectTrigger>
                  <SelectContent>
                    {getModelsForStage(stage.name).map((model) => (
                      <SelectItem key={model.full_model_id} value={model.full_model_id}>
                        {model.display_name} ({model.model_type})
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {stage.name !== "embedding" && (
                <>
                  <div className="space-y-2">
                    <Label>Temperature</Label>
                    <Input
                      type="number"
                      step="0.1"
                      min="0"
                      max="2"
                      placeholder="0.7"
                      value={mappings[stage.name]?.temperature ?? ""}
                      onChange={(e) =>
                        updateMapping(
                          stage.name,
                          "temperature",
                          e.target.value ? parseFloat(e.target.value) : null
                        )
                      }
                    />
                  </div>
                </>
              )}
            </div>

            {stage.name !== "embedding" && (
              <div className="space-y-2">
                <Label>Max Tokens</Label>
                <Input
                  type="number"
                  placeholder="4000"
                  value={mappings[stage.name]?.max_tokens ?? ""}
                  onChange={(e) =>
                    updateMapping(
                      stage.name,
                      "max_tokens",
                      e.target.value ? parseInt(e.target.value) : null
                    )
                  }
                />
              </div>
            )}
          </div>
        ))}

        <div className="space-y-2">
          <Label>变更原因 *</Label>
          <Textarea
            placeholder="请简要说明本次配置变更的原因"
            value={changeReason}
            onChange={(e) => setChangeReason(e.target.value)}
            rows={3}
          />
        </div>

        <div className="flex justify-end">
          <Button onClick={handleSave} disabled={saving}>
            {saving && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            <Save className="mr-2 h-4 w-4" />
            {saving ? "保存中..." : "保存配置"}
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
