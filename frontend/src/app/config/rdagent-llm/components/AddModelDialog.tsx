"use client";

import { useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Loader2 } from "lucide-react";
import LITELLM_PROVIDERS, { getProviderModels, LiteLLMProviderPreset } from "../litellm-providers";

interface Provider {
  id: number;
  provider_name: string;
  display_name: string;
  supports_chat: boolean;
  supports_embedding: boolean;
  supports_reasoner: boolean;
}

interface AddModelDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  provider: Provider;
  onSuccess: () => void;
  onError: (message: string) => void;
  apiBase: string;
}

export default function AddModelDialog({
  open,
  onOpenChange,
  provider,
  onSuccess,
  onError,
  apiBase,
}: AddModelDialogProps) {
  const [loading, setLoading] = useState(false);
  const [selectedSuggestion, setSelectedSuggestion] = useState("");
  const [formData, setFormData] = useState({
    model_name: "",
    display_name: "",
    model_type: "",
    model_category: "",
    description: "",
    api_key: "",
  });

  const preset: LiteLLMProviderPreset | undefined = LITELLM_PROVIDERS.find((p) => p.provider_name === provider.provider_name);
  const suggestions = getProviderModels(provider.provider_name);
  const litellmPrefix = preset?.litellm_prefix || "";

  const handleSuggestionSelect = (modelName: string) => {
    setSelectedSuggestion(modelName);
    if (!modelName) {
      setFormData({ ...formData, model_name: "", display_name: "", model_type: "", model_category: "" });
      return;
    }
    const m = suggestions.find((s) => s.model_name === modelName);
    if (m) {
      setFormData({
        ...formData,
        model_name: m.model_name,
        display_name: m.display_name,
        model_type: m.model_type,
        model_category: m.model_category,
      });
    }
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);

    try {
      const response = await fetch(`${apiBase}/rdagent/llm-config/models`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          provider_id: provider.id,
          ...formData,
        }),
      });

      const data = await response.json();

      if (!response.ok || !data.ok) {
        throw new Error(data.message || "Failed to add model");
      }

      onSuccess();
      onOpenChange(false);
      setFormData({
        model_name: "",
        display_name: "",
        model_type: "",
        model_category: "",
        description: "",
        api_key: "",
      });
      setSelectedSuggestion("");
    } catch (err) {
      onError(err instanceof Error ? err.message : "Failed to add model");
    } finally {
      setLoading(false);
    }
  };

  const getAvailableModelTypes = () => {
    const types = [];
    if (provider.supports_chat) types.push({ value: "chat", label: "对话/Coding" });
    if (provider.supports_reasoner) types.push({ value: "reasoner", label: "推理模型" });
    if (provider.supports_embedding) types.push({ value: "embedding", label: "嵌入式模型" });
    return types;
  };

  const placeholder = suggestions.length > 0 ? suggestions[0].model_name : "deepseek-chat";
  const displayPlaceholder = suggestions.length > 0 ? suggestions[0].display_name : "DeepSeek Chat";

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>添加模型 - {provider.display_name}</DialogTitle>
          <DialogDescription>
            添加新的LLM模型，系统将自动验证API Key和模型可用性
          </DialogDescription>
        </DialogHeader>

        <form onSubmit={handleSubmit} className="space-y-4">
          {/* 推荐模型选择 */}
          {suggestions.length > 0 && (
            <div className="space-y-2">
              <Label>推荐模型 (LiteLLM)</Label>
              <Select value={selectedSuggestion} onValueChange={handleSuggestionSelect}>
                <SelectTrigger>
                  <SelectValue placeholder="-- 选择推荐模型 或 手动填写下方字段 --" />
                </SelectTrigger>
                <SelectContent>
                  {suggestions.map((s) => (
                    <SelectItem key={s.model_name} value={s.model_name}>
                      {s.display_name} [{s.model_type}]
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          )}

          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="model_name">模型名称 *</Label>
              <Input
                id="model_name"
                placeholder={`例如: ${placeholder}`}
                value={formData.model_name}
                onChange={(e) =>
                  setFormData({ ...formData, model_name: e.target.value })
                }
                required
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="display_name">显示名称 *</Label>
              <Input
                id="display_name"
                placeholder={`例如: ${displayPlaceholder}`}
                value={formData.display_name}
                onChange={(e) =>
                  setFormData({ ...formData, display_name: e.target.value })
                }
                required
              />
            </div>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="model_type">模型类型 *</Label>
              <Select
                value={formData.model_type}
                onValueChange={(value) =>
                  setFormData({ ...formData, model_type: value })
                }
              >
                <SelectTrigger>
                  <SelectValue placeholder="选择模型类型" />
                </SelectTrigger>
                <SelectContent>
                  {getAvailableModelTypes().map((type) => (
                    <SelectItem key={type.value} value={type.value}>
                      {type.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-2">
              <Label htmlFor="model_category">模型分类</Label>
              <Select
                value={formData.model_category}
                onValueChange={(value) =>
                  setFormData({ ...formData, model_category: value })
                }
              >
                <SelectTrigger>
                  <SelectValue placeholder="选择分类（可选）" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="对话/Coding">对话/Coding</SelectItem>
                  <SelectItem value="推理模型">推理模型</SelectItem>
                  <SelectItem value="嵌入式模型">嵌入式模型</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>

          <div className="space-y-2">
            <Label htmlFor="description">模型说明（≤100字）</Label>
            <Textarea
              id="description"
              placeholder="简要描述模型特点、适用场景等"
              value={formData.description}
              onChange={(e) =>
                setFormData({ ...formData, description: e.target.value })
              }
              maxLength={100}
              rows={3}
            />
            <div className="text-xs text-gray-500 text-right">
              {formData.description.length}/100
            </div>
          </div>

          <div className="space-y-2">
            <Label htmlFor="api_key">API Key *</Label>
            <Input
              id="api_key"
              type="password"
              placeholder="输入API Key进行验证"
              value={formData.api_key}
              onChange={(e) =>
                setFormData({ ...formData, api_key: e.target.value })
              }
              required
            />
            <div className="text-xs text-gray-500">
              API Key将用于验证模型可用性，验证通过后才能添加
            </div>
          </div>

          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => onOpenChange(false)}
              disabled={loading}
            >
              取消
            </Button>
            <Button type="submit" disabled={loading}>
              {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              {loading ? "验证中..." : "添加模型"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
