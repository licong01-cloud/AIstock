"use client";

import { useState, useEffect } from "react";
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
import { Loader2, CheckCircle2, XCircle } from "lucide-react";

interface Provider {
  id: number;
  provider_name: string;
  display_name: string;
  supports_chat: boolean;
  supports_embedding: boolean;
  supports_reasoner: boolean;
}

interface Model {
  id: number;
  provider_id?: number;
  model_name: string;
  display_name: string;
  full_model_id: string;
  model_type: string;
  model_category: string | null;
  description: string | null;
  is_verified: boolean;
  last_verified_at: string | null;
}

interface ModelConfig {
  api_key: string;
  api_base: string | null;
}

interface EditModelDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  model: Model | null;
  providers: Provider[];
  onSuccess: () => void;
  onError: (message: string) => void;
  apiBase: string;
}

export default function EditModelDialog({
  open,
  onOpenChange,
  model,
  providers,
  onSuccess,
  onError,
  apiBase,
}: EditModelDialogProps) {
  const [loading, setLoading] = useState(false);
  const [verifying, setVerifying] = useState(false);
  const [verificationResult, setVerificationResult] = useState<{
    success: boolean;
    message: string;
    details?: any;
  } | null>(null);
  
  const [formData, setFormData] = useState({
    model_name: "",
    display_name: "",
    model_type: "",
    model_category: "",
    description: "",
    api_key: "",
    api_base: "",
  });

  // 加载模型当前配置
  useEffect(() => {
    if (model && open) {
      setFormData({
        model_name: model.model_name,
        display_name: model.display_name,
        model_type: model.model_type,
        model_category: model.model_category || "",
        description: model.description || "",
        api_key: "", // API Key不预填充，需要用户重新输入或确认
        api_base: "",
      });
      setVerificationResult(null);
      
      // 加载现有API配置
      loadModelConfig();
    }
  }, [model, open]);

  const loadModelConfig = async () => {
    if (!model) return;
    
    try {
      const response = await fetch(
        `${apiBase}/rdagent/llm-config/models/${model.id}/config`
      );
      if (response.ok) {
        const data = await response.json();
        if (data.config) {
          setFormData(prev => ({
            ...prev,
            api_base: data.config.api_base || "",
            // API Key 通常不返回，保持空
          }));
        }
      }
    } catch (err) {
      console.error("加载模型配置失败:", err);
    }
  };

  const getCurrentProvider = () => {
    return providers.find(p => p.id === model?.provider_id);
  };

  const getAvailableModelTypes = () => {
    const provider = getCurrentProvider();
    if (!provider) return [];
    
    const types = [];
    if (provider.supports_chat) types.push({ value: "chat", label: "对话/Coding" });
    if (provider.supports_reasoner) types.push({ value: "reasoner", label: "推理模型" });
    if (provider.supports_embedding) types.push({ value: "embedding", label: "嵌入式模型" });
    return types;
  };

  // 验证模型配置
  const handleVerify = async () => {
    if (!model) return;
    
    setVerifying(true);
    setVerificationResult(null);

    try {
      const response = await fetch(`${apiBase}/rdagent/llm-config/models/verify`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          model_id: model.id,
          api_key: formData.api_key || undefined,
          api_base: formData.api_base || undefined,
          run_health_check: true,
          run_litellm_test: true,
        }),
      });

      const data = await response.json();

      if (data.verification?.overall_success) {
        setVerificationResult({
          success: true,
          message: "✓ 验证通过：LiteLLM测试和RDAgent健康检查都成功",
          details: data.verification,
        });
      } else {
        const errors = data.verification?.errors || [data.detail || "验证失败"];
        setVerificationResult({
          success: false,
          message: `✗ 验证失败: ${errors.join(", ")}`,
          details: data.verification,
        });
      }
    } catch (err) {
      setVerificationResult({
        success: false,
        message: `✗ 验证失败: ${err instanceof Error ? err.message : "网络错误"}`,
      });
    } finally {
      setVerifying(false);
    }
  };

  // 保存模型配置
  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!model) return;

    // 如果没有验证过，先验证
    if (!verificationResult?.success) {
      await handleVerify();
      return;
    }

    setLoading(true);

    try {
      // 1. 更新模型基本信息
      const updateResponse = await fetch(
        `${apiBase}/rdagent/llm-config/models/${model.id}`,
        {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            model_name: formData.model_name,
            display_name: formData.display_name,
            model_type: formData.model_type,
            model_category: formData.model_category,
            description: formData.description,
          }),
        }
      );

      if (!updateResponse.ok) {
        const error = await updateResponse.json();
        throw new Error(error.detail || "更新模型失败");
      }

      // 2. 更新API配置（如果提供了API Key）
      if (formData.api_key) {
        const configResponse = await fetch(
          `${apiBase}/rdagent/llm-config/models/${model.id}/update-api-config`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              model_id: model.id,
              api_key: formData.api_key,
              api_base: formData.api_base || null,
              verify_before_save: false, // 已经验证过了
            }),
          }
        );

        if (!configResponse.ok) {
          const error = await configResponse.json();
          throw new Error(error.detail || "更新API配置失败");
        }
      }

      onSuccess();
      onOpenChange(false);
    } catch (err) {
      onError(err instanceof Error ? err.message : "保存失败");
    } finally {
      setLoading(false);
    }
  };

  if (!model) return null;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>编辑模型 - {model.display_name}</DialogTitle>
          <DialogDescription>
            修改模型配置，确认后将执行完整验证（LiteLLM + RDAgent健康检查）
          </DialogDescription>
        </DialogHeader>

        <form onSubmit={handleSubmit} className="space-y-4">
          {/* 模型基本信息 */}
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="edit_model_name">模型名称 *</Label>
              <Input
                id="edit_model_name"
                placeholder="例如: deepseek-chat"
                value={formData.model_name}
                onChange={(e) =>
                  setFormData({ ...formData, model_name: e.target.value })
                }
                required
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="edit_display_name">显示名称 *</Label>
              <Input
                id="edit_display_name"
                placeholder="例如: DeepSeek Chat"
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
              <Label htmlFor="edit_model_type">模型类型 *</Label>
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
              <Label htmlFor="edit_model_category">模型分类</Label>
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
            <Label htmlFor="edit_description">模型说明（≤100字）</Label>
            <Textarea
              id="edit_description"
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

          {/* 分隔线 */}
          <div className="border-t pt-4">
            <h4 className="font-medium mb-3">API配置</h4>
            
            <div className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="edit_api_key">
                  API Key {model.is_verified ? "（留空保持原配置）" : "*"}
                </Label>
                <Input
                  id="edit_api_key"
                  type="password"
                  placeholder={model.is_verified ? "输入新API Key或留空" : "输入API Key进行验证"}
                  value={formData.api_key}
                  onChange={(e) =>
                    setFormData({ ...formData, api_key: e.target.value })
                  }
                  required={!model.is_verified}
                />
                <div className="text-xs text-gray-500">
                  {model.is_verified 
                    ? "模型已通过验证，如需更换API Key请输入新值" 
                    : "该模型尚未验证，必须提供API Key进行验证"}
                </div>
              </div>

              <div className="space-y-2">
                <Label htmlFor="edit_api_base">API Base URL（可选）</Label>
                <Input
                  id="edit_api_base"
                  placeholder="例如: https://api.anthropic.com"
                  value={formData.api_base}
                  onChange={(e) =>
                    setFormData({ ...formData, api_base: e.target.value })
                  }
                />
              </div>

              {/* 验证按钮 */}
              <div className="flex items-center gap-3">
                <Button
                  type="button"
                  variant="outline"
                  onClick={handleVerify}
                  disabled={verifying || (!formData.api_key && !model.is_verified)}
                >
                  {verifying && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                  {verifying ? "验证中..." : "🔍 验证配置"}
                </Button>
                
                {verificationResult && (
                  <div className={`flex items-center gap-2 text-sm ${
                    verificationResult.success ? "text-green-600" : "text-red-600"
                  }`}>
                    {verificationResult.success ? (
                      <CheckCircle2 className="h-4 w-4" />
                    ) : (
                      <XCircle className="h-4 w-4" />
                    )}
                    <span className="font-medium">
                      {verificationResult.success ? "验证通过" : "验证失败"}
                    </span>
                  </div>
                )}
              </div>

              {/* 详细验证结果 */}
              {verificationResult?.details && (
                <div className="bg-gray-50 p-3 rounded-md text-sm space-y-1">
                  {verificationResult.details.litellm_test && (
                    <div className={`flex items-center gap-2 ${
                      verificationResult.details.litellm_test.success ? "text-green-600" : "text-red-600"
                    }`}>
                      {verificationResult.details.litellm_test.success ? "✓" : "✗"}
                      LiteLLM测试: {verificationResult.details.litellm_test.message}
                    </div>
                  )}
                  {verificationResult.details.rdagent_health_check && (
                    <div className={`flex items-center gap-2 ${
                      verificationResult.details.rdagent_health_check.success ? "text-green-600" : "text-red-600"
                    }`}>
                      {verificationResult.details.rdagent_health_check.success ? "✓" : "✗"}
                      RDAgent健康检查: {verificationResult.details.rdagent_health_check.message}
                    </div>
                  )}
                </div>
              )}
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
            <Button 
              type="submit" 
              disabled={loading || verifying || !verificationResult?.success}
            >
              {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              {loading ? "保存中..." : verificationResult?.success ? "保存配置" : "请先验证"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
