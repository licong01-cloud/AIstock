"use client";

import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Plus, ChevronDown, ChevronUp } from "lucide-react";
import ModelItem from "./ModelItem";
import AddModelDialog from "./AddModelDialog";
import EditModelDialog from "./EditModelDialog";

interface Provider {
  id: number;
  provider_name: string;
  display_name: string;
  api_base_url: string;
  litellm_prefix: string;
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

interface ProviderCardProps {
  provider: Provider;
  models: Model[];
  onModelAdded: () => void;
  onError: (message: string) => void;
  apiBase: string;
}

export default function ProviderCard({
  provider,
  models,
  onModelAdded,
  onError,
  apiBase,
}: ProviderCardProps) {
  const [isExpanded, setIsExpanded] = useState(true);
  const [showAddDialog, setShowAddDialog] = useState(false);
  const [showEditDialog, setShowEditDialog] = useState(false);
  const [editingModel, setEditingModel] = useState<Model | null>(null);

  const handleEditModel = (model: Model) => {
    setEditingModel(model);
    setShowEditDialog(true);
  };

  const handleEditSuccess = () => {
    setShowEditDialog(false);
    setEditingModel(null);
    onModelAdded(); // 重新加载模型列表
  };

  const getCapabilityBadges = () => {
    const badges = [];
    if (provider.supports_chat) badges.push("对话");
    if (provider.supports_reasoner) badges.push("推理");
    if (provider.supports_embedding) badges.push("嵌入");
    return badges;
  };

  return (
    <>
      <Card className="border-2">
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <CardTitle className="text-xl">{provider.display_name}</CardTitle>
              <div className="flex gap-1">
                {getCapabilityBadges().map((badge) => (
                  <Badge key={badge} variant="secondary" className="text-xs">
                    {badge}
                  </Badge>
                ))}
              </div>
            </div>
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => setShowAddDialog(true)}
              >
                <Plus className="h-4 w-4 mr-1" />
                添加模型
              </Button>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setIsExpanded(!isExpanded)}
              >
                {isExpanded ? (
                  <ChevronUp className="h-4 w-4" />
                ) : (
                  <ChevronDown className="h-4 w-4" />
                )}
              </Button>
            </div>
          </div>
          <div className="text-sm text-gray-500 mt-1">
            <div>API Base: {provider.api_base_url}</div>
            <div>Prefix: {provider.litellm_prefix}</div>
          </div>
        </CardHeader>

        {isExpanded && (
          <CardContent className="pt-0">
            {models.length === 0 ? (
              <div className="text-center py-8 text-gray-500">
                暂无模型，点击"添加模型"开始配置
              </div>
            ) : (
              <div className="space-y-2">
                {models.map((model) => (
                  <ModelItem
                    key={model.id}
                    model={model}
                    onError={onError}
                    onEdit={handleEditModel}
                    apiBase={apiBase}
                  />
                ))}
              </div>
            )}
          </CardContent>
        )}
      </Card>

      <AddModelDialog
        open={showAddDialog}
        onOpenChange={setShowAddDialog}
        provider={provider}
        onSuccess={onModelAdded}
        onError={onError}
        apiBase={apiBase}
      />

      <EditModelDialog
        open={showEditDialog}
        onOpenChange={setShowEditDialog}
        model={editingModel}
        providers={[provider]}
        onSuccess={handleEditSuccess}
        onError={onError}
        apiBase={apiBase}
      />
    </>
  );
}
