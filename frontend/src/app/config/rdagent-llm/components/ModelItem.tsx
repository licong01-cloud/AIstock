"use client";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { CheckCircle2, XCircle, RefreshCw, Pencil } from "lucide-react";

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

interface ModelItemProps {
  model: Model;
  onError: (message: string) => void;
  onEdit?: (model: Model) => void;
  apiBase: string;
}

export default function ModelItem({ model, onError, onEdit, apiBase }: ModelItemProps) {
  const getModelTypeColor = (type: string) => {
    switch (type) {
      case "chat":
        return "bg-blue-100 text-blue-800";
      case "embedding":
        return "bg-green-100 text-green-800";
      case "reasoner":
        return "bg-purple-100 text-purple-800";
      default:
        return "bg-gray-100 text-gray-800";
    }
  };

  const getModelTypeLabel = (type: string) => {
    switch (type) {
      case "chat":
        return "对话";
      case "embedding":
        return "嵌入";
      case "reasoner":
        return "推理";
      default:
        return type;
    }
  };

  return (
    <div className="flex items-center justify-between p-3 border rounded-lg hover:bg-gray-50">
      <div className="flex-1">
        <div className="flex items-center gap-2 mb-1">
          <span className="font-medium">{model.display_name}</span>
          <Badge className={getModelTypeColor(model.model_type)}>
            {getModelTypeLabel(model.model_type)}
          </Badge>
          {model.model_category && (
            <Badge variant="outline" className="text-xs">
              {model.model_category}
            </Badge>
          )}
          {model.is_verified ? (
            <CheckCircle2 className="h-4 w-4 text-green-600" />
          ) : (
            <XCircle className="h-4 w-4 text-red-600" />
          )}
        </div>
        <div className="text-sm text-gray-600">
          <div>模型ID: {model.full_model_id}</div>
          {model.description && <div className="mt-1">说明: {model.description}</div>}
          {model.last_verified_at && (
            <div className="text-xs text-gray-400 mt-1">
              最后验证: {new Date(model.last_verified_at).toLocaleString("zh-CN")}
            </div>
          )}
        </div>
      </div>
      <div className="flex items-center gap-2">
        <Button 
          variant="ghost" 
          size="sm"
          onClick={() => onEdit?.(model)}
          title="编辑模型配置"
        >
          <Pencil className="h-4 w-4" />
        </Button>
        <Button variant="ghost" size="sm" disabled>
          <RefreshCw className="h-4 w-4" />
        </Button>
      </div>
    </div>
  );
}
