"use client";

import { useState, useEffect } from "react";
import {
  Table,
  Button,
  Select,
  InputNumber,
  message,
  Space,
  Tag,
  Card,
  Divider,
  Alert,
  Tooltip,
  Spin,
} from "antd";
import { SaveOutlined, EyeOutlined, ReloadOutlined } from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";

const { Option } = Select;

interface Stage {
  id: number;
  stage_name: string;
  stage_display_name: string;
  description: string | null;
  model_id: number | null;
  model_display_name: string | null;
  full_model_id: string | null;
  provider_display_name: string | null;
  temperature: number | null;
  max_tokens: number | null;
}

interface Provider {
  id: number;
  provider_name: string;
  display_name: string;
}

interface Model {
  id: number;
  provider_id: number;
  display_name: string;
  full_model_id: string;
  model_type: string;
}

interface ConfigPreview {
  stage_mappings: Record<string, { model: string; temperature?: string; max_tokens?: string }>;
  env_variables: Record<string, string>;
  litellm_chat_model_map: Record<string, { model: string; temperature?: string; max_tokens?: string }>;
}

export default function RDAgentLLMConfigPage() {
  const [stages, setStages] = useState<Stage[]>([]);
  const [providers, setProviders] = useState<Provider[]>([]);
  const [models, setModels] = useState<Model[]>([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState<string | null>(null);
  const [configPreview, setConfigPreview] = useState<ConfigPreview | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);

  // 编辑状态
  const [editingStage, setEditingStage] = useState<string | null>(null);
  const [editProviderId, setEditProviderId] = useState<number | null>(null);
  const [editModelId, setEditModelId] = useState<number | null>(null);
  const [editTemperature, setEditTemperature] = useState<number | null>(null);
  const [editMaxTokens, setEditMaxTokens] = useState<number | null>(null);

  useEffect(() => {
    fetchStages();
    fetchProviders();
    fetchModels();
    fetchConfigPreview();
  }, []);

  const fetchStages = async () => {
    setLoading(true);
    try {
      const response = await fetch("/api/v1/llm/rdagent/stages");
      const data = await response.json();
      setStages(data.stages || []);
    } catch (error) {
      message.error("获取阶段配置失败");
    } finally {
      setLoading(false);
    }
  };

  const fetchProviders = async () => {
    try {
      const response = await fetch("/api/v1/llm/providers");
      const data = await response.json();
      setProviders(data.providers || []);
    } catch (error) {
      console.error("获取服务商列表失败", error);
    }
  };

  const fetchModels = async () => {
    try {
      const response = await fetch("/api/v1/llm/models?model_type=chat");
      const data = await response.json();
      setModels(data.models || []);
    } catch (error) {
      console.error("获取模型列表失败", error);
    }
  };

  const fetchConfigPreview = async () => {
    setPreviewLoading(true);
    try {
      const response = await fetch("/api/v1/llm/rdagent/config-preview");
      const data = await response.json();
      setConfigPreview(data);
    } catch (error) {
      console.error("获取配置预览失败", error);
    } finally {
      setPreviewLoading(false);
    }
  };

  const startEditing = (stage: Stage) => {
    setEditingStage(stage.stage_name);
    // 找到当前模型对应的provider
    const currentModel = models.find((m) => m.id === stage.model_id);
    setEditProviderId(currentModel?.provider_id || null);
    setEditModelId(stage.model_id);
    setEditTemperature(stage.temperature);
    setEditMaxTokens(stage.max_tokens);
  };

  const cancelEditing = () => {
    setEditingStage(null);
    setEditProviderId(null);
    setEditModelId(null);
    setEditTemperature(null);
    setEditMaxTokens(null);
  };

  const saveStage = async (stageName: string) => {
    if (!editModelId) {
      message.warning("请选择模型");
      return;
    }

    setSaving(stageName);
    try {
      const response = await fetch(`/api/v1/llm/rdagent/stages/${stageName}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          provider_id: editProviderId,
          model_id: editModelId,
          temperature: editTemperature,
          max_tokens: editMaxTokens,
        }),
      });

      const data = await response.json();
      if (data.success) {
        message.success(`阶段 ${stageName} 配置已保存`);
        cancelEditing();
        fetchStages();
        fetchConfigPreview();
      } else {
        message.error(data.detail || "保存失败");
      }
    } catch (error) {
      message.error("保存失败");
    } finally {
      setSaving(null);
    }
  };

  // 根据选择的服务商过滤模型
  const getFilteredModels = (providerId: number | null) => {
    if (!providerId) return [];
    return models.filter((m) => m.provider_id === providerId);
  };

  const columns: ColumnsType<Stage> = [
    {
      title: "阶段",
      dataIndex: "stage_display_name",
      key: "stage_display_name",
      width: 120,
      render: (text: string, record: Stage) => (
        <Tooltip title={record.description}>
          <span>{text}</span>
        </Tooltip>
      ),
    },
    {
      title: "服务商",
      key: "provider",
      width: 180,
      render: (_, record: Stage) => {
        if (editingStage === record.stage_name) {
          return (
            <Select
              style={{ width: "100%" }}
              placeholder="选择服务商"
              value={editProviderId}
              onChange={(value) => {
                setEditProviderId(value);
                setEditModelId(null); // 切换服务商时清空模型选择
              }}
            >
              {providers.map((p) => (
                <Option key={p.id} value={p.id}>
                  {p.display_name}
                </Option>
              ))}
            </Select>
          );
        }
        return record.provider_display_name || <Tag color="red">未配置</Tag>;
      },
    },
    {
      title: "模型",
      key: "model",
      width: 200,
      render: (_, record: Stage) => {
        if (editingStage === record.stage_name) {
          return (
            <Select
              style={{ width: "100%" }}
              placeholder="选择模型"
              value={editModelId}
              onChange={setEditModelId}
              disabled={!editProviderId}
              showSearch
              optionFilterProp="children"
            >
              {getFilteredModels(editProviderId).map((m) => (
                <Option key={m.id} value={m.id}>
                  {m.display_name}
                </Option>
              ))}
            </Select>
          );
        }
        return record.model_display_name ? (
          <Tooltip title={record.full_model_id}>
            <span>{record.model_display_name}</span>
          </Tooltip>
        ) : (
          <Tag color="red">未配置</Tag>
        );
      },
    },
    {
      title: "Temperature",
      dataIndex: "temperature",
      key: "temperature",
      width: 120,
      render: (value: number | null, record: Stage) => {
        if (editingStage === record.stage_name) {
          return (
            <InputNumber
              min={0}
              max={2}
              step={0.1}
              value={editTemperature}
              onChange={setEditTemperature}
              style={{ width: "100%" }}
            />
          );
        }
        return value !== null ? value.toFixed(2) : "-";
      },
    },
    {
      title: "Max Tokens",
      dataIndex: "max_tokens",
      key: "max_tokens",
      width: 120,
      render: (value: number | null, record: Stage) => {
        if (editingStage === record.stage_name) {
          return (
            <InputNumber
              min={100}
              max={100000}
              step={100}
              value={editMaxTokens}
              onChange={setEditMaxTokens}
              style={{ width: "100%" }}
            />
          );
        }
        return value || "-";
      },
    },
    {
      title: "操作",
      key: "actions",
      width: 150,
      render: (_, record: Stage) => {
        if (editingStage === record.stage_name) {
          return (
            <Space>
              <Button
                type="primary"
                size="small"
                icon={<SaveOutlined />}
                loading={saving === record.stage_name}
                onClick={() => saveStage(record.stage_name)}
              >
                保存
              </Button>
              <Button size="small" onClick={cancelEditing}>
                取消
              </Button>
            </Space>
          );
        }
        return (
          <Button size="small" onClick={() => startEditing(record)}>
            配置
          </Button>
        );
      },
    },
  ];

  return (
    <div style={{ padding: 24 }}>
      <Card
        title="RDAgent LLM配置"
        extra={
          <Space>
            <Button icon={<ReloadOutlined />} onClick={fetchStages}>
              刷新
            </Button>
          </Space>
        }
      >
        <Alert
          message="配置说明"
          description="为RDAgent的各个阶段选择模型。先选择服务商，再选择该服务商提供的模型。配置完成后会自动更新RDAgent的.env文件。"
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
        />

        <Table
          columns={columns}
          dataSource={stages}
          rowKey="stage_name"
          loading={loading}
          pagination={false}
        />

        <Divider />

        <Card
          type="inner"
          title={
            <Space>
              <EyeOutlined />
              <span>配置预览</span>
            </Space>
          }
          extra={
            <Button
              size="small"
              icon={<ReloadOutlined />}
              onClick={fetchConfigPreview}
              loading={previewLoading}
            >
              刷新预览
            </Button>
          }
        >
          <Spin spinning={previewLoading}>
            {configPreview && (
              <div>
                <h4>LITELLM_CHAT_MODEL_MAP:</h4>
                <pre style={{ background: "#f5f5f5", padding: 12, overflow: "auto" }}>
                  {JSON.stringify(configPreview.litellm_chat_model_map, null, 2)}
                </pre>

                <h4>环境变量:</h4>
                <pre style={{ background: "#f5f5f5", padding: 12, overflow: "auto" }}>
                  {Object.entries(configPreview.env_variables || {})
                    .map(([k, v]) => `${k}=${v}`)
                    .join("\n")}
                </pre>
              </div>
            )}
          </Spin>
        </Card>
      </Card>
    </div>
  );
}
