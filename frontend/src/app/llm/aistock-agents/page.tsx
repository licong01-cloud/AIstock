"use client";

import { useState, useEffect } from "react";
import {
  Table,
  Button,
  Select,
  InputNumber,
  Input,
  message,
  Space,
  Tag,
  Card,
  Divider,
  Alert,
  Tooltip,
  Modal,
} from "antd";
import { SaveOutlined, EditOutlined, RobotOutlined } from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";

const { Option } = Select;
const { TextArea } = Input;

interface Agent {
  id: number;
  agent_key: string;
  agent_name: string;
  agent_type: string | null;
  description: string | null;
  model_id: number | null;
  model_display_name: string | null;
  full_model_id: string | null;
  provider_display_name: string | null;
  temperature: number | null;
  max_tokens: number | null;
  system_prompt: string | null;
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

export default function AIstockAgentConfigPage() {
  const [agents, setAgents] = useState<Agent[]>([]);
  const [providers, setProviders] = useState<Provider[]>([]);
  const [models, setModels] = useState<Model[]>([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState<string | null>(null);

  // 编辑状态
  const [editingAgent, setEditingAgent] = useState<string | null>(null);
  const [editProviderId, setEditProviderId] = useState<number | null>(null);
  const [editModelId, setEditModelId] = useState<number | null>(null);
  const [editTemperature, setEditTemperature] = useState<number | null>(null);
  const [editMaxTokens, setEditMaxTokens] = useState<number | null>(null);
  const [editSystemPrompt, setEditSystemPrompt] = useState<string | null>(null);

  // 系统提示词弹窗
  const [promptModalVisible, setPromptModalVisible] = useState(false);
  const [viewingPrompt, setViewingPrompt] = useState<string | null>(null);

  useEffect(() => {
    fetchAgents();
    fetchProviders();
    fetchModels();
  }, []);

  const fetchAgents = async () => {
    setLoading(true);
    try {
      const response = await fetch("/api/v1/llm/aistock/agents");
      const data = await response.json();
      setAgents(data.agents || []);
    } catch (error) {
      message.error("获取Agent配置失败");
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
      const response = await fetch("/api/v1/llm/models");
      const data = await response.json();
      setModels(data.models || []);
    } catch (error) {
      console.error("获取模型列表失败", error);
    }
  };

  const startEditing = (agent: Agent) => {
    setEditingAgent(agent.agent_key);
    const currentModel = models.find((m) => m.id === agent.model_id);
    setEditProviderId(currentModel?.provider_id || null);
    setEditModelId(agent.model_id);
    setEditTemperature(agent.temperature);
    setEditMaxTokens(agent.max_tokens);
    setEditSystemPrompt(agent.system_prompt);
  };

  const cancelEditing = () => {
    setEditingAgent(null);
    setEditProviderId(null);
    setEditModelId(null);
    setEditTemperature(null);
    setEditMaxTokens(null);
    setEditSystemPrompt(null);
  };

  const saveAgent = async (agentKey: string) => {
    if (!editModelId) {
      message.warning("请选择模型");
      return;
    }

    setSaving(agentKey);
    try {
      const response = await fetch(`/api/v1/llm/aistock/agents/${agentKey}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          provider_id: editProviderId,
          model_id: editModelId,
          temperature: editTemperature,
          max_tokens: editMaxTokens,
          system_prompt: editSystemPrompt,
        }),
      });

      const data = await response.json();
      if (data.success) {
        message.success(`Agent ${agentKey} 配置已保存`);
        cancelEditing();
        fetchAgents();
      } else {
        message.error(data.detail || "保存失败");
      }
    } catch (error) {
      message.error("保存失败");
    } finally {
      setSaving(null);
    }
  };

  const getFilteredModels = (providerId: number | null) => {
    if (!providerId) return [];
    return models.filter((m) => m.provider_id === providerId);
  };

  const getAgentTypeTag = (type: string | null) => {
    if (!type) return null;
    const typeConfig: Record<string, { color: string; text: string }> = {
      analysis: { color: "blue", text: "分析" },
      generation: { color: "green", text: "生成" },
      processing: { color: "orange", text: "处理" },
    };
    const config = typeConfig[type] || { color: "default", text: type };
    return <Tag color={config.color}>{config.text}</Tag>;
  };

  const columns: ColumnsType<Agent> = [
    {
      title: "Agent",
      key: "agent",
      width: 180,
      render: (_, record: Agent) => (
        <div>
          <div style={{ fontWeight: 500 }}>{record.agent_name}</div>
          <div style={{ fontSize: 12, color: "#999" }}>{record.agent_key}</div>
        </div>
      ),
    },
    {
      title: "类型",
      dataIndex: "agent_type",
      key: "agent_type",
      width: 80,
      render: (type: string | null) => getAgentTypeTag(type),
    },
    {
      title: "服务商",
      key: "provider",
      width: 160,
      render: (_, record: Agent) => {
        if (editingAgent === record.agent_key) {
          return (
            <Select
              style={{ width: "100%" }}
              placeholder="选择服务商"
              value={editProviderId}
              onChange={(value) => {
                setEditProviderId(value);
                setEditModelId(null);
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
      width: 180,
      render: (_, record: Agent) => {
        if (editingAgent === record.agent_key) {
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
      width: 100,
      render: (value: number | null, record: Agent) => {
        if (editingAgent === record.agent_key) {
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
      width: 100,
      render: (value: number | null, record: Agent) => {
        if (editingAgent === record.agent_key) {
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
      title: "系统提示词",
      key: "system_prompt",
      width: 100,
      render: (_, record: Agent) => {
        if (editingAgent === record.agent_key) {
          return (
            <Button
              size="small"
              onClick={() => {
                Modal.confirm({
                  title: "编辑系统提示词",
                  content: (
                    <TextArea
                      rows={10}
                      defaultValue={editSystemPrompt || ""}
                      onChange={(e) => setEditSystemPrompt(e.target.value)}
                    />
                  ),
                  onOk: () => {},
                });
              }}
            >
              编辑
            </Button>
          );
        }
        if (record.system_prompt) {
          return (
            <Button
              size="small"
              onClick={() => {
                setViewingPrompt(record.system_prompt);
                setPromptModalVisible(true);
              }}
            >
              查看
            </Button>
          );
        }
        return <Tag>默认</Tag>;
      },
    },
    {
      title: "操作",
      key: "actions",
      width: 150,
      render: (_, record: Agent) => {
        if (editingAgent === record.agent_key) {
          return (
            <Space>
              <Button
                type="primary"
                size="small"
                icon={<SaveOutlined />}
                loading={saving === record.agent_key}
                onClick={() => saveAgent(record.agent_key)}
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
          <Button
            size="small"
            icon={<EditOutlined />}
            onClick={() => startEditing(record)}
          >
            配置
          </Button>
        );
      },
    },
  ];

  return (
    <div style={{ padding: 24 }}>
      <Card
        title={
          <Space>
            <RobotOutlined />
            <span>AIstock Agent LLM配置</span>
          </Space>
        }
      >
        <Alert
          message="配置说明"
          description="为AIstock的各个Agent选择模型。先选择服务商，再选择该服务商提供的模型。配置保存在数据库中，AIstock运行时从数据库读取配置。"
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
        />

        <Table
          columns={columns}
          dataSource={agents}
          rowKey="agent_key"
          loading={loading}
          pagination={false}
        />
      </Card>

      {/* 系统提示词查看弹窗 */}
      <Modal
        title="系统提示词"
        open={promptModalVisible}
        onCancel={() => setPromptModalVisible(false)}
        footer={null}
        width={600}
      >
        <pre
          style={{
            background: "#f5f5f5",
            padding: 12,
            overflow: "auto",
            maxHeight: 400,
            whiteSpace: "pre-wrap",
          }}
        >
          {viewingPrompt}
        </pre>
      </Modal>
    </div>
  );
}
