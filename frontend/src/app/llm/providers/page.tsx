"use client";

import { useState, useEffect } from "react";
import {
  Table,
  Button,
  Modal,
  Form,
  Input,
  Select,
  Switch,
  message,
  Space,
  Tag,
  Card,
  Popconfirm,
  Tooltip,
  Badge,
} from "antd";
import {
  PlusOutlined,
  SyncOutlined,
  DeleteOutlined,
  EditOutlined,
  ApiOutlined,
  RobotOutlined,
} from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";

const { Option } = Select;

interface Provider {
  id: number;
  provider_name: string;
  display_name: string;
  api_base_url: string;
  litellm_prefix: string;
  provider_type: string;
  default_env_prefix: string | null;
  use_proxy: boolean;
  proxy_model_prefix: string | null;
  supports_chat: boolean;
  supports_embedding: boolean;
  supports_reasoner: boolean;
  supports_vision: boolean;
  is_active: boolean;
}

interface ProviderStats {
  provider_id: number;
  provider_name: string;
  display_name: string;
  litellm_prefix: string;
  provider_type: string;
  use_proxy: boolean;
  total_models: number;
  chat_models: number;
  embedding_models: number;
  reasoner_models: number;
  vision_models: number;
  synced_models: number;
}

export default function LLMProvidersPage() {
  const [providers, setProviders] = useState<Provider[]>([]);
  const [stats, setStats] = useState<ProviderStats[]>([]);
  const [loading, setLoading] = useState(false);
  const [modalVisible, setModalVisible] = useState(false);
  const [editingProvider, setEditingProvider] = useState<Provider | null>(null);
  const [form] = Form.useForm();

  useEffect(() => {
    fetchProviders();
    fetchStats();
  }, []);

  const fetchProviders = async () => {
    setLoading(true);
    try {
      const response = await fetch("/api/v1/llm/providers");
      const data = await response.json();
      setProviders(data.providers || []);
    } catch (error) {
      message.error("获取服务商列表失败");
    } finally {
      setLoading(false);
    }
  };

  const fetchStats = async () => {
    try {
      const response = await fetch("/api/v1/llm/providers/stats");
      const data = await response.json();
      setStats(data.stats || []);
    } catch (error) {
      console.error("获取统计信息失败", error);
    }
  };

  const handleCreate = () => {
    setEditingProvider(null);
    form.resetFields();
    form.setFieldsValue({
      provider_type: "official",
      use_proxy: false,
      supports_chat: true,
      supports_embedding: false,
      supports_reasoner: false,
      supports_vision: false,
    });
    setModalVisible(true);
  };

  const handleEdit = (provider: Provider) => {
    setEditingProvider(provider);
    form.setFieldsValue(provider);
    setModalVisible(true);
  };

  const handleDelete = async (providerId: number) => {
    try {
      const response = await fetch(`/api/v1/llm/providers/${providerId}`, {
        method: "DELETE",
      });
      const data = await response.json();
      if (data.success) {
        message.success("删除成功");
        fetchProviders();
        fetchStats();
      } else {
        message.error("删除失败");
      }
    } catch (error) {
      message.error("删除失败");
    }
  };

  const handleSubmit = async (values: any) => {
    try {
      if (editingProvider) {
        const response = await fetch(
          `/api/v1/llm/providers/${editingProvider.id}`,
          {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(values),
          }
        );
        const data = await response.json();
        if (data.success) {
          message.success("更新成功");
          setModalVisible(false);
          fetchProviders();
          fetchStats();
        } else {
          message.error(data.detail || "更新失败");
        }
      } else {
        const response = await fetch("/api/v1/llm/providers", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(values),
        });
        const data = await response.json();
        if (data.success) {
          message.success("创建成功");
          setModalVisible(false);
          fetchProviders();
          fetchStats();
        } else {
          message.error(data.detail || "创建失败");
        }
      }
    } catch (error) {
      message.error("操作失败");
    }
  };

  const getStatsForProvider = (providerId: number) => {
    return stats.find((s) => s.provider_id === providerId);
  };

  const columns: ColumnsType<Provider> = [
    {
      title: "服务商名称",
      dataIndex: "display_name",
      key: "display_name",
      width: 150,
    },
    {
      title: "标识",
      dataIndex: "provider_name",
      key: "provider_name",
      width: 120,
    },
    {
      title: "类型",
      dataIndex: "provider_type",
      key: "provider_type",
      width: 100,
      render: (type: string) => {
        const typeMap: Record<string, { color: string; text: string }> = {
          official: { color: "blue", text: "官方" },
          agent: { color: "green", text: "代理商" },
          platform: { color: "orange", text: "平台" },
          proxy: { color: "purple", text: "代理服务" },
        };
        const config = typeMap[type] || { color: "default", text: type };
        return <Tag color={config.color}>{config.text}</Tag>;
      },
    },
    {
      title: "LiteLLM前缀",
      dataIndex: "litellm_prefix",
      key: "litellm_prefix",
      width: 120,
    },
    {
      title: "支持类型",
      key: "supports",
      width: 180,
      render: (_, record) => (
        <Space size="small">
          {record.supports_chat && <Tag color="blue">对话</Tag>}
          {record.supports_embedding && <Tag color="green">嵌入</Tag>}
          {record.supports_reasoner && <Tag color="orange">推理</Tag>}
          {record.supports_vision && <Tag color="purple">视觉</Tag>}
        </Space>
      ),
    },
    {
      title: "模型数量",
      key: "model_count",
      width: 100,
      render: (_, record) => {
        const s = getStatsForProvider(record.id);
        return (
          <Badge
            count={s?.total_models || 0}
            showZero
            color={s?.total_models ? "blue" : "default"}
          />
        );
      },
    },
    {
      title: "Proxy",
      dataIndex: "use_proxy",
      key: "use_proxy",
      width: 80,
      render: (useProxy: boolean) => (
        <Tag color={useProxy ? "green" : "default"}>
          {useProxy ? "是" : "否"}
        </Tag>
      ),
    },
    {
      title: "操作",
      key: "actions",
      width: 150,
      render: (_, record) => (
        <Space>
          <Tooltip title="同步模型">
            <Button
              type="link"
              icon={<SyncOutlined />}
              href={`/llm/models?provider_id=${record.id}`}
            />
          </Tooltip>
          <Button
            type="link"
            icon={<EditOutlined />}
            onClick={() => handleEdit(record)}
          />
          <Popconfirm
            title="确定删除此服务商？"
            onConfirm={() => handleDelete(record.id)}
          >
            <Button type="link" danger icon={<DeleteOutlined />} />
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <div style={{ padding: 24 }}>
      <Card
        title={
          <Space>
            <ApiOutlined />
            <span>LLM服务商管理</span>
          </Space>
        }
        extra={
          <Button type="primary" icon={<PlusOutlined />} onClick={handleCreate}>
            添加服务商
          </Button>
        }
      >
        <Table
          columns={columns}
          dataSource={providers}
          rowKey="id"
          loading={loading}
          pagination={{ pageSize: 10 }}
        />
      </Card>

      <Modal
        title={editingProvider ? "编辑服务商" : "添加服务商"}
        open={modalVisible}
        onCancel={() => setModalVisible(false)}
        onOk={() => form.submit()}
        width={600}
      >
        <Form form={form} layout="vertical" onFinish={handleSubmit}>
          <Form.Item
            name="provider_name"
            label="服务商标识"
            rules={[{ required: true, message: "请输入服务商标识" }]}
          >
            <Input placeholder="如: deepseek, siliconflow" disabled={!!editingProvider} />
          </Form.Item>

          <Form.Item
            name="display_name"
            label="显示名称"
            rules={[{ required: true, message: "请输入显示名称" }]}
          >
            <Input placeholder="如: DeepSeek, 硅基流动" />
          </Form.Item>

          <Form.Item
            name="api_base_url"
            label="API Base URL"
            rules={[{ required: true, message: "请输入API Base URL" }]}
          >
            <Input placeholder="https://api.example.com/v1" />
          </Form.Item>

          <Form.Item
            name="litellm_prefix"
            label="LiteLLM前缀"
            rules={[{ required: true, message: "请输入LiteLLM前缀" }]}
          >
            <Input placeholder="如: deepseek, openai, dashscope" />
          </Form.Item>

          <Form.Item name="provider_type" label="服务商类型">
            <Select>
              <Option value="official">官方</Option>
              <Option value="agent">代理商</Option>
              <Option value="platform">平台</Option>
              <Option value="proxy">代理服务</Option>
            </Select>
          </Form.Item>

          <Form.Item name="default_env_prefix" label="环境变量前缀">
            <Input placeholder="如: DEEPSEEK, OPENAI" />
          </Form.Item>

          <Form.Item name="use_proxy" label="使用Proxy" valuePropName="checked">
            <Switch />
          </Form.Item>

          <Form.Item
            name="proxy_model_prefix"
            label="Proxy模型前缀"
            extra="用于生成Proxy模型别名"
          >
            <Input placeholder="如: sf, ds, bq" />
          </Form.Item>

          <Form.Item label="支持的模型类型">
            <Space>
              <Form.Item name="supports_chat" valuePropName="checked" noStyle>
                <Switch checkedChildren="对话" unCheckedChildren="对话" />
              </Form.Item>
              <Form.Item name="supports_embedding" valuePropName="checked" noStyle>
                <Switch checkedChildren="嵌入" unCheckedChildren="嵌入" />
              </Form.Item>
              <Form.Item name="supports_reasoner" valuePropName="checked" noStyle>
                <Switch checkedChildren="推理" unCheckedChildren="推理" />
              </Form.Item>
              <Form.Item name="supports_vision" valuePropName="checked" noStyle>
                <Switch checkedChildren="视觉" unCheckedChildren="视觉" />
              </Form.Item>
            </Space>
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}
