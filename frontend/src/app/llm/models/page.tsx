"use client";

import { useState, useEffect } from "react";
import {
  Table,
  Button,
  Modal,
  Form,
  Input,
  Select,
  message,
  Space,
  Tag,
  Card,
  Popconfirm,
  Tooltip,
  Checkbox,
  Radio,
  Badge,
  Divider,
  Alert,
} from "antd";
import {
  PlusOutlined,
  SyncOutlined,
  DeleteOutlined,
  DownloadOutlined,
  CheckCircleOutlined,
  CloudDownloadOutlined,
} from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";

const { Option } = Select;
const { Search } = Input;

interface Provider {
  id: number;
  provider_name: string;
  display_name: string;
  litellm_prefix: string;
  use_proxy: boolean;
}

interface Model {
  id: number;
  provider_id: number;
  provider_display_name: string;
  model_name: string;
  display_name: string;
  full_model_id: string;
  model_type: string;
  model_category: string | null;
  context_window: number | null;
  max_output_tokens: number | null;
  input_price: number | null;
  output_price: number | null;
  proxy_model_alias: string | null;
  is_synced: boolean;
}

interface FetchedModel {
  model_name: string;
  model_type: string;
  context_window: number | null;
  max_output_tokens: number | null;
  input_price: number | null;
  output_price: number | null;
}

export default function LLMModelsPage() {
  const [models, setModels] = useState<Model[]>([]);
  const [providers, setProviders] = useState<Provider[]>([]);
  const [loading, setLoading] = useState(false);
  const [selectedProviderId, setSelectedProviderId] = useState<number | null>(null);
  const [selectedModelType, setSelectedModelType] = useState<string>("all");

  // 同步模型相关状态
  const [syncModalVisible, setSyncModalVisible] = useState(false);
  const [syncingProviderId, setSyncingProviderId] = useState<number | null>(null);
  const [syncModelType, setSyncModelType] = useState<string>("all");
  const [fetchedModels, setFetchedModels] = useState<FetchedModel[]>([]);
  const [selectedModelNames, setSelectedModelNames] = useState<string[]>([]);
  const [fetching, setFetching] = useState(false);
  const [importing, setImporting] = useState(false);

  // 添加模型相关状态
  const [addModalVisible, setAddModalVisible] = useState(false);
  const [addForm] = Form.useForm();

  useEffect(() => {
    fetchProviders();
    fetchModels();
  }, []);

  useEffect(() => {
    fetchModels();
  }, [selectedProviderId, selectedModelType]);

  const fetchProviders = async () => {
    try {
      const response = await fetch("/api/v1/llm/providers");
      const data = await response.json();
      setProviders(data.providers || []);
    } catch (error) {
      message.error("获取服务商列表失败");
    }
  };

  const fetchModels = async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams();
      if (selectedProviderId) {
        params.append("provider_id", selectedProviderId.toString());
      }
      if (selectedModelType && selectedModelType !== "all") {
        params.append("model_type", selectedModelType);
      }

      const response = await fetch(`/api/v1/llm/models?${params.toString()}`);
      const data = await response.json();
      setModels(data.models || []);
    } catch (error) {
      message.error("获取模型列表失败");
    } finally {
      setLoading(false);
    }
  };

  // 打开同步模型弹窗
  const handleOpenSyncModal = () => {
    setSyncingProviderId(null);
    setSyncModelType("all");
    setFetchedModels([]);
    setSelectedModelNames([]);
    setSyncModalVisible(true);
  };

  // 从服务商API获取模型列表
  const handleFetchModels = async () => {
    if (!syncingProviderId) {
      message.warning("请先选择服务商");
      return;
    }

    setFetching(true);
    try {
      const response = await fetch(
        `/api/v1/llm/providers/${syncingProviderId}/models/fetch`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ model_type: syncModelType === "all" ? null : syncModelType }),
        }
      );
      const data = await response.json();
      if (data.success) {
        setFetchedModels(data.models || []);
        message.success(`获取到 ${data.models?.length || 0} 个模型`);
      } else {
        message.error(data.error || "获取模型列表失败");
      }
    } catch (error) {
      message.error("获取模型列表失败");
    } finally {
      setFetching(false);
    }
  };

  // 批量导入选中的模型
  const handleImportModels = async () => {
    if (selectedModelNames.length === 0) {
      message.warning("请选择要导入的模型");
      return;
    }

    setImporting(true);
    try {
      const modelsToImport = fetchedModels
        .filter((m) => selectedModelNames.includes(m.model_name))
        .map((m) => ({
          model_name: m.model_name,
          display_name: `${m.model_name}`,
          model_type: m.model_type,
          context_window: m.context_window,
          max_output_tokens: m.max_output_tokens,
          input_price: m.input_price,
          output_price: m.output_price,
        }));

      const response = await fetch("/api/v1/llm/models/batch-import", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          provider_id: syncingProviderId,
          models: modelsToImport,
        }),
      });

      const data = await response.json();
      if (data.success) {
        message.success(`成功导入 ${data.imported_count} 个模型`);
        setSyncModalVisible(false);
        fetchModels();
      } else {
        message.error(data.error || "导入失败");
      }
    } catch (error) {
      message.error("导入失败");
    } finally {
      setImporting(false);
    }
  };

  const handleDeleteModel = async (modelId: number) => {
    try {
      const response = await fetch(`/api/v1/llm/models/${modelId}`, {
        method: "DELETE",
      });
      const data = await response.json();
      if (data.success) {
        message.success("删除成功");
        fetchModels();
      } else {
        message.error("删除失败");
      }
    } catch (error) {
      message.error("删除失败");
    }
  };

  const handleAddModel = async (values: any) => {
    try {
      const response = await fetch("/api/v1/llm/models", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(values),
      });
      const data = await response.json();
      if (data.success) {
        message.success("添加成功");
        setAddModalVisible(false);
        addForm.resetFields();
        fetchModels();
      } else {
        message.error(data.detail || "添加失败");
      }
    } catch (error) {
      message.error("添加失败");
    }
  };

  const getModelTypeTag = (type: string) => {
    const typeConfig: Record<string, { color: string; text: string }> = {
      chat: { color: "blue", text: "对话" },
      embedding: { color: "green", text: "嵌入" },
      reasoner: { color: "orange", text: "推理" },
      vision: { color: "purple", text: "视觉" },
    };
    const config = typeConfig[type] || { color: "default", text: type };
    return <Tag color={config.color}>{config.text}</Tag>;
  };

  const columns: ColumnsType<Model> = [
    {
      title: "服务商",
      dataIndex: "provider_display_name",
      key: "provider_display_name",
      width: 120,
    },
    {
      title: "模型名称",
      dataIndex: "display_name",
      key: "display_name",
      width: 200,
      render: (text: string, record) => (
        <Tooltip title={record.model_name}>
          <span>{text}</span>
        </Tooltip>
      ),
    },
    {
      title: "类型",
      dataIndex: "model_type",
      key: "model_type",
      width: 80,
      render: (type: string) => getModelTypeTag(type),
    },
    {
      title: "Full Model ID",
      dataIndex: "full_model_id",
      key: "full_model_id",
      width: 250,
      ellipsis: true,
      render: (text: string) => (
        <Tooltip title={text}>
          <code style={{ fontSize: 12 }}>{text}</code>
        </Tooltip>
      ),
    },
    {
      title: "上下文",
      dataIndex: "context_window",
      key: "context_window",
      width: 100,
      render: (value: number | null) =>
        value ? `${(value / 1000).toFixed(0)}K` : "-",
    },
    {
      title: "价格(输入/输出)",
      key: "price",
      width: 120,
      render: (_, record) => {
        if (!record.input_price && !record.output_price) return "-";
        return `$${record.input_price || 0}/${record.output_price || 0}`;
      },
    },
    {
      title: "同步",
      dataIndex: "is_synced",
      key: "is_synced",
      width: 60,
      render: (synced: boolean) =>
        synced ? (
          <CheckCircleOutlined style={{ color: "green" }} />
        ) : (
          <span style={{ color: "#999" }}>手动</span>
        ),
    },
    {
      title: "操作",
      key: "actions",
      width: 80,
      render: (_, record) => (
        <Popconfirm
          title="确定删除此模型？"
          onConfirm={() => handleDeleteModel(record.id)}
        >
          <Button type="link" danger icon={<DeleteOutlined />} />
        </Popconfirm>
      ),
    },
  ];

  return (
    <div style={{ padding: 24 }}>
      <Card
        title="LLM模型管理"
        extra={
          <Space>
            <Button
              type="primary"
              icon={<CloudDownloadOutlined />}
              onClick={handleOpenSyncModal}
            >
              从服务商同步模型
            </Button>
            <Button
              icon={<PlusOutlined />}
              onClick={() => setAddModalVisible(true)}
            >
              手动添加模型
            </Button>
          </Space>
        }
      >
        <Space style={{ marginBottom: 16 }}>
          <Select
            style={{ width: 200 }}
            placeholder="选择服务商筛选"
            allowClear
            value={selectedProviderId}
            onChange={setSelectedProviderId}
          >
            {providers.map((p) => (
              <Option key={p.id} value={p.id}>
                {p.display_name}
              </Option>
            ))}
          </Select>

          <Select
            style={{ width: 120 }}
            value={selectedModelType}
            onChange={setSelectedModelType}
          >
            <Option value="all">全部类型</Option>
            <Option value="chat">对话</Option>
            <Option value="embedding">嵌入</Option>
            <Option value="reasoner">推理</Option>
            <Option value="vision">视觉</Option>
          </Select>
        </Space>

        <Table
          columns={columns}
          dataSource={models}
          rowKey="id"
          loading={loading}
          pagination={{ pageSize: 20, showSizeChanger: true }}
        />
      </Card>

      {/* 同步模型弹窗 */}
      <Modal
        title="从服务商同步模型"
        open={syncModalVisible}
        onCancel={() => setSyncModalVisible(false)}
        footer={null}
        width={800}
      >
        <Alert
          message="选择服务商后，点击获取模型列表，然后勾选要导入的模型"
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
        />

        <Space style={{ marginBottom: 16 }}>
          <Select
            style={{ width: 200 }}
            placeholder="选择服务商"
            value={syncingProviderId}
            onChange={(value) => {
              setSyncingProviderId(value);
              setFetchedModels([]);
              setSelectedModelNames([]);
            }}
          >
            {providers.map((p) => (
              <Option key={p.id} value={p.id}>
                {p.display_name}
              </Option>
            ))}
          </Select>

          <Select
            style={{ width: 120 }}
            value={syncModelType}
            onChange={setSyncModelType}
          >
            <Option value="all">全部类型</Option>
            <Option value="chat">对话</Option>
            <Option value="embedding">嵌入</Option>
            <Option value="reasoner">推理</Option>
            <Option value="vision">视觉</Option>
          </Select>

          <Button
            type="primary"
            icon={<SyncOutlined spin={fetching} />}
            loading={fetching}
            onClick={handleFetchModels}
            disabled={!syncingProviderId}
          >
            获取模型列表
          </Button>
        </Space>

        {fetchedModels.length > 0 && (
          <>
            <Divider />
            <Space style={{ marginBottom: 16 }}>
              <Button
                onClick={() => {
                  setSelectedModelNames(fetchedModels.map((m) => m.model_name));
                }}
              >
                全选
              </Button>
              <Button
                onClick={() => {
                  setSelectedModelNames([]);
                }}
              >
                反选
              </Button>
              <Badge count={selectedModelNames.length} showZero color="blue" />
              <Button
                type="primary"
                icon={<DownloadOutlined />}
                loading={importing}
                onClick={handleImportModels}
                disabled={selectedModelNames.length === 0}
              >
                导入选中模型
              </Button>
            </Space>

            <Table
              size="small"
              dataSource={fetchedModels}
              rowKey="model_name"
              pagination={{ pageSize: 10 }}
              rowSelection={{
                selectedRowKeys: selectedModelNames,
                onChange: (keys) => setSelectedModelNames(keys as string[]),
              }}
              columns={[
                {
                  title: "模型名称",
                  dataIndex: "model_name",
                  key: "model_name",
                },
                {
                  title: "类型",
                  dataIndex: "model_type",
                  key: "model_type",
                  width: 80,
                  render: (type: string) => getModelTypeTag(type),
                },
                {
                  title: "上下文",
                  dataIndex: "context_window",
                  key: "context_window",
                  width: 100,
                  render: (v: number | null) => (v ? `${(v / 1000).toFixed(0)}K` : "-"),
                },
                {
                  title: "价格(输入/输出)",
                  key: "price",
                  width: 120,
                  render: (_, record) => {
                    if (!record.input_price && !record.output_price) return "-";
                    return `$${record.input_price || 0}/${record.output_price || 0}`;
                  },
                },
              ]}
            />
          </>
        )}
      </Modal>

      {/* 手动添加模型弹窗 */}
      <Modal
        title="手动添加模型"
        open={addModalVisible}
        onCancel={() => setAddModalVisible(false)}
        onOk={() => addForm.submit()}
      >
        <Form form={addForm} layout="vertical" onFinish={handleAddModel}>
          <Form.Item
            name="provider_id"
            label="服务商"
            rules={[{ required: true, message: "请选择服务商" }]}
          >
            <Select placeholder="选择服务商">
              {providers.map((p) => (
                <Option key={p.id} value={p.id}>
                  {p.display_name}
                </Option>
              ))}
            </Select>
          </Form.Item>

          <Form.Item
            name="model_name"
            label="模型名称"
            rules={[{ required: true, message: "请输入模型名称" }]}
          >
            <Input placeholder="如: deepseek-chat, gpt-4o" />
          </Form.Item>

          <Form.Item
            name="display_name"
            label="显示名称"
            rules={[{ required: true, message: "请输入显示名称" }]}
          >
            <Input placeholder="如: DeepSeek Chat, GPT-4o" />
          </Form.Item>

          <Form.Item name="model_type" label="模型类型" initialValue="chat">
            <Select>
              <Option value="chat">对话</Option>
              <Option value="embedding">嵌入</Option>
              <Option value="reasoner">推理</Option>
              <Option value="vision">视觉</Option>
            </Select>
          </Form.Item>

          <Form.Item name="context_window" label="上下文窗口">
            <Input type="number" placeholder="如: 128000" />
          </Form.Item>

          <Form.Item name="input_price" label="输入价格($/1M tokens)">
            <Input type="number" step="0.01" placeholder="如: 0.1" />
          </Form.Item>

          <Form.Item name="output_price" label="输出价格($/1M tokens)">
            <Input type="number" step="0.01" placeholder="如: 0.2" />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}
