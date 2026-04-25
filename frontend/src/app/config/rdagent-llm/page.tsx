"use client";

import { useState, useEffect } from "react";
import styles from "./page.module.css";
import LITELLM_PROVIDERS from "./litellm-providers";
import { LiteLLMProviderPreset, LiteLLMModelPreset, getProviderModels } from "./litellm-providers";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "http://localhost:8001";

// 类型定义
interface Provider {
  id: number;
  provider_name: string;
  display_name: string;
  api_base_url: string | null;
  supports_chat: boolean;
  supports_embedding: boolean;
  supports_reasoner: boolean;
  is_active: boolean;
}

interface Model {
  id: number;
  provider_id: number;
  model_name: string;
  display_name: string;
  full_model_id: string;
  model_type: string;
  model_category: string;
  description: string | null;
  is_verified: boolean;
  last_verified_at: string | null;
  provider_name: string;
  provider_display_name: string;
}

interface StageMapping {
  id: number;
  stage_name: string;
  model_id: number | null;
  temperature: number | null;
  max_tokens: number | null;
  model_name: string | null;
  model_display_name: string | null;
  provider_name: string | null;
}

interface ChangeLog {
  id: number;
  stage_name: string;
  old_model_id: number | null;
  new_model_id: number | null;
  change_reason: string;
  changed_at: string;
  changed_by: string;
}

export default function RDAgentLLMConfigPage() {
  const [activeTab, setActiveTab] = useState("providers");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  // 数据状态
  const [providers, setProviders] = useState<Provider[]>([]);
  const [models, setModels] = useState<Model[]>([]);
  const [stageMappings, setStageMappings] = useState<StageMapping[]>([]);
  const [changeLogs, setChangeLogs] = useState<ChangeLog[]>([]);
  const [currentConfig, setCurrentConfig] = useState<any>(null);

  // UI状态
  const [collapsedProviders, setCollapsedProviders] = useState<Set<number>>(new Set());
  const [showAddProvider, setShowAddProvider] = useState(false);
  const [showAddModel, setShowAddModel] = useState(false);
  const [selectedProvider, setSelectedProvider] = useState<number | null>(null);
  const [showMoreLogs, setShowMoreLogs] = useState(false);
  const [selectedPresetProvider, setSelectedPresetProvider] = useState<string>("custom");

  // 表单状态
  const [providerForm, setProviderForm] = useState({
    provider_name: "",
    display_name: "",
    api_base_url: "",
    litellm_prefix: "",
    supports_chat: true,
    supports_embedding: false,
    supports_reasoner: false,
  });

  const [modelForm, setModelForm] = useState({
    provider_id: 0,
    model_name: "",
    display_name: "",
    full_model_id: "",
    model_type: "chat",
    model_category: "对话/Coding",
    description: "",
    api_key: "",
    api_base: "",
    verify_on_add: true,
  });

  const [stageConfigForm, setStageConfigForm] = useState<{
    [key: string]: { model_id: number | null; temperature: number; max_tokens: number };
  }>({});

  const [embeddingModelId, setEmbeddingModelId] = useState<number | null>(null);

  const [changeReason, setChangeReason] = useState("");
  const [testingModelId, setTestingModelId] = useState<number | null>(null);
  const [testResults, setTestResults] = useState<{[key: number]: {success: boolean, message: string}}>({});
  
  // 编辑模型相关状态
  const [editingModel, setEditingModel] = useState<Model | null>(null);
  const [showEditModel, setShowEditModel] = useState(false);
  const [editFormData, setEditFormData] = useState({
    model_name: "",
    display_name: "",
    model_type: "",
    model_category: "",
    description: "",
    api_key: "",
    api_base: "",
  });
  const [editVerifying, setEditVerifying] = useState(false);
  const [editVerificationResult, setEditVerificationResult] = useState<{success: boolean, message: string, details?: any} | null>(null);
  const [editLoading, setEditLoading] = useState(false);

  // 加载数据
  useEffect(() => {
    loadAllData();
  }, []);

  const loadAllData = async () => {
    setLoading(true);
    try {
      await Promise.all([
        loadProviders(),
        loadModels(),
        loadStageMappings(),
        loadCurrentConfig(),
        loadChangeLogs(),
      ]);
    } catch (err) {
      console.error("加载数据失败:", err);
    } finally {
      setLoading(false);
    }
  };

  const loadProviders = async () => {
    const res = await fetch(`${API_BASE}/rdagent/llm-config/providers`);
    const data = await res.json();
    setProviders(data.providers || []);
  };

  const loadModels = async () => {
    const res = await fetch(`${API_BASE}/rdagent/llm-config/models`);
    const data = await res.json();
    setModels(data.models || []);
  };

  const loadStageMappings = async () => {
    const res = await fetch(`${API_BASE}/rdagent/llm-config/stage-mappings`);
    const data = await res.json();
    setStageMappings(data.stage_mappings || []);

    // 初始化表单
    const formData: any = {};
    (data.stage_mappings || []).forEach((mapping: StageMapping) => {
      if (mapping.stage_name === "embedding") {
        setEmbeddingModelId(mapping.model_id);
      } else {
        formData[mapping.stage_name] = {
          model_id: mapping.model_id,
          temperature: mapping.temperature || 0.7,
          max_tokens: mapping.max_tokens || 4000,
        };
      }
    });
    setStageConfigForm(formData);
  };

  const loadCurrentConfig = async () => {
    const res = await fetch(`${API_BASE}/rdagent/llm-config/current-config`);
    const data = await res.json();
    setCurrentConfig(data);
  };

  const loadChangeLogs = async () => {
    const limit = showMoreLogs ? 10 : 5;
    const res = await fetch(`${API_BASE}/rdagent/llm-config/change-logs?limit=${limit}`);
    const data = await res.json();
    setChangeLogs(data.logs || []);
  };

  // 从预设列表选择服务商
  const handlePresetProviderSelect = (providerName: string) => {
    setSelectedPresetProvider(providerName);
    if (providerName === "custom") {
      setProviderForm({
        provider_name: "",
        display_name: "",
        api_base_url: "",
        litellm_prefix: "",
        supports_chat: true,
        supports_embedding: false,
        supports_reasoner: false,
      });
    } else {
      const preset = LITELLM_PROVIDERS.find((p) => p.provider_name === providerName);
      if (preset) {
        setProviderForm({
          provider_name: preset.provider_name,
          display_name: preset.display_name,
          api_base_url: preset.api_base_url,
          litellm_prefix: preset.litellm_prefix,
          supports_chat: preset.supports_chat,
          supports_embedding: preset.supports_embedding,
          supports_reasoner: preset.supports_reasoner,
        });
      }
    }
  };

  // 添加服务商
  const handleAddProvider = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError(null);

    try {
      const res = await fetch(`${API_BASE}/rdagent/llm-config/providers`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(providerForm),
      });

      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || "添加失败");

      setSuccessMessage("服务商添加成功");
      setShowAddProvider(false);
      setSelectedPresetProvider("custom");
      await loadProviders();

      // 重置表单
      setProviderForm({
        provider_name: "",
        display_name: "",
        api_base_url: "",
        litellm_prefix: "",
        supports_chat: true,
        supports_embedding: false,
        supports_reasoner: false,
      });
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  // 添加模型
  const handleAddModel = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (modelForm.description && modelForm.description.length > 100) {
      setError("模型说明不能超过100字");
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const res = await fetch(`${API_BASE}/rdagent/llm-config/models`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(modelForm),
      });

      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || "添加失败");

      setSuccessMessage(
        `模型添加成功${data.is_verified ? "（已验证）" : "（未验证）"}`
      );
      setShowAddModel(false);
      await loadModels();
      
      // 重置表单
      setModelForm({
        provider_id: 0,
        model_name: "",
        display_name: "",
        full_model_id: "",
        model_type: "chat",
        model_category: "对话/Coding",
        description: "",
        api_key: "",
        api_base: "",
        verify_on_add: true,
      });
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  // 测试模型API配置
  const handleTestModel = async (modelId: number) => {
    setTestingModelId(modelId);
    setError(null);

    try {
      const res = await fetch(`${API_BASE}/rdagent/llm-config/models/verify`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          model_id: modelId,
          run_health_check: true,
          run_litellm_test: true
        }),
      });

      const data = await res.json();

      if (res.ok && data.verification?.overall_success) {
        setTestResults({
          ...testResults,
          [modelId]: {
            success: true,
            message: "验证通过：LiteLLM测试和RDAgent健康检查都成功"
          }
        });
      } else {
        const errors = data.verification?.errors || [data.detail || "验证失败"];
        setTestResults({
          ...testResults,
          [modelId]: {
            success: false,
            message: `验证失败: ${errors.join(", ")}`
          }
        });
      }
    } catch (err: any) {
      setTestResults({
        ...testResults,
        [modelId]: {
          success: false,
          message: `测试失败: ${err.message || "网络错误"}`
        }
      });
    } finally {
      setTestingModelId(null);
    }
  };

  // 打开编辑对话框
  const handleOpenEditModel = (model: Model) => {
    console.log('Opening edit dialog for model:', model);
    setEditingModel(model);
    setEditFormData({
      model_name: model.model_name,
      display_name: model.display_name,
      model_type: model.model_type,
      model_category: model.model_category || "",
      description: model.description || "",
      api_key: "",
      api_base: "",
    });
    setEditVerificationResult(null);
    setShowEditModel(true);
    console.log('showEditModel set to true');
    
    // 加载现有API配置
    loadModelConfig(model.id);
  };

  // 加载模型配置
  const loadModelConfig = async (modelId: number) => {
    try {
      const response = await fetch(`${API_BASE}/rdagent/llm-config/models/${modelId}/config`);
      if (response.ok) {
        const data = await response.json();
        if (data.config) {
          setEditFormData(prev => ({
            ...prev,
            api_base: data.config.api_base || "",
          }));
        }
      }
    } catch (err) {
      console.error("加载模型配置失败:", err);
    }
  };

  // 验证编辑的模型配置
  const handleVerifyEditModel = async () => {
    if (!editingModel) return;
    
    setEditVerifying(true);
    setEditVerificationResult(null);

    try {
      const response = await fetch(`${API_BASE}/rdagent/llm-config/models/verify`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          model_id: editingModel.id,
          api_key: editFormData.api_key || undefined,
          api_base: editFormData.api_base || undefined,
          run_health_check: true,
          run_litellm_test: true,
        }),
      });

      const data = await response.json();

      if (data.verification?.overall_success) {
        setEditVerificationResult({
          success: true,
          message: "验证通过：LiteLLM测试和RDAgent健康检查都成功",
          details: data.verification,
        });
      } else {
        const errors = data.verification?.errors || [data.detail || "验证失败"];
        setEditVerificationResult({
          success: false,
          message: `验证失败: ${errors.join(", ")}`,
          details: data.verification,
        });
      }
    } catch (err: any) {
      setEditVerificationResult({
        success: false,
        message: `验证失败: ${err.message || "网络错误"}`,
      });
    } finally {
      setEditVerifying(false);
    }
  };

  // 保存编辑的模型
  const handleSaveEditModel = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!editingModel) return;

    // 如果没有验证过，先验证
    if (!editVerificationResult?.success) {
      await handleVerifyEditModel();
      return;
    }

    setEditLoading(true);

    try {
      // 1. 更新模型基本信息
      const updateResponse = await fetch(
        `${API_BASE}/rdagent/llm-config/models/${editingModel.id}`,
        {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            model_name: editFormData.model_name,
            display_name: editFormData.display_name,
            model_type: editFormData.model_type,
            model_category: editFormData.model_category,
            description: editFormData.description,
          }),
        }
      );

      if (!updateResponse.ok) {
        const error = await updateResponse.json();
        throw new Error(error.detail || "更新模型失败");
      }

      // 2. 更新API配置（如果提供了API Key）
      if (editFormData.api_key) {
        const configResponse = await fetch(
          `${API_BASE}/rdagent/llm-config/models/${editingModel.id}/update-api-config`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              model_id: editingModel.id,
              api_key: editFormData.api_key,
              api_base: editFormData.api_base || null,
              verify_before_save: false,
            }),
          }
        );

        if (!configResponse.ok) {
          const error = await configResponse.json();
          throw new Error(error.detail || "更新API配置失败");
        }
      }

      setSuccessMessage("模型配置更新成功");
      setShowEditModel(false);
      setEditingModel(null);
      loadModels();
    } catch (err: any) {
      setError(err.message || "保存失败");
    } finally {
      setEditLoading(false);
    }
  };

  // 更新配置
  const handleUpdateConfig = async () => {
    if (!changeReason.trim()) {
      setError("请填写变更原因");
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const stage_mappings = Object.entries(stageConfigForm).map(([stage_name, config]) => ({
        stage_name,
        model_id: config.model_id,
        temperature: config.temperature,
        max_tokens: config.max_tokens,
      }));

      const res = await fetch(`${API_BASE}/rdagent/llm-config/update-config`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ 
          stage_mappings, 
          embedding_model_id: embeddingModelId,
          change_reason: changeReason 
        }),
      });

      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || "更新失败");

      setSuccessMessage("配置更新成功");
      setChangeReason("");
      await loadAllData();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  // 切换服务商折叠状态
  const toggleProvider = (providerId: number) => {
    const newCollapsed = new Set(collapsedProviders);
    if (newCollapsed.has(providerId)) {
      newCollapsed.delete(providerId);
    } else {
      newCollapsed.add(providerId);
    }
    setCollapsedProviders(newCollapsed);
  };

  // 获取服务商下的已有模型列表
  const getModelsByProviderId = (providerId: number) => {
    return models.filter((m) => m.provider_id === providerId);
  };

  // 4个RD-Agent阶段（与.env LITELLM_CHAT_MODEL_MAP一致）
  const rdagentStages = [
    { name: "direct_exp_gen", label: "假设生成+实验设计", description: "生成研究假设和实验方案" },
    { name: "coding", label: "代码生成", description: "生成和修改Factor/Strategy代码" },
    { name: "feedback", label: "反馈分析", description: "分析实验结果并生成反馈" },
    { name: "default", label: "默认兜底", description: "其他未定义阶段的默认配置" },
  ];

  return (
    <div className={styles.container}>
      <div className={styles.header}>
        <h1>🤖 RD-Agent 多模型LLM配置管理</h1>
        <p>管理RD-Agent的LLM服务商、模型和阶段映射配置</p>
      </div>

      {/* 错误和成功消息 */}
      {error && (
        <div className={styles.alert} style={{ backgroundColor: "#fee" }}>
          ❌ {error}
        </div>
      )}
      {successMessage && (
        <div className={styles.alert} style={{ backgroundColor: "#efe" }}>
          ✅ {successMessage}
        </div>
      )}

      {/* 标签页 */}
      <div className={styles.tabs}>
        <button
          className={activeTab === "providers" ? styles.tabActive : styles.tab}
          onClick={() => setActiveTab("providers")}
        >
          服务商管理
        </button>
        <button
          className={activeTab === "models" ? styles.tabActive : styles.tab}
          onClick={() => setActiveTab("models")}
        >
          模型管理
        </button>
        <button
          className={activeTab === "stages" ? styles.tabActive : styles.tab}
          onClick={() => setActiveTab("stages")}
        >
          阶段映射配置
        </button>
        <button
          className={activeTab === "logs" ? styles.tabActive : styles.tab}
          onClick={() => setActiveTab("logs")}
        >
          变更记录
        </button>
      </div>

      {/* 服务商管理 */}
      {activeTab === "providers" && (
        <div className={styles.content}>
          <div className={styles.sectionHeader}>
            <h2>服务商管理</h2>
            <button
              className={styles.btnPrimary}
              onClick={() => {
                setSelectedPresetProvider("custom");
                setProviderForm({
                  provider_name: "",
                  display_name: "",
                  api_base_url: "",
                  litellm_prefix: "",
                  supports_chat: true,
                  supports_embedding: false,
                  supports_reasoner: false,
                });
                setShowAddProvider(true);
              }}
            >
              + 添加服务商
            </button>
          </div>

          <div className={styles.providerGrid}>
            {providers.map((provider) => (
              <div key={provider.id} className={styles.providerCard}>
                <div
                  className={styles.providerHeader}
                  onClick={() => toggleProvider(provider.id)}
                >
                  <div>
                    <h3>{provider.display_name}</h3>
                    <p className={styles.providerName}>{provider.provider_name}</p>
                  </div>
                  <span className={styles.collapseIcon}>
                    {collapsedProviders.has(provider.id) ? "▼" : "▲"}
                  </span>
                </div>

                {!collapsedProviders.has(provider.id) && (
                  <div className={styles.providerBody}>
                    <div className={styles.capabilities}>
                      <span className={provider.supports_chat ? styles.capOn : styles.capOff}>
                        💬 对话
                      </span>
                      <span className={provider.supports_reasoner ? styles.capOn : styles.capOff}>
                        🧠 推理
                      </span>
                      <span className={provider.supports_embedding ? styles.capOn : styles.capOff}>
                        📊 嵌入
                      </span>
                    </div>

                    {provider.api_base_url && (
                      <p className={styles.apiBase}>
                        <strong>API Base:</strong> {provider.api_base_url}
                      </p>
                    )}

                    <div className={styles.modelList}>
                      <h4>模型列表 ({getModelsByProviderId(provider.id).length})</h4>
                      {getModelsByProviderId(provider.id).map((model) => (
                        <div key={model.id} className={styles.modelItem}>
                          <div>
                            <strong>{model.display_name}</strong>
                            <span className={styles.modelType}>{model.model_type}</span>
                            {model.is_verified && (
                              <span className={styles.verified}>✓ 已验证</span>
                            )}
                          </div>
                          <p className={styles.modelId}>{model.full_model_id}</p>
                          {model.description && (
                            <p className={styles.modelDesc}>{model.description}</p>
                          )}
                        </div>
                      ))}
                      {getModelsByProviderId(provider.id).length === 0 && (
                        <p className={styles.noData}>暂无模型</p>
                      )}
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>

          {/* 添加服务商对话框 */}
          {showAddProvider && (
            <div className={styles.modal}>
              <div className={styles.modalContent}>
                <h2>添加服务商</h2>
                <form onSubmit={handleAddProvider}>
                  {/* 预设服务商选择 */}
                  <div className={styles.formGroup}>
                    <label>从 LiteLLM 支持列表选择</label>
                    <select
                      value={selectedPresetProvider}
                      onChange={(e) => handlePresetProviderSelect(e.target.value)}
                    >
                      <option value="custom">-- 自定义服务商 --</option>
                      {(() => {
                        const categories = ["国产", "国际", "云平台", "推理平台"];
                        return categories.map((cat) => (
                          <optgroup key={cat} label={cat}>
                            {LITELLM_PROVIDERS.filter((p) => p.category === cat).map((p) => (
                              <option key={p.provider_name} value={p.provider_name}>
                                {p.display_name}
                              </option>
                            ))}
                          </optgroup>
                        ));
                      })()}
                    </select>
                    {selectedPresetProvider !== "custom" && (
                      <p className={styles.hint}>
                        已自动填充 {LITELLM_PROVIDERS.find((p) => p.provider_name === selectedPresetProvider)?.display_name} 的默认配置，可在下方手动调整
                      </p>
                    )}
                  </div>

                  <div className={styles.formGroup}>
                    <label>服务商名称 *</label>
                    <input
                      type="text"
                      required
                      value={providerForm.provider_name}
                      onChange={(e) =>
                        setProviderForm({ ...providerForm, provider_name: e.target.value })
                      }
                      placeholder="例如: deepseek"
                    />
                  </div>

                  <div className={styles.formGroup}>
                    <label>显示名称 *</label>
                    <input
                      type="text"
                      required
                      value={providerForm.display_name}
                      onChange={(e) =>
                        setProviderForm({ ...providerForm, display_name: e.target.value })
                      }
                      placeholder="例如: DeepSeek"
                    />
                  </div>

                  <div className={styles.formGroup}>
                    <label>API Base URL</label>
                    <input
                      type="text"
                      value={providerForm.api_base_url}
                      onChange={(e) =>
                        setProviderForm({ ...providerForm, api_base_url: e.target.value })
                      }
                      placeholder="例如: https://api.deepseek.com"
                    />
                  </div>

                  <div className={styles.formGroup}>
                    <label>LiteLLM前缀</label>
                    <input
                      type="text"
                      value={providerForm.litellm_prefix}
                      onChange={(e) =>
                        setProviderForm({ ...providerForm, litellm_prefix: e.target.value })
                      }
                      placeholder="例如: deepseek"
                    />
                  </div>

                  <div className={styles.checkboxGroup}>
                    <label>
                      <input
                        type="checkbox"
                        checked={providerForm.supports_chat}
                        onChange={(e) =>
                          setProviderForm({ ...providerForm, supports_chat: e.target.checked })
                        }
                      />
                      支持对话
                    </label>
                    <label>
                      <input
                        type="checkbox"
                        checked={providerForm.supports_embedding}
                        onChange={(e) =>
                          setProviderForm({
                            ...providerForm,
                            supports_embedding: e.target.checked,
                          })
                        }
                      />
                      支持嵌入
                    </label>
                    <label>
                      <input
                        type="checkbox"
                        checked={providerForm.supports_reasoner}
                        onChange={(e) =>
                          setProviderForm({ ...providerForm, supports_reasoner: e.target.checked })
                        }
                      />
                      支持推理
                    </label>
                  </div>

                  <div className={styles.modalActions}>
                    <button
                      type="button"
                      className={styles.btnSecondary}
                      onClick={() => {
                        setShowAddProvider(false);
                        setSelectedPresetProvider("custom");
                      }}
                    >
                      取消
                    </button>
                    <button type="submit" className={styles.btnPrimary} disabled={loading}>
                      {loading ? "添加中..." : "添加"}
                    </button>
                  </div>
                </form>
              </div>
            </div>
          )}
        </div>
      )}

      {/* 模型管理 */}
      {activeTab === "models" && (
        <div className={styles.content}>
          <div className={styles.sectionHeader}>
            <h2>模型管理</h2>
            <button
              className={styles.btnPrimary}
              onClick={() => {
                setShowAddModel(true);
                setSelectedProvider(providers[0]?.id || null);
              }}
            >
              + 添加模型
            </button>
          </div>

          <div className={styles.modelGrid}>
            {models.map((model) => (
              <div key={model.id} className={styles.modelCard}>
                <div className={styles.modelCardHeader}>
                  <h3>{model.display_name}</h3>
                  {model.is_verified && <span className={styles.verifiedBadge}>✓ 已验证</span>}
                </div>
                <p className={styles.provider}>
                  服务商: {model.provider_display_name}
                </p>
                <p className={styles.modelId}>{model.full_model_id}</p>
                <div className={styles.modelMeta}>
                  <span className={styles.badge}>{model.model_type}</span>
                  <span className={styles.badge}>{model.model_category}</span>
                </div>
                {model.description && (
                  <p className={styles.description}>{model.description}</p>
                )}
                {model.last_verified_at && (
                  <p className={styles.verifiedTime}>
                    验证时间: {new Date(model.last_verified_at).toLocaleString()}
                  </p>
                )}
                
                {/* 操作按钮 */}
                <div className={styles.modelActions} style={{ display: 'flex', gap: '0.5rem' }}>
                  <button
                    type="button"
                    className={styles.testButton}
                    onClick={() => handleTestModel(model.id)}
                    disabled={testingModelId === model.id}
                    style={{ flex: 1 }}
                  >
                    {testingModelId === model.id ? "测试中..." : "🔍 测试API"}
                  </button>
                  <button
                    type="button"
                    className={styles.editButton}
                    onClick={() => handleOpenEditModel(model)}
                    style={{ 
                      padding: '0.6rem 1rem',
                      background: '#f3f4f6',
                      border: '1px solid #d1d5db',
                      borderRadius: '6px',
                      cursor: 'pointer',
                      fontSize: '0.9rem'
                    }}
                    title="编辑模型配置"
                  >
                    ✏️ 编辑
                  </button>
                </div>

                {/* 测试结果 */}
                {testResults[model.id] && (
                  <div className={testResults[model.id].success ? styles.testSuccess : styles.testError}>
                    {testResults[model.id].success ? "✓" : "✗"} {testResults[model.id].message}
                  </div>
                )}
              </div>
            ))}
          </div>

          {/* 添加模型对话框 */}
          {showAddModel && (
            <div className={styles.modal}>
              <div className={styles.modalContent} style={{ maxWidth: "600px" }}>
                <h2>添加模型</h2>
                <form onSubmit={handleAddModel}>
                  <div className={styles.formGroup}>
                    <label>服务商 *</label>
                    <select
                      required
                      value={modelForm.provider_id}
                      onChange={(e) => {
                        const pid = parseInt(e.target.value);
                        setModelForm({
                          ...modelForm,
                          provider_id: pid,
                          // 清空模型字段，等待用户选择推荐或手动输入
                          model_name: "",
                          display_name: "",
                          full_model_id: "",
                        });
                        setSelectedProvider(pid);
                      }}
                    >
                      <option value={0}>请选择服务商</option>
                      {providers.map((p) => (
                        <option key={p.id} value={p.id}>
                          {p.display_name}
                        </option>
                      ))}
                    </select>
                  </div>

                  {/* 推荐模型选择 */}
                  {modelForm.provider_id > 0 && (() => {
                    const selectedProv = providers.find((p) => p.id === modelForm.provider_id);
                    const suggestions = selectedProv ? getProviderModels(selectedProv.provider_name) : [];
                    if (suggestions.length === 0) return null;
                    const litellmPrefix = selectedProv?.provider_name
                      ? LITELLM_PROVIDERS.find((p) => p.provider_name === selectedProv.provider_name)?.litellm_prefix || ""
                      : "";
                    return (
                      <div className={styles.formGroup}>
                        <label>推荐模型 (LiteLLM)</label>
                        <select
                          value=""
                          onChange={(e) => {
                            const m = suggestions.find((s) => s.model_name === e.target.value);
                            if (m) {
                              setModelForm({
                                ...modelForm,
                                model_name: m.model_name,
                                display_name: m.display_name,
                                full_model_id: litellmPrefix ? `${litellmPrefix}/${m.model_name}` : m.model_name,
                                model_type: m.model_type,
                                model_category: m.model_category,
                              });
                            }
                          }}
                        >
                          <option value="">-- 选择推荐模型 或 手动填写下方字段 --</option>
                          {suggestions.map((s) => (
                            <option key={s.model_name} value={s.model_name}>
                              {s.display_name} [{s.model_type}]
                            </option>
                          ))}
                        </select>
                      </div>
                    );
                  })()}

                  <div className={styles.formGroup}>
                    <label>模型名称 *</label>
                    <input
                      type="text"
                      required
                      value={modelForm.model_name}
                      onChange={(e) => {
                        const newName = e.target.value;
                        const p = providers.find((p) => p.id === modelForm.provider_id);
                        const preset = p ? LITELLM_PROVIDERS.find((pr) => pr.provider_name === p.provider_name) : undefined;
                        const prefix = preset?.litellm_prefix || "";
                        setModelForm({
                          ...modelForm,
                          model_name: newName,
                          full_model_id: prefix ? `${prefix}/${newName}` : newName,
                        });
                      }}
                      placeholder={(() => {
                        const p = providers.find((p) => p.id === modelForm.provider_id);
                        const preset = p ? LITELLM_PROVIDERS.find((pr) => pr.provider_name === p.provider_name) : undefined;
                        return preset && preset.default_models.length > 0
                          ? `例如: ${preset.default_models[0].model_name}`
                          : "例如: deepseek-chat";
                      })()}
                    />
                  </div>

                  <div className={styles.formGroup}>
                    <label>显示名称 *</label>
                    <input
                      type="text"
                      required
                      value={modelForm.display_name}
                      onChange={(e) =>
                        setModelForm({ ...modelForm, display_name: e.target.value })
                      }
                      placeholder={(() => {
                        const p = providers.find((p) => p.id === modelForm.provider_id);
                        const preset = p ? LITELLM_PROVIDERS.find((pr) => pr.provider_name === p.provider_name) : undefined;
                        return preset && preset.default_models.length > 0
                          ? `例如: ${preset.default_models[0].display_name}`
                          : "例如: DeepSeek Chat";
                      })()}
                    />
                  </div>

                  <div className={styles.formGroup}>
                    <label>完整模型ID *</label>
                    <input
                      type="text"
                      required
                      value={modelForm.full_model_id}
                      onChange={(e) =>
                        setModelForm({ ...modelForm, full_model_id: e.target.value })
                      }
                      placeholder={(() => {
                        const p = providers.find((p) => p.id === modelForm.provider_id);
                        if (!p) return "例如: deepseek/deepseek-chat";
                        const preset = LITELLM_PROVIDERS.find((pr) => pr.provider_name === p.provider_name);
                        if (preset && preset.default_models.length > 0) {
                          return `例如: ${preset.litellm_prefix}/${preset.default_models[0].model_name}`;
                        }
                        return "例如: deepseek/deepseek-chat";
                      })()}
                    />
                  </div>

                  <div className={styles.formRow}>
                    <div className={styles.formGroup}>
                      <label>模型类型 *</label>
                      <select
                        value={modelForm.model_type}
                        onChange={(e) =>
                          setModelForm({ ...modelForm, model_type: e.target.value })
                        }
                      >
                        <option value="chat">chat</option>
                        <option value="reasoner">reasoner</option>
                        <option value="embedding">embedding</option>
                      </select>
                    </div>

                    <div className={styles.formGroup}>
                      <label>模型分类 *</label>
                      <select
                        value={modelForm.model_category}
                        onChange={(e) =>
                          setModelForm({ ...modelForm, model_category: e.target.value })
                        }
                      >
                        <option value="对话/Coding">对话/Coding</option>
                        <option value="推理模型">推理模型</option>
                        <option value="嵌入式模型">嵌入式模型</option>
                      </select>
                    </div>
                  </div>

                  <div className={styles.formGroup}>
                    <label>
                      模型说明 <span className={styles.hint}>(≤100字)</span>
                    </label>
                    <textarea
                      value={modelForm.description}
                      onChange={(e) =>
                        setModelForm({ ...modelForm, description: e.target.value })
                      }
                      placeholder="简要描述模型特点..."
                      maxLength={100}
                      rows={3}
                    />
                    <div className={styles.charCount}>
                      {modelForm.description.length}/100
                    </div>
                  </div>

                  <div className={styles.formGroup}>
                    <label>
                      <input
                        type="checkbox"
                        checked={modelForm.verify_on_add}
                        onChange={(e) =>
                          setModelForm({ ...modelForm, verify_on_add: e.target.checked })
                        }
                      />
                      添加时验证API可用性
                    </label>
                  </div>

                  {modelForm.verify_on_add && (
                    <>
                      <div className={styles.formGroup}>
                        <label>API Key *</label>
                        <input
                          type="password"
                          required={modelForm.verify_on_add}
                          value={modelForm.api_key}
                          onChange={(e) =>
                            setModelForm({ ...modelForm, api_key: e.target.value })
                          }
                          placeholder="用于验证的API Key"
                        />
                      </div>

                      <div className={styles.formGroup}>
                        <label>API Base (可选)</label>
                        <input
                          type="text"
                          value={modelForm.api_base}
                          onChange={(e) =>
                            setModelForm({ ...modelForm, api_base: e.target.value })
                          }
                          placeholder="自定义API Base URL"
                        />
                      </div>
                    </>
                  )}

                  <div className={styles.modalActions}>
                    <button
                      type="button"
                      className={styles.btnSecondary}
                      onClick={() => setShowAddModel(false)}
                    >
                      取消
                    </button>
                    <button type="submit" className={styles.btnPrimary} disabled={loading}>
                      {loading ? "添加中..." : "添加"}
                    </button>
                  </div>
                </form>
              </div>
            </div>
          )}
        </div>
      )}

      {/* 阶段映射配置 */}
      {activeTab === "stages" && (
        <div className={styles.content}>
          <h2>阶段映射配置</h2>
          <p className={styles.hint}>配置6个RD-Agent阶段使用的模型和参数</p>

          <div className={styles.stageGrid}>
            {rdagentStages.map((stage) => {
              const config = stageConfigForm[stage.name] || {
                model_id: null,
                temperature: 0.7,
                max_tokens: 4000,
              };

              return (
                <div key={stage.name} className={styles.stageCard}>
                  <h3>{stage.label}</h3>
                  <p className={styles.stageDesc}>{stage.description}</p>

                  <div className={styles.formGroup}>
                    <label>选择模型</label>
                    <select
                      value={config.model_id || ""}
                      onChange={(e) =>
                        setStageConfigForm({
                          ...stageConfigForm,
                          [stage.name]: {
                            ...config,
                            model_id: e.target.value ? parseInt(e.target.value) : null,
                          },
                        })
                      }
                    >
                      <option value="">未配置</option>
                      {models
                        .filter((m) => m.model_type === "chat" || m.model_type === "reasoner")
                        .map((m) => (
                          <option key={m.id} value={m.id}>
                            {m.display_name} ({m.provider_display_name})
                          </option>
                        ))}
                    </select>
                  </div>

                  <div className={styles.formRow}>
                    <div className={styles.formGroup}>
                      <label>Temperature</label>
                      <input
                        type="number"
                        step="0.1"
                        min="0"
                        max="2"
                        value={config.temperature}
                        onChange={(e) =>
                          setStageConfigForm({
                            ...stageConfigForm,
                            [stage.name]: {
                              ...config,
                              temperature: parseFloat(e.target.value),
                            },
                          })
                        }
                      />
                    </div>

                    <div className={styles.formGroup}>
                      <label>Max Tokens</label>
                      <input
                        type="number"
                        step="100"
                        min="100"
                        max="32000"
                        value={config.max_tokens}
                        onChange={(e) =>
                          setStageConfigForm({
                            ...stageConfigForm,
                            [stage.name]: {
                              ...config,
                              max_tokens: parseInt(e.target.value),
                            },
                          })
                        }
                      />
                    </div>
                  </div>
                </div>
              );
            })}
          </div>

          <h2 style={{ marginTop: "40px" }}>Embedding模型配置</h2>
          <p className={styles.hint}>配置全局统一的embedding模型（用于RAG、记忆、检索等）</p>

          <div className={styles.globalEmbeddingConfig}>
            <div className={styles.formGroup}>
              <label>选择Embedding模型</label>
              <select
                value={embeddingModelId || ""}
                onChange={(e) => setEmbeddingModelId(e.target.value ? parseInt(e.target.value) : null)}
              >
                <option value="">未配置</option>
                {models
                  .filter((m) => m.model_type === "embedding")
                  .map((m) => (
                    <option key={m.id} value={m.id}>
                      {m.display_name} ({m.provider_display_name})
                    </option>
                  ))}
              </select>
              <p className={styles.hint} style={{ marginTop: "8px" }}>
                注意：RDAgent只支持全局统一的embedding模型，所有阶段使用相同的embedding模型
              </p>
            </div>
          </div>

          <div className={styles.updateSection}>
            <div className={styles.formGroup}>
              <label>变更原因 *</label>
              <textarea
                required
                value={changeReason}
                onChange={(e) => setChangeReason(e.target.value)}
                placeholder="请说明本次配置变更的原因..."
                rows={3}
              />
            </div>

            <button
              className={styles.btnPrimary}
              onClick={handleUpdateConfig}
              disabled={loading || !changeReason.trim()}
            >
              {loading ? "更新中..." : "保存配置"}
            </button>
          </div>
        </div>
      )}

      {/* 变更记录 */}
      {activeTab === "logs" && (
        <div className={styles.content}>
          <h2>变更记录</h2>

          <div className={styles.logList}>
            {changeLogs.map((log: any) => (
              <div key={log.id} className={styles.logItem}>
                <div className={styles.logHeader}>
                  <strong>{log.stage_name}</strong>
                  <span className={styles.logTime}>
                    {new Date(log.changed_at).toLocaleString()}
                  </span>
                </div>
                <p className={styles.logChange}>
                  模型变更: {log.old_model_name} → {log.new_model_name}
                </p>
                <p className={styles.logReason}>
                  原因: {log.change_reason} | 操作人: {log.changed_by}
                </p>
              </div>
            ))}

            {changeLogs.length === 0 && <p className={styles.noData}>暂无变更记录</p>}
          </div>

          {changeLogs.length >= 5 && (
            <button
              className={styles.btnSecondary}
              onClick={() => {
                setShowMoreLogs(!showMoreLogs);
                loadChangeLogs();
              }}
            >
              {showMoreLogs ? "显示更少" : "查看更多"}
            </button>
          )}
        </div>
      )}
      {/* 编辑模型对话框 - 内联实现 */}
      {showEditModel && editingModel && (
        <div 
          style={{
            position: 'fixed',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            backgroundColor: 'rgba(0, 0, 0, 0.5)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            zIndex: 9999,
          }}
          onClick={(e) => {
            if (e.target === e.currentTarget) {
              setShowEditModel(false);
            }
          }}
        >
          <div 
            style={{
              background: 'white',
              borderRadius: '12px',
              padding: '2rem',
              width: '90%',
              maxWidth: '600px',
              maxHeight: '90vh',
              overflow: 'auto',
              boxShadow: '0 25px 50px -12px rgba(0, 0, 0, 0.25)',
            }}
          >
            <h2 style={{ marginBottom: '0.5rem' }}>编辑模型 - {editingModel.display_name}</h2>
            <p style={{ color: '#666', marginBottom: '1.5rem', fontSize: '0.9rem' }}>
              修改模型配置，确认后将执行完整验证（LiteLLM + RDAgent健康检查）
            </p>

            <form onSubmit={handleSaveEditModel}>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem', marginBottom: '1rem' }}>
                <div>
                  <label style={{ display: 'block', marginBottom: '0.5rem', fontWeight: 500 }}>模型名称 *</label>
                  <input
                    type="text"
                    value={editFormData.model_name}
                    onChange={(e) => setEditFormData({...editFormData, model_name: e.target.value})}
                    required
                    style={{ width: '100%', padding: '0.5rem', border: '1px solid #d1d5db', borderRadius: '6px' }}
                  />
                </div>
                <div>
                  <label style={{ display: 'block', marginBottom: '0.5rem', fontWeight: 500 }}>显示名称 *</label>
                  <input
                    type="text"
                    value={editFormData.display_name}
                    onChange={(e) => setEditFormData({...editFormData, display_name: e.target.value})}
                    required
                    style={{ width: '100%', padding: '0.5rem', border: '1px solid #d1d5db', borderRadius: '6px' }}
                  />
                </div>
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem', marginBottom: '1rem' }}>
                <div>
                  <label style={{ display: 'block', marginBottom: '0.5rem', fontWeight: 500 }}>模型类型 *</label>
                  <select
                    value={editFormData.model_type}
                    onChange={(e) => setEditFormData({...editFormData, model_type: e.target.value})}
                    required
                    style={{ width: '100%', padding: '0.5rem', border: '1px solid #d1d5db', borderRadius: '6px' }}
                  >
                    <option value="chat">对话/Coding</option>
                    <option value="reasoner">推理模型</option>
                    <option value="embedding">嵌入式模型</option>
                  </select>
                </div>
                <div>
                  <label style={{ display: 'block', marginBottom: '0.5rem', fontWeight: 500 }}>模型分类</label>
                  <select
                    value={editFormData.model_category}
                    onChange={(e) => setEditFormData({...editFormData, model_category: e.target.value})}
                    style={{ width: '100%', padding: '0.5rem', border: '1px solid #d1d5db', borderRadius: '6px' }}
                  >
                    <option value="">请选择</option>
                    <option value="对话/Coding">对话/Coding</option>
                    <option value="推理模型">推理模型</option>
                    <option value="嵌入式模型">嵌入式模型</option>
                  </select>
                </div>
              </div>

              <div style={{ marginBottom: '1rem' }}>
                <label style={{ display: 'block', marginBottom: '0.5rem', fontWeight: 500 }}>模型说明（≤100字）</label>
                <textarea
                  value={editFormData.description}
                  onChange={(e) => setEditFormData({...editFormData, description: e.target.value})}
                  maxLength={100}
                  rows={3}
                  style={{ width: '100%', padding: '0.5rem', border: '1px solid #d1d5db', borderRadius: '6px' }}
                />
                <div style={{ textAlign: 'right', fontSize: '0.8rem', color: '#999' }}>
                  {editFormData.description.length}/100
                </div>
              </div>

              <div style={{ borderTop: '1px solid #e5e7eb', paddingTop: '1rem', marginBottom: '1rem' }}>
                <h4 style={{ marginBottom: '1rem' }}>API配置</h4>
                
                <div style={{ marginBottom: '1rem' }}>
                  <label style={{ display: 'block', marginBottom: '0.5rem', fontWeight: 500 }}>
                    API Key {editingModel.is_verified ? '（留空保持原配置）' : '*'}
                  </label>
                  <input
                    type="password"
                    value={editFormData.api_key}
                    onChange={(e) => setEditFormData({...editFormData, api_key: e.target.value})}
                    placeholder={editingModel.is_verified ? '输入新API Key或留空' : '输入API Key进行验证'}
                    required={!editingModel.is_verified}
                    style={{ width: '100%', padding: '0.5rem', border: '1px solid #d1d5db', borderRadius: '6px' }}
                  />
                  <div style={{ fontSize: '0.8rem', color: '#666', marginTop: '0.25rem' }}>
                    {editingModel.is_verified ? '模型已通过验证，如需更换API Key请输入新值' : '该模型尚未验证，必须提供API Key进行验证'}
                  </div>
                </div>

                <div style={{ marginBottom: '1rem' }}>
                  <label style={{ display: 'block', marginBottom: '0.5rem', fontWeight: 500 }}>API Base URL（可选）</label>
                  <input
                    type="text"
                    value={editFormData.api_base}
                    onChange={(e) => setEditFormData({...editFormData, api_base: e.target.value})}
                    placeholder="例如: https://api.anthropic.com"
                    style={{ width: '100%', padding: '0.5rem', border: '1px solid #d1d5db', borderRadius: '6px' }}
                  />
                </div>

                {/* 验证按钮 */}
                <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', marginBottom: '1rem' }}>
                  <button
                    type="button"
                    onClick={handleVerifyEditModel}
                    disabled={editVerifying || (!editFormData.api_key && !editingModel.is_verified)}
                    style={{
                      padding: '0.5rem 1rem',
                      background: editVerifying ? '#9ca3af' : '#667eea',
                      color: 'white',
                      border: 'none',
                      borderRadius: '6px',
                      cursor: editVerifying || (!editFormData.api_key && !editingModel.is_verified) ? 'not-allowed' : 'pointer',
                    }}
                  >
                    {editVerifying ? '验证中...' : '🔍 验证配置'}
                  </button>
                  
                  {editVerificationResult && (
                    <div style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: '0.5rem',
                      color: editVerificationResult.success ? '#059669' : '#dc2626',
                    }}>
                      {editVerificationResult.success ? '✓' : '✗'}
                      <span>{editVerificationResult.success ? '验证通过' : '验证失败'}</span>
                    </div>
                  )}
                </div>

                {/* 详细验证结果 */}
                {editVerificationResult?.details && (
                  <div style={{ background: '#f9fafb', padding: '0.75rem', borderRadius: '6px', fontSize: '0.85rem', marginBottom: '1rem' }}>
                    {editVerificationResult.details.litellm_test && (
                      <div style={{ color: editVerificationResult.details.litellm_test.success ? '#059669' : '#dc2626' }}>
                        {editVerificationResult.details.litellm_test.success ? '✓' : '✗'} LiteLLM: {editVerificationResult.details.litellm_test.message}
                      </div>
                    )}
                    {editVerificationResult.details.rdagent_health_check && (
                      <div style={{ color: editVerificationResult.details.rdagent_health_check.success ? '#059669' : '#dc2626' }}>
                        {editVerificationResult.details.rdagent_health_check.success ? '✓' : '✗'} RDAgent: {editVerificationResult.details.rdagent_health_check.message}
                      </div>
                    )}
                  </div>
                )}
              </div>

              <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '0.75rem' }}>
                <button
                  type="button"
                  onClick={() => setShowEditModel(false)}
                  disabled={editLoading}
                  style={{
                    padding: '0.5rem 1rem',
                    background: 'white',
                    border: '1px solid #d1d5db',
                    borderRadius: '6px',
                    cursor: 'pointer',
                  }}
                >
                  取消
                </button>
                <button
                  type="submit"
                  disabled={editLoading || editVerifying || !editVerificationResult?.success}
                  style={{
                    padding: '0.5rem 1rem',
                    background: !editVerificationResult?.success ? '#9ca3af' : '#667eea',
                    color: 'white',
                    border: 'none',
                    borderRadius: '6px',
                    cursor: !editVerificationResult?.success ? 'not-allowed' : 'pointer',
                  }}
                >
                  {editLoading ? '保存中...' : editVerificationResult?.success ? '保存配置' : '请先验证'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}
