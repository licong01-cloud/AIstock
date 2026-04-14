/**
 * LiteLLM 支持的服务商预设列表 + 推荐模型
 * 用于添加服务商/模型时自动填充配置
 */

export interface LiteLLMModelPreset {
  model_name: string;       // 模型标识，如 "deepseek-chat"
  display_name: string;     // 显示名称
  model_type: "chat" | "reasoner" | "embedding";
  model_category: string;   // 模型分类
}

export interface LiteLLMProviderPreset {
  provider_name: string;
  display_name: string;
  api_base_url: string;
  litellm_prefix: string;
  supports_chat: boolean;
  supports_embedding: boolean;
  supports_reasoner: boolean;
  category: string;
  default_models: LiteLLMModelPreset[];
}

const LITELLM_PROVIDERS: LiteLLMProviderPreset[] = [
  // ===== 国产服务商 =====
  {
    provider_name: "deepseek",
    display_name: "DeepSeek",
    api_base_url: "https://api.deepseek.com",
    litellm_prefix: "deepseek",
    supports_chat: true,
    supports_embedding: false,
    supports_reasoner: true,
    category: "国产",
    default_models: [
      { model_name: "deepseek-chat", display_name: "DeepSeek V3", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "deepseek-reasoner", display_name: "DeepSeek R1", model_type: "reasoner", model_category: "推理模型" },
    ],
  },
  {
    provider_name: "siliconflow",
    display_name: "硅基流动 (SiliconFlow)",
    api_base_url: "https://api.siliconflow.cn/v1",
    litellm_prefix: "openai",
    supports_chat: true,
    supports_embedding: true,
    supports_reasoner: false,
    category: "国产",
    default_models: [
      { model_name: "deepseek-ai/DeepSeek-V3", display_name: "DeepSeek V3 (硅基流动)", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "deepseek-ai/DeepSeek-R1", display_name: "DeepSeek R1 (硅基流动)", model_type: "chat", model_category: "推理模型" },
      { model_name: "Qwen/Qwen3-235B-A22B", display_name: "Qwen3 235B (硅基流动)", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "BAAI/bge-m3", display_name: "BGE-M3 嵌入", model_type: "embedding", model_category: "嵌入式模型" },
    ],
  },
  {
    provider_name: "dashscope",
    display_name: "阿里云百炼 (DashScope)",
    api_base_url: "https://dashscope.aliyuncs.com/compatible-mode/v1",
    litellm_prefix: "openai",
    supports_chat: true,
    supports_embedding: true,
    supports_reasoner: true,
    category: "国产",
    default_models: [
      { model_name: "qwen-plus", display_name: "Qwen Plus", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "qwen-turbo", display_name: "Qwen Turbo", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "qwen-max", display_name: "Qwen Max", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "qwen3-235b-a22b", display_name: "Qwen3 235B", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "qwq-plus", display_name: "QwQ Plus (推理)", model_type: "reasoner", model_category: "推理模型" },
      { model_name: "text-embedding-v3", display_name: "Text Embedding V3", model_type: "embedding", model_category: "嵌入式模型" },
    ],
  },
  {
    provider_name: "moonshot",
    display_name: "月之暗面 (Moonshot AI)",
    api_base_url: "https://api.moonshot.cn/v1",
    litellm_prefix: "openai",
    supports_chat: true,
    supports_embedding: false,
    supports_reasoner: false,
    category: "国产",
    default_models: [
      { model_name: "moonshot-v1-8k", display_name: "Moonshot V1 8K", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "moonshot-v1-32k", display_name: "Moonshot V1 32K", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "moonshot-v1-128k", display_name: "Moonshot V1 128K", model_type: "chat", model_category: "对话/Coding" },
    ],
  },
  {
    provider_name: "zhipu",
    display_name: "智谱AI (Z.AI)",
    api_base_url: "https://open.bigmodel.cn/api/paas/v4",
    litellm_prefix: "openai",
    supports_chat: true,
    supports_embedding: true,
    supports_reasoner: false,
    category: "国产",
    default_models: [
      { model_name: "glm-4-plus", display_name: "GLM-4 Plus", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "glm-4-flash", display_name: "GLM-4 Flash", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "glm-4-flashx", display_name: "GLM-4 FlashX", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "glm-4-long", display_name: "GLM-4 Long", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "embedding-3", display_name: "Embedding-3", model_type: "embedding", model_category: "嵌入式模型" },
    ],
  },
  {
    provider_name: "volcengine",
    display_name: "火山引擎 (Volcengine)",
    api_base_url: "https://ark.cn-beijing.volces.com/api/v3",
    litellm_prefix: "openai",
    supports_chat: true,
    supports_embedding: true,
    supports_reasoner: false,
    category: "国产",
    default_models: [
      { model_name: "doubao-1.5-pro-32k", display_name: "Doubao 1.5 Pro 32K", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "doubao-1.5-lite-32k", display_name: "Doubao 1.5 Lite 32K", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "doubao-embedding", display_name: "Doubao Embedding", model_type: "embedding", model_category: "嵌入式模型" },
    ],
  },
  {
    provider_name: "minimax",
    display_name: "MiniMax",
    api_base_url: "https://api.minimax.chat/v1",
    litellm_prefix: "minimax",
    supports_chat: true,
    supports_embedding: false,
    supports_reasoner: false,
    category: "国产",
    default_models: [
      { model_name: "MiniMax-Text-01", display_name: "MiniMax Text 01", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "abab6.5s-chat", display_name: "ABAB 6.5s Chat", model_type: "chat", model_category: "对话/Coding" },
    ],
  },
  // ===== 国际服务商 =====
  {
    provider_name: "openai",
    display_name: "OpenAI",
    api_base_url: "https://api.openai.com/v1",
    litellm_prefix: "openai",
    supports_chat: true,
    supports_embedding: true,
    supports_reasoner: true,
    category: "国际",
    default_models: [
      { model_name: "gpt-4o", display_name: "GPT-4o", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "gpt-4o-mini", display_name: "GPT-4o Mini", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "o3", display_name: "O3", model_type: "reasoner", model_category: "推理模型" },
      { model_name: "o4-mini", display_name: "O4 Mini", model_type: "reasoner", model_category: "推理模型" },
      { model_name: "text-embedding-3-large", display_name: "Text Embedding 3 Large", model_type: "embedding", model_category: "嵌入式模型" },
    ],
  },
  {
    provider_name: "anthropic",
    display_name: "Anthropic (Claude)",
    api_base_url: "https://api.anthropic.com",
    litellm_prefix: "anthropic",
    supports_chat: true,
    supports_embedding: false,
    supports_reasoner: true,
    category: "国际",
    default_models: [
      { model_name: "claude-sonnet-4-20250514", display_name: "Claude Sonnet 4", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "claude-haiku-4-20250506", display_name: "Claude Haiku 4", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "claude-opus-4-20250514", display_name: "Claude Opus 4", model_type: "reasoner", model_category: "推理模型" },
    ],
  },
  {
    provider_name: "gemini",
    display_name: "Google AI Studio (Gemini)",
    api_base_url: "https://generativelanguage.googleapis.com/v1beta",
    litellm_prefix: "gemini",
    supports_chat: true,
    supports_embedding: true,
    supports_reasoner: true,
    category: "国际",
    default_models: [
      { model_name: "gemini-2.5-pro", display_name: "Gemini 2.5 Pro", model_type: "reasoner", model_category: "推理模型" },
      { model_name: "gemini-2.5-flash", display_name: "Gemini 2.5 Flash", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "gemini-2.0-flash", display_name: "Gemini 2.0 Flash", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "text-embedding-004", display_name: "Text Embedding 004", model_type: "embedding", model_category: "嵌入式模型" },
    ],
  },
  {
    provider_name: "mistral",
    display_name: "Mistral AI",
    api_base_url: "https://api.mistral.ai/v1",
    litellm_prefix: "mistral",
    supports_chat: true,
    supports_embedding: true,
    supports_reasoner: false,
    category: "国际",
    default_models: [
      { model_name: "mistral-large-latest", display_name: "Mistral Large", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "mistral-small-latest", display_name: "Mistral Small", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "codestral-latest", display_name: "Codestral", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "mistral-embed", display_name: "Mistral Embed", model_type: "embedding", model_category: "嵌入式模型" },
    ],
  },
  {
    provider_name: "cohere",
    display_name: "Cohere",
    api_base_url: "https://api.cohere.ai/v1",
    litellm_prefix: "cohere",
    supports_chat: true,
    supports_embedding: true,
    supports_reasoner: false,
    category: "国际",
    default_models: [
      { model_name: "command-r-plus", display_name: "Command R+", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "command-r", display_name: "Command R", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "embed-english-v3.0", display_name: "Embed English V3", model_type: "embedding", model_category: "嵌入式模型" },
    ],
  },
  {
    provider_name: "xai",
    display_name: "xAI (Grok)",
    api_base_url: "https://api.x.ai/v1",
    litellm_prefix: "xai",
    supports_chat: true,
    supports_embedding: false,
    supports_reasoner: false,
    category: "国际",
    default_models: [
      { model_name: "grok-3", display_name: "Grok 3", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "grok-3-mini", display_name: "Grok 3 Mini", model_type: "chat", model_category: "对话/Coding" },
    ],
  },
  // ===== 云平台 =====
  {
    provider_name: "azure",
    display_name: "Azure OpenAI",
    api_base_url: "",
    litellm_prefix: "azure",
    supports_chat: true,
    supports_embedding: true,
    supports_reasoner: false,
    category: "云平台",
    default_models: [
      { model_name: "gpt-4o", display_name: "GPT-4o (Azure)", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "gpt-4o-mini", display_name: "GPT-4o Mini (Azure)", model_type: "chat", model_category: "对话/Coding" },
    ],
  },
  {
    provider_name: "bedrock",
    display_name: "AWS Bedrock",
    api_base_url: "",
    litellm_prefix: "bedrock",
    supports_chat: true,
    supports_embedding: true,
    supports_reasoner: true,
    category: "云平台",
    default_models: [
      { model_name: "anthropic.claude-sonnet-4-20250514-v1:0", display_name: "Claude Sonnet 4 (Bedrock)", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "anthropic.claude-haiku-4-20250506-v1:0", display_name: "Claude Haiku 4 (Bedrock)", model_type: "chat", model_category: "对话/Coding" },
    ],
  },
  {
    provider_name: "vertex_ai",
    display_name: "Google Vertex AI",
    api_base_url: "",
    litellm_prefix: "vertex_ai",
    supports_chat: true,
    supports_embedding: true,
    supports_reasoner: true,
    category: "云平台",
    default_models: [
      { model_name: "gemini-2.5-pro", display_name: "Gemini 2.5 Pro (Vertex)", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "gemini-2.5-flash", display_name: "Gemini 2.5 Flash (Vertex)", model_type: "chat", model_category: "对话/Coding" },
    ],
  },
  // ===== 推理/开源平台 =====
  {
    provider_name: "ollama",
    display_name: "Ollama (本地)",
    api_base_url: "http://localhost:11434",
    litellm_prefix: "ollama",
    supports_chat: true,
    supports_embedding: true,
    supports_reasoner: false,
    category: "推理平台",
    default_models: [
      { model_name: "qwen3:14b", display_name: "Qwen3 14B (本地)", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "deepseek-r1:14b", display_name: "DeepSeek R1 14B (本地)", model_type: "chat", model_category: "推理模型" },
      { model_name: "llama3.1:8b", display_name: "Llama 3.1 8B (本地)", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "nomic-embed-text", display_name: "Nomic Embed (本地)", model_type: "embedding", model_category: "嵌入式模型" },
    ],
  },
  {
    provider_name: "vllm",
    display_name: "vLLM (本地)",
    api_base_url: "http://localhost:8000/v1",
    litellm_prefix: "openai",
    supports_chat: true,
    supports_embedding: true,
    supports_reasoner: false,
    category: "推理平台",
    default_models: [],
  },
  {
    provider_name: "together_ai",
    display_name: "Together AI",
    api_base_url: "https://api.together.xyz/v1",
    litellm_prefix: "together_ai",
    supports_chat: true,
    supports_embedding: true,
    supports_reasoner: false,
    category: "推理平台",
    default_models: [
      { model_name: "deepseek-ai/DeepSeek-V3", display_name: "DeepSeek V3 (Together)", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "meta-llama/Llama-3.3-70B-Instruct-Turbo", display_name: "Llama 3.3 70B", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "togethercomputer/m2-bert-80M-8k-retrieval", display_name: "M2 Bert Embedding", model_type: "embedding", model_category: "嵌入式模型" },
    ],
  },
  {
    provider_name: "fireworks_ai",
    display_name: "Fireworks AI",
    api_base_url: "https://api.fireworks.ai/inference/v1",
    litellm_prefix: "fireworks_ai",
    supports_chat: true,
    supports_embedding: true,
    supports_reasoner: false,
    category: "推理平台",
    default_models: [
      { model_name: "accounts/fireworks/models/llama-v3p3-70b-instruct", display_name: "Llama 3.3 70B (Fireworks)", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "nomic-ai/nomic-embed-text-v1.5", display_name: "Nomic Embed V1.5", model_type: "embedding", model_category: "嵌入式模型" },
    ],
  },
  {
    provider_name: "openrouter",
    display_name: "OpenRouter",
    api_base_url: "https://openrouter.ai/api/v1",
    litellm_prefix: "openrouter",
    supports_chat: true,
    supports_embedding: false,
    supports_reasoner: false,
    category: "推理平台",
    default_models: [
      { model_name: "deepseek/deepseek-chat", display_name: "DeepSeek V3 (OpenRouter)", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "anthropic/claude-sonnet-4-20250514", display_name: "Claude Sonnet 4 (OpenRouter)", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "openai/gpt-4o", display_name: "GPT-4o (OpenRouter)", model_type: "chat", model_category: "对话/Coding" },
    ],
  },
  {
    provider_name: "groq",
    display_name: "Groq",
    api_base_url: "https://api.groq.com/openai/v1",
    litellm_prefix: "groq",
    supports_chat: true,
    supports_embedding: false,
    supports_reasoner: false,
    category: "推理平台",
    default_models: [
      { model_name: "llama-3.3-70b-versatile", display_name: "Llama 3.3 70B (Groq)", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "llama-3.1-8b-instant", display_name: "Llama 3.1 8B (Groq)", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "deepseek-r1-distill-llama-70b", display_name: "DeepSeek R1 70B (Groq)", model_type: "chat", model_category: "推理模型" },
    ],
  },
  {
    provider_name: "deepinfra",
    display_name: "DeepInfra",
    api_base_url: "https://api.deepinfra.com/v1/openai",
    litellm_prefix: "deepinfra",
    supports_chat: true,
    supports_embedding: true,
    supports_reasoner: false,
    category: "推理平台",
    default_models: [
      { model_name: "meta-llama/Llama-3.3-70B-Instruct", display_name: "Llama 3.3 70B", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "deepseek-ai/DeepSeek-V3", display_name: "DeepSeek V3", model_type: "chat", model_category: "对话/Coding" },
      { model_name: "BAAI/bge-m3", display_name: "BGE-M3", model_type: "embedding", model_category: "嵌入式模型" },
    ],
  },
  {
    provider_name: "huggingface",
    display_name: "HuggingFace",
    api_base_url: "https://api-inference.huggingface.co/models",
    litellm_prefix: "huggingface",
    supports_chat: true,
    supports_embedding: true,
    supports_reasoner: false,
    category: "推理平台",
    default_models: [],
  },
];

export default LITELLM_PROVIDERS;

/**
 * 根据 provider_name 查找预设
 */
export function findProviderPreset(providerName: string): LiteLLMProviderPreset | undefined {
  return LITELLM_PROVIDERS.find((p) => p.provider_name === providerName);
}

/**
 * 根据 provider_name 获取推荐模型列表
 */
export function getProviderModels(providerName: string): LiteLLMModelPreset[] {
  return findProviderPreset(providerName)?.default_models ?? [];
}
