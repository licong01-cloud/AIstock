// 简化的类名合并函数，不依赖TailwindCSS
export function cn(...inputs: (string | undefined | null | false)[]) {
  return inputs.filter(Boolean).join(' ');
}
