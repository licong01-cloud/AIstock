/** @type {import('next').NextConfig} */
const paperV2ApiProxyTarget = (process.env.PAPER_V2_API_PROXY_TARGET || "").replace(/\/$/, "");

const nextConfig = {
  reactStrictMode: true,
  async rewrites() {
    if (!paperV2ApiProxyTarget) return [];
    return [
      {
        source: "/api/v1/:path*",
        destination: `${paperV2ApiProxyTarget}/:path*`,
      },
    ];
  },
};

export default nextConfig;
