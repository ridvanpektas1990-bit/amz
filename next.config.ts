import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  eslint: { ignoreDuringBuilds: true },
  // typescript: { ignoreBuildErrors: true }, // nur aktivieren, wenn irgendwann TS selbst blockt
};

export default nextConfig;
