/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { reactRouter } from '@react-router/dev/vite';
import tailwindcss from '@tailwindcss/vite';
import { defineConfig, loadEnv, type Plugin } from 'vite';
import tsconfigPaths from 'vite-tsconfig-paths';
import babel from 'vite-plugin-babel';

const ReactCompilerConfig = {
  // compilationMode: 'all' is the default - compile everything
  // No longer need 'annotation' mode since all components are compatible
};

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const clusterProxyTarget = env.VITE_CLUSTER_PROXY_TARGET;
  const yarnAssistProxyTarget = env.VITE_YARN_ASSIST_PROXY_TARGET;
  const mockMode = env.VITE_API_MOCK_MODE ?? 'static';

  const isDev = mode === 'development';

  // Development-only: load the YARN Assist mock plugin only in static mock mode
  // without a real backend proxy. Import is conditional so it never enters the
  // production bundle.
  let assistMockPlugin: Plugin | null = null;
  if (isDev && mockMode === 'static' && !yarnAssistProxyTarget) {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const { yarnAssistMockPlugin } = require('./dev/yarnAssistMockPlugin') as {
      yarnAssistMockPlugin: () => Plugin;
    };
    assistMockPlugin = yarnAssistMockPlugin();
  }

  // Build proxy config
  type ProxyEntry = { target: string; changeOrigin: boolean; secure: boolean };
  const proxyConfig: Record<string, ProxyEntry> = {};

  if (clusterProxyTarget) {
    proxyConfig['/ws/v1/cluster'] = {
      target: clusterProxyTarget,
      changeOrigin: true,
      secure: false,
    };
    proxyConfig['/conf'] = {
      target: clusterProxyTarget,
      changeOrigin: true,
      secure: false,
    };
  }

  if (yarnAssistProxyTarget) {
    proxyConfig['/yarn-assist'] = {
      target: yarnAssistProxyTarget,
      changeOrigin: true,
      secure: false,
    };
    proxyConfig['/yarn-mcp'] = {
      target: yarnAssistProxyTarget,
      changeOrigin: true,
      secure: false,
    };
  }

  return {
    base: '/scheduler-ui/',
    plugins: [
      tailwindcss(),
      reactRouter(),
      babel({
        filter: /\.[jt]sx?$/,
        babelConfig: {
          presets: ['@babel/preset-typescript'],
          plugins: [['babel-plugin-react-compiler', ReactCompilerConfig]],
        },
      }),
      tsconfigPaths(),
      ...(assistMockPlugin ? [assistMockPlugin] : []),
    ],
    // Proxy configuration for local development only
    // In WAR deployment, servlet filter handles API routing to ResourceManager
    server: Object.keys(proxyConfig).length > 0 ? { proxy: proxyConfig } : undefined,
  };
});
