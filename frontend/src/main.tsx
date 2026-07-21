import { StrictMode, Suspense, lazy } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import '@fontsource-variable/geist'
import '@fontsource-variable/geist-mono'
import { ThemeProvider } from '@/components/theme-provider'
import { AuthProvider } from '@/lib/auth-context'
import { RefreshProvider } from '@/lib/refresh-context'
import { AddChannelProvider } from '@/lib/add-channel-context'
import { AuthGate } from '@/components/auth/auth-gate'
import { AppShell } from '@/components/app-shell'
import { Toaster } from '@/components/ui/sonner'
import { LoginPage } from '@/components/auth/login-page'
import '@/app/globals.css'

const HomePage = lazy(() => import('@/app/home-page'))
const DashboardPage = lazy(() => import('@/app/page'))
const ChannelsPage = lazy(() => import('@/app/channels-page'))
const KeysPage = lazy(() => import('@/app/keys-page'))
const GatewayPage = lazy(() => import('@/app/gateway-page'))
const UsagePage = lazy(() => import('@/app/usage-page'))
const SettingsPage = lazy(() => import('@/app/settings-page'))
const OAuthPage = lazy(() => import('@/app/oauth-page'))

function RouteFallback() {
  return <div className="px-4 py-10 text-sm text-muted-foreground">页面加载中...</div>
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <ThemeProvider attribute="class" defaultTheme="light" enableSystem disableTransitionOnChange>
      <AuthProvider>
        <RefreshProvider>
          <BrowserRouter>
            <AddChannelProvider>
				<Suspense fallback={<RouteFallback />}>
				<Routes>
                <Route index element={<HomePage />} />
                <Route path="login" element={<LoginPage />} />
                <Route element={<AuthGate><AppShell /></AuthGate>}>
                  <Route path="dashboard" element={<DashboardPage />} />
                  <Route path="channels" element={<ChannelsPage />} />
                  <Route path="keys" element={<KeysPage />} />
                  <Route path="gateway" element={<GatewayPage />} />
                  <Route path="usage" element={<UsagePage />} />
                  <Route path="oauth" element={<OAuthPage />} />
                  <Route path="notifications" element={<Navigate to="/settings?tab=notifications" replace />} />
                  <Route path="settings" element={<SettingsPage />} />
                </Route>
				</Routes>
				</Suspense>
            </AddChannelProvider>
          </BrowserRouter>
        </RefreshProvider>
          <Toaster richColors closeButton position="top-right" />
      </AuthProvider>
    </ThemeProvider>
  </StrictMode>,
)
