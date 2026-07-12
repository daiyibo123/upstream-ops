import { StrictMode } from 'react'
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
import HomePage from '@/app/home-page'
import DashboardPage from '@/app/page'
import ChannelsPage from '@/app/channels-page'
import KeysPage from '@/app/keys-page'
import GatewayPage from '@/app/gateway-page'
import UsagePage from '@/app/usage-page'
import SettingsPage from '@/app/settings-page'
import '@/app/globals.css'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <ThemeProvider attribute="class" defaultTheme="light" enableSystem disableTransitionOnChange>
      <AuthProvider>
        <RefreshProvider>
          <BrowserRouter>
            <AddChannelProvider>
              <Routes>
                <Route index element={<HomePage />} />
                <Route path="login" element={<LoginPage />} />
                <Route element={<AuthGate><AppShell /></AuthGate>}>
                  <Route path="dashboard" element={<DashboardPage />} />
                  <Route path="channels" element={<ChannelsPage />} />
                  <Route path="keys" element={<KeysPage />} />
                  <Route path="gateway" element={<GatewayPage />} />
                  <Route path="usage" element={<UsagePage />} />
                  <Route path="notifications" element={<Navigate to="/settings?tab=notifications" replace />} />
                  <Route path="settings" element={<SettingsPage />} />
                </Route>
              </Routes>
            </AddChannelProvider>
          </BrowserRouter>
        </RefreshProvider>
          <Toaster richColors closeButton position="top-right" />
      </AuthProvider>
    </ThemeProvider>
  </StrictMode>,
)
