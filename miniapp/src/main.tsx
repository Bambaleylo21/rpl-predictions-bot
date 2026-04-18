import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import WebApp from '@twa-dev/sdk'
import './index.css'
import App from './App.tsx'

// Safe init: works inside Telegram Mini App and does not crash in normal browser.
try {
  WebApp.ready()
  WebApp.expand()
} catch {
  // Outside Telegram environment.
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
