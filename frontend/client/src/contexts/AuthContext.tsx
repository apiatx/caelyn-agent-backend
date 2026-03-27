import { createContext, useContext, useState, useEffect, useCallback, ReactNode } from 'react';
import { useLocation } from 'wouter';

const AGENT_BACKEND_URL = 'https://fast-api-server-trading-agent-aidanpilon.replit.app';

interface AuthContextType {
  token: string | null;
  userId: string | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  login: (username: string, password: string, rememberMe: boolean) => Promise<void>;
  logout: () => void;
  getAuthHeaders: () => Record<string, string>;
}

const AuthContext = createContext<AuthContextType | null>(null);

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within AuthProvider');
  return ctx;
}

function getStoredToken(): string | null {
  return localStorage.getItem('caelyn_token') || sessionStorage.getItem('caelyn_token');
}

function storeToken(token: string, rememberMe: boolean) {
  if (rememberMe) {
    localStorage.setItem('caelyn_token', token);
    sessionStorage.removeItem('caelyn_token');
  } else {
    sessionStorage.setItem('caelyn_token', token);
    localStorage.removeItem('caelyn_token');
  }
}

function clearToken() {
  localStorage.removeItem('caelyn_token');
  sessionStorage.removeItem('caelyn_token');
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [token, setToken] = useState<string | null>(null);
  const [userId, setUserId] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [, navigate] = useLocation();

  // Verify stored token on mount
  useEffect(() => {
    const stored = getStoredToken();
    if (!stored) {
      setIsLoading(false);
      return;
    }
    // Validate token server-side
    fetch(`${AGENT_BACKEND_URL}/api/auth/verify`, {
      headers: { Authorization: `Bearer ${stored}` },
    })
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (data?.valid) {
          setToken(stored);
          setUserId(data.user_id);
        } else {
          clearToken();
        }
      })
      .catch(() => {
        // Network error — allow continuing with stored token optimistically
        setToken(stored);
      })
      .finally(() => setIsLoading(false));
  }, []);

  const login = useCallback(async (username: string, password: string, rememberMe: boolean) => {
    const res = await fetch(`${AGENT_BACKEND_URL}/api/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password, remember_me: rememberMe }),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail || 'Login failed');
    }
    const data = await res.json();
    storeToken(data.token, rememberMe);
    setToken(data.token);
    setUserId(data.user_id);
  }, []);

  const logout = useCallback(() => {
    clearToken();
    setToken(null);
    setUserId(null);
    navigate('/login');
  }, [navigate]);

  const getAuthHeaders = useCallback((): Record<string, string> => {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      'X-API-Key': 'hippo_ak_7f3x9k2m4p8q1w5t',
    };
    if (token) {
      headers['Authorization'] = `Bearer ${token}`;
    }
    return headers;
  }, [token]);

  return (
    <AuthContext.Provider value={{
      token,
      userId,
      isAuthenticated: !!token,
      isLoading,
      login,
      logout,
      getAuthHeaders,
    }}>
      {children}
    </AuthContext.Provider>
  );
}
