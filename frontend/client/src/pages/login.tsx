import { useState, useEffect } from 'react';
import { useLocation } from 'wouter';
import { useAuth } from '@/contexts/AuthContext';
import { Loader2, Eye, EyeOff, Shield } from 'lucide-react';
import caelynLogo from "@assets/image_1771576238262.png";
import caelynFairy from "@assets/image_1771572217667.png";

export default function LoginPage() {
  const { login, isAuthenticated } = useAuth();
  const [, navigate] = useLocation();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [rememberMe, setRememberMe] = useState(false);
  const [showPassword, setShowPassword] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    if (isAuthenticated) navigate('/app/caelyn-ai');
  }, [isAuthenticated, navigate]);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!username.trim() || !password) return;
    setError('');
    setIsLoading(true);
    try {
      await login(username.trim(), password, rememberMe);
      navigate('/app/caelyn-ai');
    } catch (err: any) {
      setError(err.message || 'Invalid credentials. Please try again.');
    } finally {
      setIsLoading(false);
    }
  }

  return (
    <div className="login-root">
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

        .login-root {
          min-height: 100vh;
          background: #050608;
          font-family: 'Outfit', sans-serif;
          display: flex;
          position: relative;
          overflow: hidden;
          color: #e2e8f0;
        }

        .login-ambient {
          position: fixed;
          inset: 0;
          background:
            radial-gradient(ellipse 900px 700px at 15% 20%, rgba(40,160,220,0.05) 0%, transparent 70%),
            radial-gradient(ellipse 700px 600px at 85% 75%, rgba(60,180,240,0.03) 0%, transparent 70%),
            radial-gradient(ellipse 1100px 500px at 50% 50%, rgba(50,170,230,0.02) 0%, transparent 60%);
          pointer-events: none;
          z-index: 0;
        }

        .login-brand {
          width: 52%;
          min-height: 100vh;
          padding: 3.5rem;
          border-right: 1px solid rgba(255,255,255,0.06);
          display: flex;
          flex-direction: column;
          justify-content: space-between;
          position: relative;
          z-index: 1;
        }

        .login-form-panel {
          flex: 1;
          display: flex;
          align-items: center;
          justify-content: center;
          padding: 3rem 2.5rem;
          position: relative;
          z-index: 1;
        }

        .login-mobile-logo {
          display: none;
          text-align: center;
          margin-bottom: 2rem;
        }

        @media (max-width: 800px) {
          .login-brand { display: none; }
          .login-mobile-logo { display: block; }
          .login-form-panel { padding: 2rem 1.25rem; align-items: flex-start; padding-top: 3rem; }
        }

        .ice { color: #5cc8f0; }
        .gradient-text {
          background: linear-gradient(135deg, #e0f0ff 0%, #5cc8f0 40%, #2090d0 100%);
          -webkit-background-clip: text;
          -webkit-text-fill-color: transparent;
          background-clip: text;
        }

        .login-input {
          width: 100%;
          box-sizing: border-box;
          background: rgba(255,255,255,0.03);
          border: 1px solid rgba(255,255,255,0.09);
          border-radius: 8px;
          padding: 0.75rem 1rem;
          color: #e2e8f0;
          font-size: 0.9rem;
          font-family: 'Outfit', sans-serif;
          transition: border-color 0.2s, box-shadow 0.2s;
        }
        .login-input::placeholder { color: rgba(255,255,255,0.18); }
        .login-input:focus {
          outline: none;
          border-color: rgba(92,200,240,0.45);
          box-shadow: 0 0 0 2px rgba(92,200,240,0.1);
        }

        .login-input-password {
          padding-right: 2.75rem;
        }

        .sign-in-btn {
          width: 100%;
          padding: 0.88rem;
          background: linear-gradient(135deg, #1a78b0, #3bacd8);
          border: 1px solid rgba(92,200,240,0.25);
          border-radius: 8px;
          color: #fff;
          font-family: 'Outfit', sans-serif;
          font-weight: 600;
          font-size: 0.95rem;
          letter-spacing: 0.02em;
          cursor: pointer;
          transition: all 0.25s;
          display: flex;
          align-items: center;
          justify-content: center;
          gap: 0.5rem;
          box-shadow: 0 4px 24px rgba(92,200,240,0.1);
        }
        .sign-in-btn:hover:not(:disabled) {
          background: linear-gradient(135deg, #2090d0, #5cc8f0);
          box-shadow: 0 4px 32px rgba(92,200,240,0.22);
          transform: translateY(-1px);
        }
        .sign-in-btn:disabled {
          opacity: 0.38;
          cursor: not-allowed;
          transform: none;
        }

        .feature-dot {
          width: 6px;
          height: 6px;
          border-radius: 50%;
          background: #5cc8f0;
          box-shadow: 0 0 8px rgba(92,200,240,0.4);
          flex-shrink: 0;
        }

        @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
        .spinning { animation: spin 1s linear infinite; }
      `}</style>

      <div className="login-ambient" />

      {/* ── LEFT BRAND PANEL ── */}
      <div className="login-brand">
        <div>
          {/* Logo */}
          <div style={{ maxWidth: 280, marginBottom: '2.5rem' }}>
            <img src={caelynLogo} alt="CaelynAI" style={{ width: '100%', height: 'auto', display: 'block' }} />
          </div>

          {/* Badge */}
          <div style={{
            display: 'inline-block', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.68rem', fontWeight: 500,
            letterSpacing: '0.12em', textTransform: 'uppercase', color: '#5cc8f0',
            border: '1px solid rgba(92,200,240,0.2)', borderRadius: 100, padding: '0.35rem 1rem',
            marginBottom: '2rem', background: 'rgba(92,200,240,0.04)'
          }}>
            Cross-Asset Trading Intelligence
          </div>

          {/* Headline */}
          <h1 style={{ fontSize: 'clamp(1.7rem, 2.8vw, 2.4rem)', fontWeight: 700, lineHeight: 1.18, letterSpacing: '-0.03em', marginBottom: '1.1rem' }}>
            One agent. Every market.<br />
            <span className="gradient-text">Institutional-grade signal.</span>
          </h1>

          <p style={{ color: '#475569', fontSize: '0.93rem', lineHeight: 1.7, maxWidth: 400, marginBottom: '2.5rem' }}>
            19 live data sources. Deterministic scoring. Specific trade plans with entry, stop, and target — not market commentary.
          </p>

          {/* Feature list */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: '0.9rem' }}>
            {[
              { label: 'Real-time Data', desc: '19 providers aggregated per request' },
              { label: 'AI Synthesis', desc: 'Claude-powered trade plans with full context' },
              { label: 'Cross-asset', desc: 'Stocks, crypto & commodities on one scorecard' },
              { label: 'HyperLiquid Perps', desc: 'Live funding rates, OI, squeeze candidates' },
            ].map(f => (
              <div key={f.label} style={{ display: 'flex', alignItems: 'center', gap: '0.9rem' }}>
                <div className="feature-dot" />
                <div>
                  <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.7rem', fontWeight: 500, color: '#5cc8f0', letterSpacing: '0.05em' }}>{f.label}</span>
                  <span style={{ color: '#475569', fontSize: '0.8rem', marginLeft: '0.6rem' }}>{f.desc}</span>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Bottom: fairy image + tagline */}
        <div>
          <div style={{ display: 'flex', justifyContent: 'center', marginBottom: '1.25rem' }}>
            <img
              src={caelynFairy}
              alt="CaelynAI"
              style={{
                width: 180, height: 180, borderRadius: '50%', objectFit: 'cover',
                border: '1px solid rgba(92,200,240,0.12)',
                boxShadow: '0 0 40px rgba(92,200,240,0.07), 0 0 80px rgba(40,160,220,0.05)'
              }}
            />
          </div>
          <div style={{ width: 40, height: 1.5, background: 'linear-gradient(135deg, #2090d0, #5cc8f0)', margin: '0 auto 1rem', borderRadius: 2 }} />
          <p style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.72rem', color: '#64748b', letterSpacing: '0.05em', textAlign: 'center' }}>
            <span style={{ background: 'linear-gradient(135deg, #2090d0, #3b82f6, #80d8f8)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent', backgroundClip: 'text', fontWeight: 600 }}>CaelynAI</span> — Signal over noise.
          </p>
        </div>
      </div>

      {/* ── RIGHT FORM PANEL ── */}
      <div className="login-form-panel">
        <div style={{ width: '100%', maxWidth: 420 }}>

          {/* Mobile-only logo */}
          <div className="login-mobile-logo">
            <div style={{ maxWidth: 200, margin: '0 auto 1rem' }}>
              <img src={caelynLogo} alt="CaelynAI" style={{ width: '100%', height: 'auto' }} />
            </div>
          </div>

          {/* Section label */}
          <h3 style={{
            fontFamily: "'JetBrains Mono', monospace", fontSize: '0.72rem', fontWeight: 500,
            letterSpacing: '0.12em', textTransform: 'uppercase', color: '#5cc8f0', marginBottom: '0.6rem'
          }}>
            Terminal Access
          </h3>

          <h2 style={{ fontSize: '1.6rem', fontWeight: 600, letterSpacing: '-0.02em', color: '#e2e8f0', marginBottom: '0.4rem' }}>
            Sign in to your account
          </h2>
          <p style={{ color: '#475569', fontSize: '0.88rem', marginBottom: '2rem', lineHeight: 1.6 }}>
            Enter your credentials to access the trading terminal.
          </p>

          {/* Form card */}
          <div style={{
            background: 'rgba(10,12,18,0.85)',
            border: '1px solid rgba(255,255,255,0.07)',
            borderRadius: 14,
            padding: '1.75rem 2rem',
          }}>
            <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: '1.2rem' }}>

              {/* Username */}
              <div>
                <label style={{
                  display: 'block', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.65rem',
                  fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase',
                  color: '#64748b', marginBottom: '0.45rem'
                }}>
                  Username
                </label>
                <input
                  type="text"
                  className="login-input"
                  value={username}
                  onChange={e => setUsername(e.target.value)}
                  placeholder="Enter username"
                  autoComplete="username"
                  required
                />
              </div>

              {/* Password */}
              <div>
                <label style={{
                  display: 'block', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.65rem',
                  fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase',
                  color: '#64748b', marginBottom: '0.45rem'
                }}>
                  Password
                </label>
                <div style={{ position: 'relative' }}>
                  <input
                    type={showPassword ? 'text' : 'password'}
                    className="login-input login-input-password"
                    value={password}
                    onChange={e => setPassword(e.target.value)}
                    placeholder="Enter password"
                    autoComplete="current-password"
                    required
                  />
                  <button
                    type="button"
                    onClick={() => setShowPassword(v => !v)}
                    tabIndex={-1}
                    style={{
                      position: 'absolute', right: '0.8rem', top: '50%', transform: 'translateY(-50%)',
                      background: 'none', border: 'none', cursor: 'pointer',
                      color: 'rgba(255,255,255,0.28)', display: 'flex', alignItems: 'center',
                      padding: 0
                    }}
                  >
                    {showPassword ? <EyeOff size={15} /> : <Eye size={15} />}
                  </button>
                </div>
              </div>

              {/* Remember me */}
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                <button
                  type="button"
                  role="checkbox"
                  aria-checked={rememberMe}
                  onClick={() => setRememberMe(v => !v)}
                  style={{
                    width: 17, height: 17, borderRadius: 4, flexShrink: 0,
                    border: rememberMe ? '1px solid rgba(92,200,240,0.55)' : '1px solid rgba(255,255,255,0.14)',
                    background: rememberMe ? 'rgba(92,200,240,0.12)' : 'rgba(255,255,255,0.03)',
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                    cursor: 'pointer', transition: 'all 0.2s',
                    boxShadow: rememberMe ? '0 0 10px rgba(92,200,240,0.12)' : 'none',
                    padding: 0
                  }}
                >
                  {rememberMe && (
                    <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="#5cc8f0" strokeWidth={3.5}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                    </svg>
                  )}
                </button>
                <span
                  onClick={() => setRememberMe(v => !v)}
                  style={{ fontSize: '0.84rem', color: '#475569', cursor: 'pointer', userSelect: 'none' }}
                >
                  Remember me for 30 days
                </span>
              </div>

              {/* Error */}
              {error && (
                <div style={{
                  background: 'rgba(239,68,68,0.07)', border: '1px solid rgba(239,68,68,0.18)',
                  borderRadius: 8, padding: '0.7rem 1rem', color: '#f87171', fontSize: '0.82rem', lineHeight: 1.5
                }}>
                  {error}
                </div>
              )}

              {/* Submit */}
              <button
                type="submit"
                disabled={isLoading || !username.trim() || !password}
                className="sign-in-btn"
              >
                {isLoading ? (
                  <>
                    <Loader2 size={15} className="spinning" />
                    Authenticating...
                  </>
                ) : 'Sign In'}
              </button>
            </form>

            {/* Security note */}
            <div style={{
              marginTop: '1.4rem', paddingTop: '1.1rem',
              borderTop: '1px solid rgba(255,255,255,0.06)',
              display: 'flex', alignItems: 'flex-start', gap: '0.5rem'
            }}>
              <Shield size={12} style={{ color: 'rgba(255,255,255,0.18)', flexShrink: 0, marginTop: 2 }} />
              <p style={{ color: 'rgba(255,255,255,0.18)', fontSize: '0.73rem', lineHeight: 1.55, margin: 0 }}>
                Secured with JWT authentication. Tokens expire after 24h unless Remember me is enabled.
              </p>
            </div>
          </div>

          {/* Bottom tagline */}
          <p style={{
            textAlign: 'center', marginTop: '1.5rem',
            fontFamily: "'JetBrains Mono', monospace", fontSize: '0.7rem',
            color: '#475569', letterSpacing: '0.04em'
          }}>
            Not financial advice. All trading involves risk.
          </p>
        </div>
      </div>
    </div>
  );
}
