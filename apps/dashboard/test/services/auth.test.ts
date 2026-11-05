import { getSession, setSession, clearSession, isAuthenticated } from '@/services/auth/session';
import { Session } from '@/types/auth';

const mockSession: Session = {
  user: {
    id: 'u1', email: 'test@example.com', fullName: 'Test',
    role: 'admin', status: 'active',
    createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
  },
  accessToken: 'tok',
  refreshToken: 'ref',
  expiresAt: new Date(Date.now() + 60000).toISOString(),
};

beforeEach(() => localStorage.clear());

describe('session storage', () => {
  it('returns null when no session is stored', () => {
    expect(getSession()).toBeNull();
  });

  it('round-trips a session', () => {
    setSession(mockSession);
    expect(getSession()?.user.id).toBe('u1');
  });

  it('clears the session', () => {
    setSession(mockSession);
    clearSession();
    expect(getSession()).toBeNull();
  });

  it('reports authentication state correctly', () => {
    expect(isAuthenticated()).toBe(false);
    setSession(mockSession);
    expect(isAuthenticated()).toBe(true);
  });
});
