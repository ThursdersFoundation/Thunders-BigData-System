import type { ReactNode } from 'react';

interface RoleGuardProps {
  /** Minimum role required to render the children. */
  role: 'admin' | 'editor' | 'viewer';
  children: ReactNode;
}

const STORAGE_KEY = 'thunders.admin.role';

/** Read the role persisted at login time (TODO: parse from JWT claims). */
export function getCurrentRole(): string | null {
  if (typeof window === 'undefined') {
    return null;
  }
  return window.localStorage.getItem(STORAGE_KEY);
}

/** Render children only when the current role matches; otherwise show Forbidden. */
export default function RoleGuard({ role, children }: RoleGuardProps) {
  const currentRole = getCurrentRole();

  if (currentRole !== role) {
    return (
      <div className="admin-card mx-auto mt-12 max-w-md text-center">
        <h1 className="text-xl font-semibold text-red-700">Forbidden</h1>
        <p className="mt-2 text-sm text-red-600">
          Your role &quot;{currentRole ?? 'anonymous'}&quot; cannot access this area.
        </p>
        <p className="mt-4 text-xs text-slate-500">
          Set localStorage key &quot;thunders.admin.role&quot; to &quot;{role}&quot; for
          local testing.
        </p>
      </div>
    );
  }

  return <>{children}</>;
}
