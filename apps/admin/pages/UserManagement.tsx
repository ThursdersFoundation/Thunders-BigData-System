import { useState } from 'react';

interface AdminUser {
  id: string;
  name: string;
  email: string;
  role: 'admin' | 'analyst' | 'viewer';
  disabled: boolean;
}

const INITIAL_USERS: AdminUser[] = [
  { id: 'u-001', name: 'Ada Lovelace', email: 'ada@thunders.io', role: 'admin', disabled: false },
  { id: 'u-002', name: 'Grace Hopper', email: 'grace@thunders.io', role: 'analyst', disabled: false },
  { id: 'u-003', name: 'Alan Turing', email: 'alan@thunders.io', role: 'viewer', disabled: false },
  { id: 'u-004', name: 'Edsger Dijkstra', email: 'edsger@thunders.io', role: 'viewer', disabled: true },
];

/**
 * User management table with inline role editing and a disable toggle.
 * State is local for now; TODO wire to PATCH /api/v1/users/{id}.
 */
export default function UserManagement() {
  const [users, setUsers] = useState<AdminUser[]>(INITIAL_USERS);

  const updateRole = (userId: string, role: AdminUser['role']) => {
    setUsers((prev) => prev.map((user) => (user.id === userId ? { ...user, role } : user)));
  };

  const toggleDisabled = (userId: string) => {
    setUsers((prev) =>
      prev.map((user) => (user.id === userId ? { ...user, disabled: !user.disabled } : user))
    );
  };

  return (
    <section>
      <h1 className="text-lg font-semibold text-slate-900">User Management</h1>
      <p className="mt-1 text-sm text-slate-600">
        Change roles and disable accounts. Changes are local until the admin API is wired up.
      </p>

      <div className="admin-card mt-6 overflow-x-auto">
        <table className="min-w-full divide-y divide-slate-200 text-sm">
          <thead>
            <tr>
              <th className="px-4 py-3 text-left text-xs font-semibold uppercase text-slate-500">User</th>
              <th className="px-4 py-3 text-left text-xs font-semibold uppercase text-slate-500">Email</th>
              <th className="px-4 py-3 text-left text-xs font-semibold uppercase text-slate-500">Role</th>
              <th className="px-4 py-3 text-left text-xs font-semibold uppercase text-slate-500">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {users.map((user) => (
              <tr key={user.id} className={user.disabled ? 'opacity-50' : ''}>
                <td className="px-4 py-3 font-medium text-slate-800">{user.name}</td>
                <td className="px-4 py-3 text-slate-600">{user.email}</td>
                <td className="px-4 py-3">
                  <select
                    value={user.role}
                    onChange={(event) => updateRole(user.id, event.target.value as AdminUser['role'])}
                    className="rounded border border-slate-200 px-2 py-1 text-xs"
                  >
                    <option value="admin">admin</option>
                    <option value="analyst">analyst</option>
                    <option value="viewer">viewer</option>
                  </select>
                </td>
                <td className="px-4 py-3">
                  <button
                    type="button"
                    onClick={() => toggleDisabled(user.id)}
                    className={`rounded-md px-3 py-1.5 text-xs font-medium transition ${
                      user.disabled
                        ? 'bg-emerald-50 text-emerald-700 hover:bg-emerald-100'
                        : 'bg-red-50 text-red-700 hover:bg-red-100'
                    }`}
                  >
                    {user.disabled ? 'Enable' : 'Disable'}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
