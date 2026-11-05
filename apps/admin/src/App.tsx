import { Route, Routes } from 'react-router-dom';

import AdminSidebar from './components/AdminSidebar';
import RoleGuard from './components/RoleGuard';
import AdminDashboard from './pages/AdminDashboard';
import UserManagement from './pages/UserManagement';

/**
 * Top-level routing for the admin console.
 * Every admin route is wrapped in a RoleGuard (admin role required).
 */
export default function App() {
  return (
    <div className="flex min-h-screen bg-slate-100">
      <AdminSidebar />
      <main className="min-w-0 flex-1 p-6">
        <Routes>
          <Route
            path="/"
            element={
              <RoleGuard role="admin">
                <AdminDashboard />
              </RoleGuard>
            }
          />
          <Route
            path="/users"
            element={
              <RoleGuard role="admin">
                <UserManagement />
              </RoleGuard>
            }
          />
        </Routes>
      </main>
    </div>
  );
}
