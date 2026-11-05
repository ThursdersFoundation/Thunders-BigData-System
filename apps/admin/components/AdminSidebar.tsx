import { NavLink } from 'react-router-dom';

const NAV_ITEMS = [
  { to: '/', label: 'Dashboard', end: true },
  { to: '/users', label: 'User Management', end: false },
];

/** Sidebar navigation for the admin console. */
export default function AdminSidebar() {
  return (
    <aside className="flex w-56 shrink-0 flex-col border-r border-slate-200 bg-white">
      <div className="flex items-center gap-2 px-5 py-5">
        <span className="flex h-8 w-8 items-center justify-center rounded bg-blue-600 text-sm font-bold text-white">
          T
        </span>
        <div>
          <p className="text-sm font-semibold text-slate-900">Thunders</p>
          <p className="text-xs text-slate-500">Admin Console</p>
        </div>
      </div>
      <nav className="flex flex-1 flex-col gap-1 px-3">
        {NAV_ITEMS.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.end}
            className={({ isActive }) =>
              `rounded-md px-3 py-2 text-sm font-medium transition ${
                isActive ? 'bg-blue-50 text-blue-700' : 'text-slate-600 hover:bg-slate-100'
              }`
            }
          >
            {item.label}
          </NavLink>
        ))}
      </nav>
      <p className="px-5 py-4 text-xs text-slate-400">Restricted area</p>
    </aside>
  );
}
