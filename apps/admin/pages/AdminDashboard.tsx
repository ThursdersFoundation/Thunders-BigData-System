import { useState } from 'react';

interface SystemStats {
  usersCount: number;
  activePipelines: number;
  storageUsedGb: number;
  failedRuns24h: number;
}

// TODO: replace with GET /api/v1/metrics/summary once the admin API client
// exists; keep the same shape to make the swap trivial.
const MOCK_STATS: SystemStats = {
  usersCount: 42,
  activePipelines: 8,
  storageUsedGb: 742.5,
  failedRuns24h: 3,
};

/** System overview with KPI cards for the admin console landing page. */
export default function AdminDashboard() {
  const [stats] = useState<SystemStats>(MOCK_STATS);

  const cards = [
    { title: 'Users', value: String(stats.usersCount), hint: 'registered accounts' },
    { title: 'Active Pipelines', value: String(stats.activePipelines), hint: 'currently scheduled' },
    { title: 'Storage Used', value: `${stats.storageUsedGb.toFixed(1)} GB`, hint: 'across all datasets' },
    { title: 'Failed Runs (24h)', value: String(stats.failedRuns24h), hint: 'requires review' },
  ];

  return (
    <section>
      <h1 className="text-lg font-semibold text-slate-900">System Overview</h1>
      <p className="mt-1 text-sm text-slate-600">
        Platform health at a glance. Values are currently mocked on the client.
      </p>

      <div className="mt-6 grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
        {cards.map((card) => (
          <div key={card.title} className="admin-card flex flex-col gap-1">
            <span className="text-xs font-medium uppercase tracking-wide text-slate-500">
              {card.title}
            </span>
            <span className="text-2xl font-semibold text-slate-900">{card.value}</span>
            <span className="text-xs text-slate-400">{card.hint}</span>
          </div>
        ))}
      </div>
    </section>
  );
}
