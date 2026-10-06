'use client';

import React, { useState, useMemo } from 'react';
import { 
  Car, 
  Search, 
  Compass, 
  MapPin, 
  TrendingUp
} from 'lucide-react';
import { Driver, LocationTelemetry } from '@/types';

interface FleetSidebarProps {
  drivers: Driver[];
  telemetryMap: Record<string, LocationTelemetry>;
  selectedDriverId: string | null;
  onSelectDriver: (driverId: string) => void;
}

export const FleetSidebar: React.FC<FleetSidebarProps> = ({
  drivers,
  telemetryMap,
  selectedDriverId,
  onSelectDriver,
}) => {
  const [search, setSearch] = useState('');
  const [filter, setFilter] = useState<'all' | 'moving' | 'idle'>('all');

  const getHeadingDirection = (degree: number): string => {
    const directions = ['U (N)', 'TL (NE)', 'T (E)', 'TG (SE)', 'S (S)', 'BD (SW)', 'B (W)', 'BL (NW)'];
    const index = Math.round(((degree % 360) / 45)) % 8;
    return directions[index];
  };

  const filteredDrivers = useMemo(() => {
    return drivers.filter((driver) => {
      const matchSearch =
        driver.name.toLowerCase().includes(search.toLowerCase()) ||
        driver.code.toLowerCase().includes(search.toLowerCase()) ||
        driver.vehicle.toLowerCase().includes(search.toLowerCase());

      if (!matchSearch) return false;

      const telem = telemetryMap[driver.code] || telemetryMap[driver.id];
      const isMoving = telem && telem.speed > 2;

      if (filter === 'moving') return isMoving;
      if (filter === 'idle') return !isMoving;
      return true;
    });
  }, [drivers, telemetryMap, search, filter]);

  return (
    <aside className="w-80 sm:w-96 border-r border-slate-800/80 bg-slate-950/70 backdrop-blur-xl flex flex-col z-10 shrink-0 h-[calc(100vh-4rem)]">
      {/* Search & Filter Header */}
      <div className="p-4 border-b border-slate-800/80 space-y-3">
        <div className="relative">
          <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-slate-500" />
          <input
            type="text"
            placeholder="Cari supir, kode, plat..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="w-full pl-9 pr-3 py-2 bg-slate-900/90 border border-slate-800 rounded-lg text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:border-cyan-500 transition font-mono"
          />
        </div>

        {/* Filter Pills */}
        <div className="flex space-x-1.5 text-xs font-mono">
          {(['all', 'moving', 'idle'] as const).map((tab) => (
            <button
              key={tab}
              onClick={() => setFilter(tab)}
              className={`flex-1 py-1.5 rounded-md capitalize transition ${
                filter === tab
                  ? 'bg-cyan-500/20 text-cyan-300 border border-cyan-500/40 font-semibold'
                  : 'bg-slate-900/60 text-slate-400 hover:text-slate-200 border border-slate-800/60'
              }`}
            >
              {tab === 'all' ? `Semua (${drivers.length})` : tab}
            </button>
          ))}
        </div>
      </div>

      {/* Driver List */}
      <div className="flex-1 overflow-y-auto p-3 space-y-2.5">
        {filteredDrivers.length === 0 ? (
          <div className="text-center py-12 text-slate-500 text-xs font-mono">
            Tidak ada armada yang sesuai
          </div>
        ) : (
          filteredDrivers.map((driver) => {
            const telem = telemetryMap[driver.code] || telemetryMap[driver.id];
            const isSelected = selectedDriverId === driver.id || selectedDriverId === driver.code;
            const isMoving = telem && telem.speed > 2;

            return (
              <div
                key={driver.id || driver.code}
                onClick={() => onSelectDriver(driver.id || driver.code)}
                className={`group p-3.5 rounded-xl border transition-all cursor-pointer relative overflow-hidden ${
                  isSelected
                    ? 'bg-slate-900/90 border-cyan-500/60 shadow-lg shadow-cyan-500/10'
                    : 'bg-slate-900/40 border-slate-800/80 hover:bg-slate-900/80 hover:border-slate-700'
                }`}
              >
                {/* Active Indicator Bar on side */}
                <div
                  className={`absolute left-0 top-0 bottom-0 w-1 ${
                    isMoving ? 'bg-emerald-400' : 'bg-slate-600'
                  }`}
                />

                <div className="flex items-start justify-between">
                  <div className="flex items-center space-x-2.5">
                    <div
                      className={`w-9 h-9 rounded-lg flex items-center justify-center ${
                        isMoving
                          ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
                          : 'bg-slate-800 text-slate-400'
                      }`}
                    >
                      <Car className="w-5 h-5" />
                    </div>
                    <div>
                      <h2 className="font-semibold text-sm text-slate-100 group-hover:text-cyan-400 transition flex items-center gap-1.5">
                        {driver.name}
                      </h2>
                      <div className="flex items-center space-x-2 text-[11px] text-slate-400 font-mono">
                        <span className="text-cyan-400">{driver.code}</span>
                        <span>•</span>
                        <span className="font-semibold text-slate-300">{driver.vehicle}</span>
                      </div>
                    </div>
                  </div>

                  {/* Status Badge */}
                  <span
                    className={`px-2 py-0.5 rounded text-[10px] font-mono font-semibold uppercase tracking-wider ${
                      isMoving
                        ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/30'
                        : 'bg-slate-800 text-slate-400 border border-slate-700'
                    }`}
                  >
                    {isMoving ? 'Moving' : 'Idle'}
                  </span>
                </div>

                {/* Telemetry Snapshot */}
                {telem ? (
                  <div className="mt-3 pt-2.5 border-t border-slate-800/60 grid grid-cols-2 gap-2 text-xs font-mono">
                    <div className="flex items-center space-x-1.5 text-slate-300">
                      <TrendingUp className="w-3.5 h-3.5 text-cyan-400 shrink-0" />
                      <span className="text-slate-400">Speed:</span>
                      <span className="font-bold text-emerald-400">{telem.speed.toFixed(1)} km/h</span>
                    </div>

                    <div className="flex items-center space-x-1.5 text-slate-300 justify-end">
                      <Compass className="w-3.5 h-3.5 text-amber-400 shrink-0" />
                      <span className="text-slate-400">Arah:</span>
                      <span className="text-slate-200">{getHeadingDirection(telem.heading)}</span>
                    </div>

                    <div className="col-span-2 flex items-center space-x-1.5 text-[11px] text-slate-500">
                      <MapPin className="w-3.5 h-3.5 text-slate-500 shrink-0" />
                      <span className="truncate">
                        {telem.latitude.toFixed(4)}, {telem.longitude.toFixed(4)}
                      </span>
                    </div>
                  </div>
                ) : (
                  <div className="mt-2 text-[11px] text-slate-500 font-mono italic">
                    Menunggu telemetri GPS...
                  </div>
                )}
              </div>
            );
          })
        )}
      </div>

      {/* Footer System Info */}
      <div className="p-3 border-t border-slate-800/80 bg-slate-950/80 text-[11px] font-mono text-slate-500 flex items-center justify-between">
        <span>Kota Medan, Sumatera Utara</span>
        <span className="text-cyan-500/80">3.5952° N, 98.6722° E</span>
      </div>
    </aside>
  );
};
