'use client';

import React, { useEffect, useState } from 'react';
import { 
  Radio, 
  Activity, 
  Gauge, 
  Clock, 
  ShieldAlert, 
  Layers, 
  Maximize2,
  Crosshair
} from 'lucide-react';

interface NavbarProps {
  wsConnected: boolean;
  activeCount: number;
  avgSpeed: number;
  alertCount: number;
  showGeofences: boolean;
  mapStyle: 'street' | 'dark' | 'satellite';
  onSelectMapStyle: (style: 'street' | 'dark' | 'satellite') => void;
  onToggleGeofences: () => void;
  onCenterMap: () => void;
  onFitFleet: () => void;
}

export const Navbar: React.FC<NavbarProps> = ({
  wsConnected,
  activeCount,
  avgSpeed,
  alertCount,
  showGeofences,
  mapStyle,
  onSelectMapStyle,
  onToggleGeofences,
  onCenterMap,
  onFitFleet,
}) => {
  const [currentTime, setCurrentTime] = useState<string>('');

  useEffect(() => {
    const updateClock = () => {
      const now = new Date();
      setCurrentTime(
        now.toLocaleTimeString('id-ID', {
          timeZone: 'Asia/Jakarta',
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit',
        }) + ' WIB'
      );
    };
    updateClock();
    const interval = setInterval(updateClock, 1000);
    return () => clearInterval(interval);
  }, []);

  const toggleFullscreen = () => {
    if (!document.fullscreenElement) {
      document.documentElement.requestFullscreen().catch(() => {});
    } else {
      document.exitFullscreen().catch(() => {});
    }
  };

  return (
    <header className="h-16 border-b border-slate-800/80 bg-slate-950/80 backdrop-blur-md px-4 flex items-center justify-between z-20 shrink-0">
      {/* Brand / Logo */}
      <div className="flex items-center space-x-3 cursor-pointer" onClick={onCenterMap}>
        <div className="relative flex items-center justify-center w-10 h-10 rounded-xl bg-gradient-to-tr from-cyan-600 to-blue-500 shadow-lg shadow-cyan-500/20 text-white">
          <Radio className="w-5 h-5 animate-pulse" />
          <span className="absolute -top-1 -right-1 w-3 h-3 bg-emerald-400 rounded-full border-2 border-slate-950" />
        </div>
        <div>
          <div className="flex items-center space-x-2">
            <h1 className="font-extrabold text-base tracking-wider text-slate-100 flex items-center gap-1.5">
              FLEET RADAR <span className="text-xs font-semibold px-1.5 py-0.5 rounded bg-cyan-500/10 text-cyan-400 border border-cyan-500/30">MEDAN</span>
            </h1>
          </div>
          <p className="text-[11px] text-slate-400 font-mono tracking-tight">Realtime Distributed Telemetry</p>
        </div>
      </div>

      {/* Metrics HUD */}
      <div className="hidden md:flex items-center space-x-4 font-mono text-xs">
        {/* WebSocket Live Status */}
        <div className="flex items-center space-x-2 px-3 py-1.5 rounded-lg bg-slate-900/90 border border-slate-800">
          <span
            className={`w-2 h-2 rounded-full ${
              wsConnected ? 'bg-emerald-400 shadow-[0_0_8px_#34d399] animate-pulse' : 'bg-rose-500'
            }`}
          />
          <span className={wsConnected ? 'text-emerald-400 font-semibold' : 'text-rose-400'}>
            {wsConnected ? 'STREAM ACTIVE' : 'DISCONNECTED'}
          </span>
        </div>

        {/* Active Fleet */}
        <div className="flex items-center space-x-2 px-3 py-1.5 rounded-lg bg-slate-900/90 border border-slate-800 text-slate-300">
          <Activity className="w-4 h-4 text-cyan-400" />
          <span>ARMADA:</span>
          <span className="text-cyan-400 font-bold">{activeCount} UNITS</span>
        </div>

        {/* Avg Speed */}
        <div className="flex items-center space-x-2 px-3 py-1.5 rounded-lg bg-slate-900/90 border border-slate-800 text-slate-300">
          <Gauge className="w-4 h-4 text-emerald-400" />
          <span>AVG SPEED:</span>
          <span className="text-emerald-400 font-bold">{avgSpeed.toFixed(1)} km/h</span>
        </div>

        {/* Alerts Pill */}
        {alertCount > 0 && (
          <div className="flex items-center space-x-2 px-3 py-1.5 rounded-lg bg-rose-950/60 border border-rose-800/80 text-rose-300 animate-pulse">
            <ShieldAlert className="w-4 h-4 text-rose-400" />
            <span className="font-bold">{alertCount} ALERTS</span>
          </div>
        )}
      </div>

      {/* Right Controls */}
      <div className="flex items-center space-x-2.5">
        {/* Tipe Peta Buttons */}
        <div className="flex items-center bg-slate-900 border border-slate-800 p-0.5 rounded-lg text-xs font-mono">
          {(['street', 'satellite', 'dark'] as const).map((style) => {
            const labels = { street: 'Terang', satellite: 'Satelit', dark: 'Dark' };
            const isActive = mapStyle === style;
            return (
              <button
                key={style}
                onClick={() => onSelectMapStyle(style)}
                className={`px-2.5 py-1 rounded text-[11px] font-semibold transition ${
                  isActive
                    ? 'bg-cyan-500 text-slate-950 shadow-sm font-bold'
                    : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                {labels[style]}
              </button>
            );
          })}
        </div>

        {/* Fokus Armada Button */}
        <button
          onClick={onFitFleet}
          className="flex items-center space-x-1.5 text-xs px-3 py-1.5 rounded-lg bg-slate-900 hover:bg-slate-800 border border-slate-800 text-cyan-400 hover:text-cyan-300 transition"
          title="Fokuskan Semua Kendaraan di Peta"
        >
          <Crosshair className="w-3.5 h-3.5" />
          <span className="hidden sm:inline font-mono">Fokus Armada</span>
        </button>

        {/* Toggle Geofence Layers */}
        <button
          onClick={onToggleGeofences}
          className={`flex items-center space-x-1.5 text-xs px-3 py-1.5 rounded-lg border transition-all ${
            showGeofences
              ? 'bg-cyan-950/60 border-cyan-500/50 text-cyan-300 shadow-sm shadow-cyan-500/20'
              : 'bg-slate-900 border-slate-800 text-slate-400 hover:text-slate-200'
          }`}
          title="Toggle Geofence Boundaries"
        >
          <Layers className="w-3.5 h-3.5" />
          <span className="hidden md:inline">Geofences</span>
        </button>

        {/* Clock */}
        <div className="hidden xl:flex items-center space-x-1.5 text-xs font-mono text-slate-400 bg-slate-900/60 px-2.5 py-1.5 rounded-lg border border-slate-800/60">
          <Clock className="w-3.5 h-3.5 text-slate-400" />
          <span>{currentTime || '--:--:-- WIB'}</span>
        </div>

        {/* Fullscreen Button */}
        <button
          onClick={toggleFullscreen}
          className="p-1.5 rounded-lg bg-slate-900 hover:bg-slate-800 border border-slate-800 text-slate-400 hover:text-slate-200 transition"
          title="Toggle Fullscreen"
        >
          <Maximize2 className="w-4 h-4" />
        </button>
      </div>
    </header>
  );
};
