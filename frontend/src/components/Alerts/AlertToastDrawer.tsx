'use client';

import React, { useState } from 'react';
import { ShieldAlert, ChevronDown, ChevronUp, MapPin } from 'lucide-react';
import { GeofenceAlert } from '@/types';

interface AlertToastDrawerProps {
  alerts: GeofenceAlert[];
  onFocusAlert: (alert: GeofenceAlert) => void;
  onClearAlerts: () => void;
}

export const AlertToastDrawer: React.FC<AlertToastDrawerProps> = ({
  alerts,
  onFocusAlert,
  onClearAlerts,
}) => {
  const [isOpen, setIsOpen] = useState(true);

  if (alerts.length === 0) return null;

  return (
    <div className="fixed bottom-4 right-4 z-30 max-w-sm w-full font-mono">
      {/* Alert Header Bar */}
      <div 
        onClick={() => setIsOpen(!isOpen)}
        className="bg-slate-900/90 border border-rose-500/40 backdrop-blur-md rounded-t-xl px-4 py-2.5 flex items-center justify-between cursor-pointer shadow-lg shadow-rose-950/40"
      >
        <div className="flex items-center space-x-2">
          <div className="w-2.5 h-2.5 rounded-full bg-rose-500 animate-ping" />
          <span className="text-xs font-bold text-rose-300 uppercase tracking-wider flex items-center gap-1.5">
            <ShieldAlert className="w-4 h-4 text-rose-400" />
            Security Feed ({alerts.length})
          </span>
        </div>
        <div className="flex items-center space-x-2 text-slate-400">
          <button
            onClick={(e) => {
              e.stopPropagation();
              onClearAlerts();
            }}
            className="text-[10px] text-slate-400 hover:text-slate-200 px-1.5 py-0.5 rounded bg-slate-800"
            title="Clear list"
          >
            Clear
          </button>
          {isOpen ? <ChevronDown className="w-4 h-4" /> : <ChevronUp className="w-4 h-4" />}
        </div>
      </div>

      {/* Alert List Accordion Content */}
      {isOpen && (
        <div className="bg-slate-950/90 border-x border-b border-rose-500/30 backdrop-blur-xl rounded-b-xl max-h-72 overflow-y-auto p-2.5 space-y-2">
          {alerts.map((alert) => {
            const isEnter = alert.event_type === 'ENTER';
            return (
              <div
                key={alert.id || `${alert.driver_code}-${alert.timestamp}`}
                onClick={() => onFocusAlert(alert)}
                className={`p-3 rounded-lg border text-xs cursor-pointer transition-all hover:scale-[1.01] ${
                  isEnter
                    ? 'bg-amber-950/30 border-amber-500/40 text-amber-200 hover:bg-amber-950/50'
                    : 'bg-rose-950/30 border-rose-500/40 text-rose-200 hover:bg-rose-950/50'
                }`}
              >
                <div className="flex items-center justify-between mb-1">
                  <span
                    className={`px-1.5 py-0.5 rounded text-[10px] font-bold tracking-wider ${
                      isEnter
                        ? 'bg-amber-500/20 text-amber-400 border border-amber-500/40'
                        : 'bg-rose-500/20 text-rose-400 border border-rose-500/40'
                    }`}
                  >
                    {isEnter ? '⚠️ MASUK WILAYAH' : '🚨 KELUAR WILAYAH'}
                  </span>
                  <span className="text-[10px] text-slate-400">
                    {new Date(alert.timestamp).toLocaleTimeString('id-ID', {
                      timeZone: 'Asia/Jakarta',
                    })}
                  </span>
                </div>

                <div className="text-slate-100 font-semibold mt-1">
                  {alert.driver_name || alert.driver_code}
                </div>

                <div className="text-[11px] text-slate-300 mt-0.5">
                  Zona: <span className="font-semibold underline decoration-rose-500">{alert.geofence_name || alert.geofence_id}</span>
                </div>

                <div className="flex items-center justify-between text-[10px] text-slate-400 mt-2 pt-1.5 border-t border-slate-800">
                  <span>Kecepatan: {alert.speed?.toFixed(1) || 0} km/h</span>
                  <span className="text-cyan-400 flex items-center gap-0.5">
                    <MapPin className="w-3 h-3" /> Focus on map
                  </span>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
};
