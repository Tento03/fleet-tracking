'use client';

import React, { useEffect, useState, useRef, useCallback } from 'react';
import dynamic from 'next/dynamic';
import { Navbar } from '@/components/Header/Navbar';
import { FleetSidebar } from '@/components/Sidebar/FleetSidebar';
import { AlertToastDrawer } from '@/components/Alerts/AlertToastDrawer';
import { 
  Driver, 
  LocationTelemetry, 
  Geofence, 
  GeofenceAlert, 
  WebSocketMessage 
} from '@/types';
import { 
  fetchGeofences, 
  fetchRecentAlerts, 
  fetchAllDrivers 
} from '@/lib/api';

// Dynamic import Leaflet component with SSR disabled
const FleetMap = dynamic(
  () => import('@/components/Map/FleetMap').then((mod) => mod.FleetMap),
  { 
    ssr: false,
    loading: () => (
      <div className="w-full h-[calc(100vh-4rem)] bg-slate-950 flex flex-col items-center justify-center font-mono text-cyan-400">
        <div className="w-12 h-12 border-2 border-cyan-500 border-t-transparent rounded-full animate-spin mb-4" />
        <p className="text-sm tracking-widest uppercase">Memuat Radar Satelit Medan...</p>
      </div>
    )
  }
);

export default function DashboardPage() {
  const [drivers, setDrivers] = useState<Driver[]>([]);
  const [telemetryMap, setTelemetryMap] = useState<Record<string, LocationTelemetry>>({});
  const [geofences, setGeofences] = useState<Geofence[]>([]);
  const [alerts, setAlerts] = useState<GeofenceAlert[]>([]);
  const [selectedDriverId, setSelectedDriverId] = useState<string | null>(null);
  const [wsConnected, setWsConnected] = useState<boolean>(false);
  const [showGeofences, setShowGeofences] = useState<boolean>(true);
  const [centerTrigger, setCenterTrigger] = useState<number>(0);

  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  // Synthesize soft warning beep using Web Audio API
  const playAlertSound = useCallback(() => {
    try {
      const AudioCtxClass = window.AudioContext || (window as unknown as { webkitAudioContext: typeof AudioContext }).webkitAudioContext;
      if (!AudioCtxClass) return;
      const audioCtx = new AudioCtxClass();
      const osc = audioCtx.createOscillator();
      const gain = audioCtx.createGain();
      osc.type = 'sine';
      osc.frequency.setValueAtTime(587.33, audioCtx.currentTime); // D5
      osc.frequency.exponentialRampToValueAtTime(880, audioCtx.currentTime + 0.15); // A5
      gain.gain.setValueAtTime(0.2, audioCtx.currentTime);
      gain.gain.exponentialRampToValueAtTime(0.01, audioCtx.currentTime + 0.25);
      osc.connect(gain);
      gain.connect(audioCtx.destination);
      osc.start();
      osc.stop(audioCtx.currentTime + 0.25);
    } catch {
      // AudioContext maybe blocked before user gesture
    }
  }, []);

  // Initial Data Fetch
  useEffect(() => {
    async function initData() {
      const [driverList, geoList, alertList] = await Promise.all([
        fetchAllDrivers(),
        fetchGeofences(),
        fetchRecentAlerts(15),
      ]);

      if (driverList && driverList.length > 0) {
        setDrivers(driverList);
      }
      if (geoList && geoList.length > 0) {
        setGeofences(geoList);
      }
      if (alertList && alertList.length > 0) {
        setAlerts(alertList);
      }
    }
    initData();
  }, []);

  // WebSocket Connection
  useEffect(() => {
    const wsUrl = process.env.NEXT_PUBLIC_WS_URL || 'ws://localhost:8080/ws';

    function connect() {
      console.log('Connecting to WebSocket:', wsUrl);
      const ws = new WebSocket(wsUrl);
      wsRef.current = ws;

      ws.onopen = () => {
        console.log('WebSocket connected successfully');
        setWsConnected(true);
      };

      ws.onmessage = (event) => {
        try {
          const msg: WebSocketMessage = JSON.parse(event.data);

          if (msg.type === 'location_update') {
            const data = msg.data as unknown as LocationTelemetry;
            const telem: LocationTelemetry = {
              event_id: data.event_id || '',
              driver_id: data.driver_id || '',
              driver_code: data.driver_code || '',
              driver_name: data.driver_name,
              vehicle: data.vehicle,
              latitude: Number(data.latitude),
              longitude: Number(data.longitude),
              speed: Number(data.speed || 0),
              heading: Number(data.heading || 0),
              timestamp: data.timestamp || new Date().toISOString(),
            };

            setTelemetryMap((prev) => ({
              ...prev,
              [telem.driver_code]: telem,
            }));

            // Auto-discover drivers if not in list yet
            setDrivers((prev) => {
              const exists = prev.some((d) => d.code === telem.driver_code);
              if (!exists && telem.driver_code) {
                return [
                  ...prev,
                  {
                    id: telem.driver_id || telem.driver_code,
                    code: telem.driver_code,
                    name: telem.driver_name || telem.driver_code,
                    phone: '',
                    vehicle: telem.vehicle || 'Fleet Unit',
                    status: 'active',
                  },
                ];
              }
              return prev;
            });
          } else if (msg.type === 'geofence_alert') {
            const alertData = msg.data as unknown as GeofenceAlert;
            setAlerts((prev) => [alertData, ...prev.slice(0, 20)]);
            playAlertSound();
          }
        } catch (err) {
          console.error('Error parsing WebSocket message:', err);
        }
      };

      ws.onclose = () => {
        console.log('WebSocket closed, attempting reconnect in 3s...');
        setWsConnected(false);
        reconnectTimeoutRef.current = setTimeout(connect, 3000);
      };

      ws.onerror = (err) => {
        console.error('WebSocket error:', err);
        ws.close();
      };
    }

    connect();

    return () => {
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, [playAlertSound]);

  // Aggregate stats
  const activeCount = Object.keys(telemetryMap).length;
  const avgSpeed =
    activeCount > 0
      ? Object.values(telemetryMap).reduce((acc, curr) => acc + curr.speed, 0) / activeCount
      : 0;

  const handleSelectDriver = (driverId: string) => {
    setSelectedDriverId(driverId);
  };

  const handleFocusAlert = (alert: GeofenceAlert) => {
    setSelectedDriverId(alert.driver_code || alert.driver_id);
  };

  const handleCenterMap = () => {
    setCenterTrigger((prev) => prev + 1);
  };

  const handleClearAlerts = () => {
    setAlerts([]);
  };

  return (
    <div className="flex flex-col h-screen w-screen bg-slate-950 overflow-hidden select-none">
      {/* Top Navbar */}
      <Navbar
        wsConnected={wsConnected}
        activeCount={activeCount}
        avgSpeed={avgSpeed}
        alertCount={alerts.length}
        showGeofences={showGeofences}
        onToggleGeofences={() => setShowGeofences(!showGeofences)}
        onCenterMap={handleCenterMap}
      />

      {/* Main Body (Sidebar + Map) */}
      <main className="flex flex-1 overflow-hidden relative">
        {/* Left Sidebar */}
        <FleetSidebar
          drivers={drivers}
          telemetryMap={telemetryMap}
          selectedDriverId={selectedDriverId}
          onSelectDriver={handleSelectDriver}
        />

        {/* Center Live Map */}
        <div className="flex-1 relative h-full">
          <FleetMap
            drivers={drivers}
            telemetryMap={telemetryMap}
            geofences={geofences}
            selectedDriverId={selectedDriverId}
            showGeofences={showGeofences}
            onSelectDriver={handleSelectDriver}
            centerTrigger={centerTrigger}
          />
        </div>

        {/* Real-time Alert Toast Drawer */}
        <AlertToastDrawer
          alerts={alerts}
          onFocusAlert={handleFocusAlert}
          onClearAlerts={handleClearAlerts}
        />
      </main>
    </div>
  );
}
