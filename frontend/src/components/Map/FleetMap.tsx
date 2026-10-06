'use client';

import React, { useEffect, useRef, useState } from 'react';
import L from 'leaflet';
import { Driver, LocationTelemetry, Geofence } from '@/types';
import { Navigation, Map } from 'lucide-react';

interface FleetMapProps {
  drivers: Driver[];
  telemetryMap: Record<string, LocationTelemetry>;
  geofences: Geofence[];
  selectedDriverId: string | null;
  showGeofences: boolean;
  onSelectDriver: (driverId: string) => void;
  centerTrigger: number;
}

const MEDAN_COORDS: [number, number] = [-3.5952, 98.6722];

export const FleetMap: React.FC<FleetMapProps> = ({
  drivers,
  telemetryMap,
  geofences,
  selectedDriverId,
  showGeofences,
  onSelectDriver,
  centerTrigger,
}) => {
  const mapContainerRef = useRef<HTMLDivElement | null>(null);
  const mapInstanceRef = useRef<L.Map | null>(null);
  const tileLayerRef = useRef<L.TileLayer | null>(null);
  const markersRef = useRef<Record<string, L.Marker>>({});
  const trailsRef = useRef<Record<string, L.Polyline>>({});
  const trailPointsRef = useRef<Record<string, [number, number][]>>({});
  const geofenceLayersRef = useRef<L.LayerGroup | null>(null);

  const [mapStyle, setMapStyle] = useState<'street' | 'dark' | 'satellite'>('street');

  // Initialize Leaflet Map
  useEffect(() => {
    if (!mapContainerRef.current || mapInstanceRef.current) return;

    const map = L.map(mapContainerRef.current, {
      center: MEDAN_COORDS,
      zoom: 13,
      zoomControl: false,
    });

    // Default: Clean and Bright Street Map (OpenStreetMap)
    const tile = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
      maxZoom: 19,
      className: 'map-tiles-street',
    }).addTo(map);

    tileLayerRef.current = tile;

    // Zoom control at bottom right
    L.control.zoom({ position: 'bottomright' }).addTo(map);

    const geofenceGroup = L.layerGroup().addTo(map);
    geofenceLayersRef.current = geofenceGroup;
    mapInstanceRef.current = map;

    return () => {
      map.remove();
      mapInstanceRef.current = null;
    };
  }, []);

  // Update Tile Layer when style changes
  useEffect(() => {
    const map = mapInstanceRef.current;
    if (!map) return;

    if (tileLayerRef.current) {
      map.removeLayer(tileLayerRef.current);
    }

    let url = 'https://tile.openstreetmap.org/{z}/{x}/{y}.png';
    let className = 'map-tiles-street';

    if (mapStyle === 'dark') {
      url = 'https://tile.openstreetmap.org/{z}/{x}/{y}.png';
      className = 'map-tiles-dark';
    } else if (mapStyle === 'satellite') {
      url = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}';
      className = '';
    }

    const newTile = L.tileLayer(url, {
      attribution: '&copy; OpenStreetMap / ESRI',
      maxZoom: 19,
      className: className,
    }).addTo(map);

    tileLayerRef.current = newTile;
  }, [mapStyle]);

  // Handle Center on Medan trigger
  useEffect(() => {
    if (mapInstanceRef.current && centerTrigger > 0) {
      mapInstanceRef.current.flyTo(MEDAN_COORDS, 13, { duration: 1.2 });
    }
  }, [centerTrigger]);

  // Handle Fly-To on Driver Selection
  useEffect(() => {
    if (!selectedDriverId || !mapInstanceRef.current) return;

    // Find driver telem
    const telem = telemetryMap[selectedDriverId];
    if (telem) {
      mapInstanceRef.current.flyTo([telem.latitude, telem.longitude], 16, { duration: 1.0 });
      // Open popup
      const marker = markersRef.current[telem.driver_code] || markersRef.current[selectedDriverId];
      if (marker) {
        marker.openPopup();
      }
    }
  }, [selectedDriverId, telemetryMap]);

  // Render / Update Geofences
  useEffect(() => {
    const group = geofenceLayersRef.current;
    if (!group) return;

    group.clearLayers();
    if (!showGeofences) return;

    geofences.forEach((geo) => {
      if (geo.type === 'circle' && geo.center_lat && geo.center_lng && geo.radius_m) {
        const circle = L.circle([geo.center_lat, geo.center_lng], {
          radius: geo.radius_m,
          color: '#38bdf8',
          fillColor: '#0284c7',
          fillOpacity: 0.15,
          weight: 2,
          dashArray: '6, 6',
        });

        circle.bindTooltip(`<b>🛡️ ${geo.name}</b><br/>Radius: ${geo.radius_m}m`, {
          permanent: false,
          direction: 'top',
          className: 'bg-slate-900 border border-cyan-500/40 text-slate-100 rounded px-2 py-1 text-xs font-mono',
        });

        group.addLayer(circle);
      } else if (geo.type === 'polygon' && geo.polygon && geo.polygon.coordinates) {
        // GeoJSON uses [lng, lat], Leaflet expects [lat, lng]
        const coords = geo.polygon.coordinates[0];
        if (coords && coords.length > 0) {
          const latlngs: [number, number][] = coords.map((c) => [c[1], c[0]]);
          const poly = L.polygon(latlngs, {
            color: '#f43f5e',
            fillColor: '#e11d48',
            fillOpacity: 0.18,
            weight: 2,
          });

          poly.bindTooltip(`<b>⚠️ ${geo.name}</b><br/>Polygon Zone`, {
            permanent: false,
            direction: 'center',
            className: 'bg-slate-900 border border-rose-500/40 text-slate-100 rounded px-2 py-1 text-xs font-mono',
          });

          group.addLayer(poly);
        }
      }
    });
  }, [geofences, showGeofences]);

  // Update Markers & Trails when telemetry changes
  useEffect(() => {
    const map = mapInstanceRef.current;
    if (!map) return;

    Object.values(telemetryMap).forEach((telem) => {
      const driver = drivers.find(
        (d) => d.code === telem.driver_code || d.id === telem.driver_id
      );

      const driverCode = telem.driver_code || (driver ? driver.code : 'driver');
      const driverName = driver ? driver.name : telem.driver_name || driverCode;
      const vehicle = driver ? driver.vehicle : telem.vehicle || 'Fleet';
      const pos: [number, number] = [telem.latitude, telem.longitude];

      // Update Breadcrumb Trails
      if (!trailPointsRef.current[driverCode]) {
        trailPointsRef.current[driverCode] = [];
      }
      const points = trailPointsRef.current[driverCode];
      points.push(pos);
      if (points.length > 25) points.shift(); // Keep last 25 positions

      if (!trailsRef.current[driverCode]) {
        trailsRef.current[driverCode] = L.polyline(points, {
          color: '#38bdf8',
          weight: 3,
          opacity: 0.6,
          lineJoin: 'round',
        }).addTo(map);
      } else {
        trailsRef.current[driverCode].setLatLngs(points);
      }

      // Marker Icon HTML with rotation
      const heading = telem.heading || 0;
      const speed = telem.speed || 0;
      const isMoving = speed > 2;

      const iconHtml = `
        <div class="relative flex flex-col items-center cursor-pointer group" style="width: 50px; height: 50px;">
          <!-- Driver Code / Speed Tag -->
          <div class="absolute -top-6 whitespace-nowrap bg-slate-900/90 border border-slate-700/80 px-2 py-0.5 rounded text-[10px] font-mono font-bold text-slate-200 shadow-md flex items-center gap-1">
            <span class="text-cyan-400">${driverCode}</span>
            <span class="text-emerald-400">${speed.toFixed(0)} km/h</span>
          </div>

          <!-- Pulsing Ring -->
          <div class="absolute inset-0 rounded-full ${
            isMoving ? 'pulse-ring' : ''
          } bg-cyan-500/20"></div>

          <!-- Vehicle / Arrow Navigation Icon -->
          <div class="w-10 h-10 rounded-full bg-slate-900/95 border-2 ${
            isMoving ? 'border-cyan-400 shadow-[0_0_12px_#38bdf8]' : 'border-slate-600'
          } flex items-center justify-center text-cyan-400 transition-transform duration-300"
               style="transform: rotate(${heading}deg);">
            <svg class="w-5 h-5 fill-current text-cyan-400 drop-shadow" viewBox="0 0 24 24">
              <path d="M12 2L4.5 20.29l.71.71L12 18l6.79 3 .71-.71z" />
            </svg>
          </div>
        </div>
      `;

      const customIcon = L.divIcon({
        html: iconHtml,
        className: 'custom-vehicle-marker',
        iconSize: [50, 50],
        iconAnchor: [25, 25],
        popupAnchor: [0, -25],
      });

      // Popup Content (High-Tech Card)
      const popupHtml = `
        <div class="p-3.5 font-mono text-xs text-slate-200 min-w-[210px]">
          <div class="flex items-center justify-between border-b border-slate-800 pb-2 mb-2">
            <div>
              <div class="font-bold text-sm text-cyan-400">${driverName}</div>
              <div class="text-[10px] text-slate-400">${driverCode} • ${vehicle}</div>
            </div>
            <span class="px-1.5 py-0.5 rounded text-[9px] font-bold ${
              isMoving
                ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/40'
                : 'bg-slate-800 text-slate-400'
            }">
              ${isMoving ? 'MOVING' : 'IDLE'}
            </span>
          </div>

          <div class="space-y-1.5 text-slate-300">
            <div class="flex justify-between">
              <span class="text-slate-400">Kecepatan:</span>
              <span class="font-bold text-emerald-400">${speed.toFixed(1)} km/h</span>
            </div>
            <div class="flex justify-between">
              <span class="text-slate-400">Arah Hadap:</span>
              <span class="text-slate-200">${heading.toFixed(0)}°</span>
            </div>
            <div class="flex justify-between">
              <span class="text-slate-400">Latitude:</span>
              <span class="text-slate-300">${telem.latitude.toFixed(5)}</span>
            </div>
            <div class="flex justify-between">
              <span class="text-slate-400">Longitude:</span>
              <span class="text-slate-300">${telem.longitude.toFixed(5)}</span>
            </div>
          </div>

          ${
            driver?.phone
              ? `<div class="mt-2.5 pt-2 border-t border-slate-800/80 text-[10px] text-slate-400 flex items-center gap-1">
                   <span>📞 HP:</span> <span class="text-slate-200">${driver.phone}</span>
                 </div>`
              : ''
          }
        </div>
      `;

      if (!markersRef.current[driverCode]) {
        const marker = L.marker(pos, { icon: customIcon }).addTo(map);
        marker.bindPopup(popupHtml);
        marker.on('click', () => {
          onSelectDriver(driver?.id || driverCode);
        });
        markersRef.current[driverCode] = marker;
      } else {
        const marker = markersRef.current[driverCode];
        marker.setLatLng(pos);
        marker.setIcon(customIcon);
        marker.setPopupContent(popupHtml);
      }
    });
  }, [telemetryMap, drivers, onSelectDriver]);

  return (
    <div className="relative w-full h-[calc(100vh-4rem)] overflow-hidden bg-slate-950">
      <div ref={mapContainerRef} className="w-full h-full" id="fleet-map" />

      {/* Floating Map Controls & Legend */}
      <div className="absolute top-4 right-4 z-10 bg-slate-950/85 border border-slate-800/80 backdrop-blur-md rounded-xl p-3 text-xs font-mono shadow-xl hidden sm:block max-w-[220px]">
        {/* Style Switcher */}
        <div className="flex items-center space-x-1.5 text-slate-300 font-semibold mb-2">
          <Map className="w-4 h-4 text-cyan-400" />
          <span>TIPE PETA</span>
        </div>
        <div className="grid grid-cols-3 gap-1 bg-slate-900/90 border border-slate-800 p-1 rounded-lg mb-3">
          <button
            onClick={() => setMapStyle('street')}
            className={`py-1 text-[10px] font-bold rounded transition ${
              mapStyle === 'street'
                ? 'bg-cyan-500 text-slate-950 shadow-sm'
                : 'text-slate-400 hover:text-slate-200'
            }`}
          >
            Terang
          </button>
          <button
            onClick={() => setMapStyle('satellite')}
            className={`py-1 text-[10px] font-bold rounded transition ${
              mapStyle === 'satellite'
                ? 'bg-cyan-500 text-slate-950 shadow-sm'
                : 'text-slate-400 hover:text-slate-200'
            }`}
          >
            Satelit
          </button>
          <button
            onClick={() => setMapStyle('dark')}
            className={`py-1 text-[10px] font-bold rounded transition ${
              mapStyle === 'dark'
                ? 'bg-cyan-500 text-slate-950 shadow-sm'
                : 'text-slate-400 hover:text-slate-200'
            }`}
          >
            Dark
          </button>
        </div>

        <div className="flex items-center space-x-2 text-slate-300 font-semibold mb-2 pt-2 border-t border-slate-800/80">
          <Navigation className="w-4 h-4 text-cyan-400" />
          <span>LEGENDA</span>
        </div>
        <div className="space-y-1.5 text-[11px] text-slate-400">
          <div className="flex items-center space-x-2">
            <span className="w-2.5 h-2.5 rounded-full bg-cyan-400 shadow-[0_0_6px_#38bdf8]" />
            <span>Armada Aktif Bergerak</span>
          </div>
          <div className="flex items-center space-x-2">
            <span className="w-2.5 h-2.5 rounded border border-cyan-400/80 bg-cyan-500/20" />
            <span>Zona Lingkaran Geofence</span>
          </div>
          <div className="flex items-center space-x-2">
            <span className="w-2.5 h-2.5 rounded border border-rose-500/80 bg-rose-500/20" />
            <span>Zona Poligon Geofence</span>
          </div>
        </div>
      </div>
    </div>
  );
};
