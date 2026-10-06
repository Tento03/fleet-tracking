export interface Driver {
  id: string;
  code: string;
  name: string;
  phone: string;
  vehicle: string;
  status: 'active' | 'inactive' | 'idle' | string;
  created_at?: string;
  updated_at?: string;
  last_location?: {
    driver_id: string;
    driver_code: string;
    latitude: number;
    longitude: number;
    speed: number;
    heading: number;
    updated_at: string;
  };
}

export interface LocationTelemetry {
  event_id: string;
  driver_id: string;
  driver_code: string;
  driver_name?: string;
  vehicle?: string;
  latitude: number;
  longitude: number;
  speed: number; // km/h
  heading: number; // 0-360 degrees
  timestamp: string;
}

export interface Geofence {
  id: string;
  name: string;
  type: 'circle' | 'polygon';
  center_lat?: number;
  center_lng?: number;
  radius_m?: number;
  polygon?: {
    type: string;
    coordinates: number[][][]; // [lng, lat]
  } | null;
  active: boolean;
  created_at: string;
}

export interface GeofenceAlert {
  id: string;
  geofence_id: string;
  geofence_name?: string;
  driver_id: string;
  driver_code: string;
  driver_name?: string;
  event_type: 'ENTER' | 'EXIT';
  latitude: number;
  longitude: number;
  speed: number;
  timestamp: string;
}

export interface DashboardStats {
  total_drivers: number;
  active_drivers: number;
  total_events: number;
  timestamp?: string;
}

export interface WebSocketMessage {
  type: 'location_update' | 'geofence_alert' | 'driver_status_change' | 'ping';
  data: Record<string, unknown>;
}
