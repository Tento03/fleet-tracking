import { Driver, Geofence, GeofenceAlert, DashboardStats } from '@/types';

const API_BASE = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8080';

export async function fetchActiveDrivers(): Promise<Driver[]> {
  try {
    const res = await fetch(`${API_BASE}/drivers/active`, { cache: 'no-store' });
    if (!res.ok) return [];
    const json = await res.json();
    return json.data || json || [];
  } catch (err) {
    console.error('Failed to fetch active drivers:', err);
    return [];
  }
}

export async function fetchAllDrivers(): Promise<Driver[]> {
  try {
    const res = await fetch(`${API_BASE}/drivers`, { cache: 'no-store' });
    if (!res.ok) return [];
    const json = await res.json();
    return json.data || json || [];
  } catch (err) {
    console.error('Failed to fetch drivers:', err);
    return [];
  }
}

export async function fetchGeofences(): Promise<Geofence[]> {
  try {
    const res = await fetch(`${API_BASE}/geofences`, { cache: 'no-store' });
    if (!res.ok) return [];
    const json = await res.json();
    return json.data || json || [];
  } catch (err) {
    console.error('Failed to fetch geofences:', err);
    return [];
  }
}

export async function fetchRecentAlerts(limit: number = 20): Promise<GeofenceAlert[]> {
  try {
    const res = await fetch(`${API_BASE}/geofences/alerts?limit=${limit}`, { cache: 'no-store' });
    if (!res.ok) return [];
    const json = await res.json();
    return json.data || json || [];
  } catch (err) {
    console.error('Failed to fetch alerts:', err);
    return [];
  }
}

export async function fetchDashboardStats(): Promise<DashboardStats | null> {
  try {
    const res = await fetch(`${API_BASE}/dashboard/stats`, { cache: 'no-store' });
    if (!res.ok) return null;
    const json = await res.json();
    return json.data || json;
  } catch (err) {
    console.error('Failed to fetch dashboard stats:', err);
    return null;
  }
}

export async function fetchDriverHistory(driverId: string, limit: number = 50) {
  try {
    const res = await fetch(`${API_BASE}/drivers/${driverId}/history?limit=${limit}`, { cache: 'no-store' });
    if (!res.ok) return [];
    const json = await res.json();
    return json.data || json || [];
  } catch (err) {
    console.error(`Failed to fetch history for driver ${driverId}:`, err);
    return [];
  }
}
