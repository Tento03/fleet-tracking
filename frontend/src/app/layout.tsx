import type { Metadata } from "next";
import localFont from "next/font/local";
import "./globals.css";

const geistSans = localFont({
  src: "./fonts/GeistVF.woff",
  variable: "--font-geist-sans",
  weight: "100 900",
});
const geistMono = localFont({
  src: "./fonts/GeistMonoVF.woff",
  variable: "--font-geist-mono",
  weight: "100 900",
});

export const metadata: Metadata = {
  title: "Fleet Command Center | Realtime Distributed Vehicle Tracking",
  description: "Enterprise-grade real-time fleet telemetry radar and geofence monitoring system for Medan City.",
  keywords: ["fleet tracking", "realtime gps", "geofencing", "golang", "kafka", "distributed systems"],
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="id" className="dark">
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased bg-slate-950 text-slate-100`}
      >
        {children}
      </body>
    </html>
  );
}
