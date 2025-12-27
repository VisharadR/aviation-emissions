"use client";

import { useEffect, useMemo, useState } from "react";
import { MapContainer, TileLayer, CircleMarker, Polyline, Popup, useMap } from "react-leaflet";

function FixLeafletResize({ trigger }) {
  const map = useMap();

  useEffect(() => {
    // run a couple times to handle Next layout + fonts settling
    const t1 = setTimeout(() => map.invalidateSize(), 50);
    const t2 = setTimeout(() => map.invalidateSize(), 250);
    const t3 = setTimeout(() => map.invalidateSize(), 600);

    return () => {
      clearTimeout(t1);
      clearTimeout(t2);
      clearTimeout(t3);
    };
  }, [map, trigger]);

  return null;
}

export default function EmissionsMap({ airports, routes }) {
  const [mounted, setMounted] = useState(false);

  useEffect(() => setMounted(true), []);

  // Force a re-trigger when data changes
  const trigger = useMemo(() => `${airports?.length || 0}-${routes?.length || 0}`, [airports, routes]);

  if (!mounted) return null;

  function bubbleRadius(co2_kg) {
    const tons = co2_kg / 1000;
    return Math.max(3, Math.min(25, Math.sqrt(tons)));
  }

  return (
    <div style={{ height: 600, width: "100%", border: "1px solid #ddd", borderRadius: 8, overflow: "hidden" }}>
      <MapContainer
        center={[20, 0]}
        zoom={2}
        scrollWheelZoom={true}
        style={{ height: "100%", width: "100%" }}   // IMPORTANT: explicit style
      >
        <FixLeafletResize trigger={trigger} />

        <TileLayer
          attribution='&copy; OpenStreetMap contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />

        {routes?.map((r, i) => (
          <Polyline
            key={`route-${i}`}
            positions={[
              [r.dep_lat, r.dep_lon],
              [r.arr_lat, r.arr_lon],
            ]}
            pathOptions={{ weight: 2, opacity: 0.35 }}
          >
            <Popup>
              <div style={{ fontSize: 12 }}>
                <div><b>{r.dep}</b> → <b>{r.arr}</b></div>
                <div>CO₂: {(r.co2_kg / 1000).toFixed(1)} tons</div>
              </div>
            </Popup>
          </Polyline>
        ))}

        {airports?.map((a, i) => (
          <CircleMarker
            key={`airport-${i}`}
            center={[a.lat, a.lon]}
            radius={bubbleRadius(a.co2_kg)}
            pathOptions={{ opacity: 0.7, fillOpacity: 0.5 }}
          >
            <Popup>
              <div style={{ fontSize: 12 }}>
                <div><b>{a.icao}</b></div>
                <div>{a.airport_name}</div>
                <div>{a.country}</div>
                <div>CO₂: {(a.co2_kg / 1000).toFixed(1)} tons</div>
              </div>
            </Popup>
          </CircleMarker>
        ))}
      </MapContainer>
    </div>
  );
}
