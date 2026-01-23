"use client";

import { useEffect, useMemo, useState, useRef } from "react";
import { MapContainer, TileLayer, CircleMarker, Polyline, Popup, useMap } from "react-leaflet";

function FixLeafletResize({ trigger }) {
  const map = useMap();

  useEffect(() => {
    if (!map) return;
    
    // run a couple times to handle Next layout + fonts settling
    const t1 = setTimeout(() => {
      try {
        map.invalidateSize();
      } catch (e) {
        // Map might be destroyed
      }
    }, 50);
    const t2 = setTimeout(() => {
      try {
        map.invalidateSize();
      } catch (e) {
        // Map might be destroyed
      }
    }, 250);
    const t3 = setTimeout(() => {
      try {
        map.invalidateSize();
      } catch (e) {
        // Map might be destroyed
      }
    }, 600);

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

  useEffect(() => {
    setMounted(true);
  }, []);

  // Create a unique key when data changes to force remount
  // Use a hash of the data to ensure uniqueness
  const mapKey = useMemo(() => {
    if (!airports || !routes) return "initial";
    const airportsStr = airports.map(a => `${a.icao}-${a.lat}-${a.lon}`).join(",");
    const routesStr = routes.map(r => `${r.dep}-${r.arr}`).join(",");
    // Simple hash function
    let hash = 0;
    const str = airportsStr + routesStr;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash; // Convert to 32bit integer
    }
    return `map-${Math.abs(hash)}`;
  }, [airports, routes]);

  if (!mounted) {
    return (
      <div style={{ height: 600, width: "100%", border: "1px solid #ddd", borderRadius: 8, display: "flex", alignItems: "center", justifyContent: "center" }}>
        <p>Loading map...</p>
      </div>
    );
  }

  if (!airports || !routes || airports.length === 0) {
    return (
      <div style={{ height: 600, width: "100%", border: "1px solid #ddd", borderRadius: 8, display: "flex", alignItems: "center", justifyContent: "center" }}>
        <p>No map data available</p>
      </div>
    );
  }

  function bubbleRadius(co2_kg) {
    const tons = co2_kg / 1000;
    return Math.max(3, Math.min(25, Math.sqrt(tons)));
  }

  return (
    <div style={{ height: 600, width: "100%", border: "1px solid #ddd", borderRadius: 8, overflow: "hidden" }}>
      <MapContainer
        key={mapKey}
        center={[20, 0]}
        zoom={2}
        scrollWheelZoom={true}
        style={{ height: "100%", width: "100%" }}
      >
        <FixLeafletResize trigger={mapKey} />

        <TileLayer
          attribution='&copy; OpenStreetMap contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />

        {routes?.map((r, i) => (
          <Polyline
            key={`route-${i}-${r.dep}-${r.arr}`}
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
            key={`airport-${i}-${a.icao}`}
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
