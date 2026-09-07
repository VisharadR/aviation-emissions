"use client";
import { useEffect, useRef } from "react";
import L from "leaflet";

// Provider strings stay text, rather than passing through Leaflet's HTML API.
function popup(lines) {
  const content = document.createElement("div");
  lines.forEach((line, index) => {
    const row = document.createElement(index === 0 ? "strong" : "div");
    row.textContent = line;
    content.appendChild(row);
  });
  return content;
}

export default function LiveMap({ airports, flights, airport, selected, onSelect, scope = "world" }) {
  const world = scope === "world";
  const center = world ? [20, 0] : [42.65, -75.6];
  const zoom = world ? 2 : 7;
  const container = useRef(null);
  const instance = useRef(null);
  const select = useRef(onSelect);
  select.current = onSelect;

  useEffect(() => {
    // Strict Mode/Fast Refresh can replay effects on the same DOM node.
    // Each setup owns a fresh map and each cleanup removes exactly that map.
    const map = L.map(container.current, { scrollWheelZoom: false, preferCanvas: true, worldCopyJump: true, minZoom: 2 }).setView(center, zoom);
    L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
    }).addTo(map);
    const airportLayer = L.layerGroup().addTo(map);
    const flightLayer = L.layerGroup().addTo(map);
    instance.current = { map, airportLayer, flightLayer, markers: new Map(), focus: null };
    const resize = new ResizeObserver(() => map.invalidateSize());
    resize.observe(container.current);
    return () => {
      resize.disconnect();
      instance.current = null;
      map.remove();
    };
  }, [scope]);

  useEffect(() => {
    const current = instance.current;
    if (!current) return;
    current.airportLayer.clearLayers();
    airports.forEach(a => L.circleMarker([a.lat, a.lon], {
      radius: a.scheduled ? 5 : 3, color: "#49758e", weight: 1, fillColor: "#fff", fillOpacity: 1,
    }).bindPopup(popup([a.code + " · " + a.name, a.municipality, a.scheduled ? "Scheduled service" : "General aviation"]))
      .addTo(current.airportLayer));
  }, [airports, scope]);

  useEffect(() => {
    const current = instance.current;
    if (!current) return;
    const ids = new Set(flights.map(f => f.id));
    for (const [id, marker] of current.markers) {
      if (!ids.has(id)) {
        current.flightLayer.removeLayer(marker);
        current.markers.delete(id);
      }
    }
    flights.forEach(f => {
      let marker = current.markers.get(f.id);
      if (!marker) {
        marker = L.circleMarker([f.lat, f.lon], { color: "#fff", weight: 2, fillOpacity: 1 })
          .on("click", () => select.current(f.id)).addTo(current.flightLayer);
        current.markers.set(f.id, marker);
      }
      const signature = JSON.stringify([f.position_time, f.lat, f.lon, f.phase, f.airport_code, f.co2_kg_min, f.on_ground, f.id === selected]);
      if (marker.observationSignature === signature) return;
      marker.observationSignature = signature;
      marker.setLatLng([f.lat, f.lon]).setRadius(f.id === selected ? 9 : 6)
        .setStyle({ fillColor: f.on_ground ? "#718096" : "#009e87" });
      const content = popup([f.callsign, f.phase + " · " + (f.airport_code ? f.airport_code + " vicinity" : "No nearby airport"),
        "Estimated CO₂: " + (f.co2_kg_min == null ? "Not modelled" : f.co2_kg_min + " kg/min")]);
      if (marker.getPopup()) marker.setPopupContent(content);
      else marker.bindPopup(content);
    });
  }, [flights, selected, scope]);

  useEffect(() => {
    const current = instance.current;
    if (!current) return;
    const target = flights.find(f => f.id === selected) || airports.find(a => a.id === airport);
    const focus = target ? airport + ":" + (selected || "") : "all";
    // Polling must not undo the viewer's pan/zoom.
    if (current.focus === focus) return;
    current.focus = focus;
    current.map.setView(target ? [target.lat, target.lon] : center, target ? (selected ? 10 : 9) : zoom);
  }, [airport, selected, airports, flights, scope]);

  return <div ref={container} role="region" aria-label={world ? "Live map of worldwide airports and aircraft" : "Live map of New York airports and aircraft"}
    style={{ height: 420, width: "100%", background: "#dce8ed" }} />;
}

