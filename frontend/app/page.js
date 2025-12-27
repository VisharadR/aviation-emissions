"use client";

import { useEffect, useState } from "react";

import dynamic from "next/dynamic";
const EmissionsMap = dynamic(() => import("./components/EmissionsMap"), { ssr: false });

export default function Home() {
  const [date, setDate] = useState("2025-12-25");
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [mapData, setMapData] = useState(null);


  async function fetchData() {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`http://127.0.0.1:8000/co2/summary/${date}`);
      if (!res.ok) throw new Error("No data for this date");
      const json = await res.json();
      setData(json);

      const resMap = await fetch(`http://127.0.0.1:8000/co2/map/${date}`);
      if (!resMap.ok) throw new Error("No map data for this date");
      const jsonMap = await resMap.json();
      setMapData(jsonMap);

    } catch (err) {
      setError(err.message);
      setData(null);
      setMapData(null);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    fetchData();
  }, []);

  return (
    <main className="p-8 max-w-6xl mx-auto">
      <h1 className="text-3xl font-bold mb-2">✈️ Aviation CO₂ Emissions</h1>
      <p className="text-gray-600 mb-6">
        Global aviation CO₂ estimates based on OpenSky ADS-B data
      </p>

      {/* Date Picker */}
      <div className="flex gap-4 items-center mb-6">
        <input
          type="date"
          value={date}
          onChange={(e) => setDate(e.target.value)}
          className="border px-3 py-2 rounded"
        />
        <button
          onClick={fetchData}
          className="bg-black text-white px-4 py-2 rounded"
        >
          Load
        </button>
      </div>

      {loading && <p>Loading…</p>}
      {error && <p className="text-red-600">{error}</p>}

      {data && (
        <>
          {/* KPI cards */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
            <KPI title="Flights Computed" value={data.flights_computed} />
            <KPI
              title="Total CO₂ (tons)"
              value={data.total_co2_tons.toFixed(0)}
            />
            <KPI title="Date" value={data.date} />
          </div>

          {/* Top Routes */}
          <Section title="Top Routes by CO₂">
            <Table
              headers={["From", "To", "CO₂ (tons)"]}
              rows={data.top_routes.map((r) => [
                r.dep,
                r.arr,
                (r.co2_kg / 1000).toFixed(1),
              ])}
            />
          </Section>
          
          {/* Top Airports */}
          <Section title="Top Departure Airports">
            <Table
              headers={["Airport", "CO₂ (tons)"]}
              rows={data.top_departure_airports.map((a) => [
                a.dep,
                (a.co2_kg / 1000).toFixed(1),
              ])}
            />
          </Section>

          {/* World Map */}
          {mapData && (
            <Section title="World Map (Airport CO₂ bubbles + Top route lines)">
              <EmissionsMap airports={mapData.airports} routes={mapData.routes} />
            </Section>
          )}
        </>
      )}
    </main>
  );
}

function KPI({ title, value }) {
  return (
    <div className="border rounded p-4">
      <p className="text-sm text-gray-500">{title}</p>
      <p className="text-2xl font-semibold">{value}</p>
    </div>
  );
}

function Section({ title, children }) {
  return (
    <section className="mb-8">
      <h2 className="text-xl font-semibold mb-3">{title}</h2>
      {children}
    </section>
  );
}

function Table({ headers, rows }) {
  return (
    <table className="w-full border">
      <thead className="bg-gray-100">
        <tr>
          {headers.map((h) => (
            <th key={h} className="border px-3 py-2 text-left">
              {h}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, i) => (
          <tr key={i}>
            {row.map((cell, j) => (
              <td key={j} className="border px-3 py-2">
                {cell}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
