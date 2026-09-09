"use client";
import { useEffect, useMemo, useRef, useState } from "react";
import dynamic from "next/dynamic";
import Link from "next/link";
import styles from "./live.module.css";

const LiveMap = dynamic(() => import("./LiveMap"), { ssr: false, loading: () => <div className={styles.mapLoading}>Preparing airport map…</div> });
const fmt = (n, digits = 0) => n == null ? "—" : n.toLocaleString("en-US", { maximumFractionDigits: digits });
const clock = (t) => t ? new Date(t * 1000).toLocaleTimeString("en-US", { timeZone: "UTC", hour: "2-digit", minute: "2-digit", second: "2-digit" }) + " UTC" : "Awaiting observations";

const EMPTY = [];
const countryNames = new Intl.DisplayNames(["en"], { type: "region" });
const countryName = code => { try { return countryNames.of(code) || code; } catch { return code; } };
export default function LiveDashboard({ scope = "world" }) {
  const worldwide = scope === "world";
  const [country, setCountry] = useState("all");
  const [page, setPage] = useState(0);
  const [data, setData] = useState(null), [error, setError] = useState(null);
  const [busy, setBusy] = useState(false), [paused, setPaused] = useState(false);
  const [airport, setAirport] = useState("all"), [query, setQuery] = useState("");
  const [selected, setSelected] = useState(null), [now, setNow] = useState(0);
  const [history, setHistory] = useState([]);
  const refresh = useRef(() => {});
  const pauseRef = useRef(false);
  useEffect(() => { pauseRef.current = paused; }, [paused]);
  useEffect(() => {
    if (process.env.NEXT_PUBLIC_LIVE_ONLY === "true" && !process.env.NEXT_PUBLIC_LIVE_API_URL) {
      setError("Live backend disconnected. Flight observations and emission estimates are unavailable.");
      refresh.current = () => {};
      return;
    }
    let alive = true, fetching = false, nextPollAt = 0;
    const controller = new AbortController();
    const update = async () => {
      if (fetching) return;
      fetching = true; setBusy(true);
      try {
        const apiOrigin = process.env.NEXT_PUBLIC_LIVE_API_URL?.replace(/\/$/, "");
        const res = await fetch(apiOrigin ? `${apiOrigin}/live/${scope}` : `/api/live/${scope}`, { cache: "no-store", signal: controller.signal });
        const next = await res.json();
        if (!res.ok) throw new Error(next.error || "Live feed unavailable");
        if (!alive) return;
        setData(next); setError(null);
        nextPollAt = Math.max(Date.now()+1000, Math.min(Date.now()+30000, next.next_fetch_at*1000+250));
        if (next.status === "live" && next.flights.some(f => f.co2_kg_min != null)) setHistory(old => old.some(p => p.t === next.observed_at) ? old : [...old, { t: next.observed_at, rate: next.flights.reduce((sum, f) => sum + (f.co2_kg_min || 0), 0) }].slice(-30));
      } catch (e) { nextPollAt = Date.now()+30000; if (alive) setError(e.message); }
      finally { fetching = false; if (alive) setBusy(false); }
    };
    refresh.current = update; update();
    const poll = setInterval(() => { if (!pauseRef.current && document.visibilityState === "visible" && Date.now() >= nextPollAt) update(); }, 1000);
    return () => { alive = false; controller.abort(); clearInterval(poll); };
  }, [scope]);
  useEffect(() => { setNow(Date.now()/1000); const timer = setInterval(() => setNow(Date.now()/1000), 1000); return () => clearInterval(timer); }, []);
  const airports = data?.airports || EMPTY;
  const fresh = !error && data?.status === "live" && now - data.observed_at <= 120;
  const expiryTick = Math.floor(now / 5) * 5;
  const flights = useMemo(() => (data?.flights || EMPTY).filter(f => expiryTick-f.position_time <= 115 && expiryTick-f.last_contact <= 115), [data, expiryTick]);
  const visible = useMemo(() => flights.filter(f =>
    (country === "all" || (country === "unassociated" ? !f.airport_id : f.airport_country === country)) &&
    (airport === "all" || f.airport_id === airport) &&
    `${f.callsign} ${f.id}`.toLowerCase().includes(query.toLowerCase())
  ), [flights, country, airport, query]);
  const modelled = visible.filter(f => f.co2_kg_min != null);
  const total = modelled.reduce((n, f) => n + f.co2_kg_min, 0);
  const active = new Set(visible.map(f => f.airport_id).filter(Boolean)).size;
  const detail = visible.find(f => f.id === selected);
  const ranks = useMemo(() => {
    const byId = new Map(airports.map(a => [a.id, a]));
    const groups = new Map();
    visible.forEach(f => {
      if (!f.airport_id) return;
      const a = groups.get(f.airport_id) || { ...byId.get(f.airport_id), count: 0, rate: null };
      a.count++;
      if (f.co2_kg_min != null) a.rate = (a.rate || 0) + f.co2_kg_min;
      groups.set(f.airport_id, a);
    });
    return [...groups.values()].sort((a,b) => (b.rate || 0)-(a.rate || 0)).slice(0,5);
  }, [airports, visible]);
  const airportOptions = useMemo(() => airports.filter(a => country === "all" || a.country === country), [airports, country]);
  const pageCount = Math.max(1, Math.ceil(visible.length / 100));
  const currentPage = Math.min(page, pageCount - 1);
  const pageFlights = visible.slice(currentPage * 100, (currentPage + 1) * 100);
  useEffect(() => { setPage(0); setSelected(null); }, [country, airport, query]);
  const peak = Math.max(1, ...history.map(h => h.rate));
  const status = paused ? "Paused" : fresh ? "Live observations" : error ? "Feed unavailable" : data ? "Feed delayed" : "Connecting";

  return <main className={styles.shell}>
    <nav className={styles.nav}><Link href="/" className={styles.brand}>model<span>.earth</span> <small>AVIATION</small></Link><div><Link href={worldwide ? "/live/ny" : "/live"}>{worldwide ? "New York view" : "World view"}</Link>{process.env.NEXT_PUBLIC_LIVE_ONLY !== "true" && <Link href="/">Historical analytics ↗</Link>}<span className={styles.navLabel}>{worldwide ? "WORLDWIDE" : "NEW YORK STATE"}</span></div></nav>
    <header className={styles.header}><div><p className={styles.eyebrow}>AIRPORT OBSERVATORY / {worldwide ? "WORLD" : "NY"}</p><h1>{worldwide ? "The world, in flight" : "New York, in flight"}<span>.</span></h1><p>Live aircraft observations. A clearer view of estimated carbon emissions.</p></div><div className={styles.feed}><span className={`${styles.badge} ${fresh && !paused ? styles.live : ""}`}><i />{status}</span><span>Observed {clock(data?.observed_at)}</span></div></header>
    <section className={styles.toolbar} aria-label="Flight controls">{worldwide && <label>Airport country<select aria-label="Airport country" value={country} onChange={e => {setCountry(e.target.value); setAirport("all");}}><option value="all">All countries · includes en route</option><option value="unassociated">No nearby airport</option>{(data?.countries || []).map(c => <option key={c} value={c}>{countryName(c)}</option>)}</select></label>}<label>Airport<select aria-label="Airport" value={airport} onChange={e => {setAirport(e.target.value); setSelected(null);}}><option value="all">{worldwide ? "All observed airports" : "All New York airports"}</option>{airportOptions.map(a => <option key={a.id} value={a.id}>{a.code} · {a.name}</option>)}</select></label><label className={styles.search}>Find an aircraft<input type="search" placeholder="Callsign or ICAO address" value={query} onChange={e => setQuery(e.target.value)} /></label><button onClick={() => {setPaused(p => !p); if (paused) refresh.current();}}>{paused ? "Resume updates" : "Pause updates"}</button><button disabled={busy} onClick={() => refresh.current()}>{busy ? "Updating…" : "Refresh ↻"}</button></section>
    {(error || data?.error || (data && !fresh)) && <div className={styles.notice} role="status">{error || data?.error || "Observations have expired. Current emissions are unavailable until the feed updates."} {data?.next_fetch_at && !error ? `Next provider check: ${clock(data.next_fetch_at)}.` : ""}</div>}
    <section className={styles.stats} aria-label="Current observations"><article><span>Estimated CO₂ rate</span><strong>{fresh && modelled.length ? fmt(total/1000,2) : "—"}<small>t / min</small></strong><p>Visible modelled aircraft · not a trip total</p></article><article><span>Aircraft observed</span><strong>{fresh ? visible.length : "—"}<small>aircraft</small></strong><p>{fresh ? modelled.length : "—"} with an airborne rate estimate</p></article><article><span>Airports with nearby activity</span><strong>{fresh ? active : "—"}<small>/ {fmt(data?.airport_count)}</small></strong><p>Nearest airport within 25 km</p></article><article><span>Position freshness</span><strong>{data?.observed_at ? Math.max(0, Math.floor(now-data.observed_at)) : "—"}<small>seconds</small></strong><p>Positions expire after 120 seconds</p></article></section>
    <section className={styles.workspace}><article className={styles.mapCard}><div className={styles.sectionHeading}><div><p className={styles.eyebrow}>THE AIRSPACE</p><h2>{worldwide ? "A worldwide perspective" : "Across the Empire State"}</h2></div><span>{fresh ? visible.length : 0} visible aircraft</span></div><LiveMap scope={scope} airports={airports} flights={fresh ? visible : EMPTY} airport={airport} selected={selected} onSelect={setSelected} /><div className={styles.mapLegend}><span><b className={styles.dot} /> {worldwide ? "Airport with observations" : "New York airport"}</span><span><b className={styles.plane}>●</b> Observed aircraft</span><span>Proximity ≠ confirmed route</span></div></article>
    <aside className={styles.side}><article className={styles.trend}><p className={styles.eyebrow}>THIS SESSION · {worldwide ? "WORLDWIDE" : "NEW YORK"}</p><h2>Emission rate trend</h2>{history.length > 1 ? <><svg viewBox="0 0 300 105" role="img" aria-label="Estimated emission rate per observed snapshot in this session"><path d={`M ${history.map((h,i) => `${i*300/(history.length-1)},${95-h.rate/peak*85}`).join(" L ")}`} fill="none" stroke="#1dbba1" strokeWidth="3" /></svg><div className={styles.trendAxis}><span>{clock(history[0].t)}</span><span>{fmt(peak/1000,2)} t/min peak</span></div></> : <p className={styles.empty}>The trend begins after two fresh observations. No historical points are simulated.</p>}</article><article className={styles.ranking}><p className={styles.eyebrow}>NEARBY ACTIVITY</p><h2>Airport breakdown</h2>{fresh && ranks.length ? ranks.map(a => <button key={a.id} onClick={() => setAirport(a.id)}><div><strong>{a.code}</strong><span>{a.count} aircraft</span><b>{fmt(a.rate,1)} <small>kg/min</small></b></div><span className={styles.bar}><i style={{width:`${Math.max(2, a.rate/Math.max(1,ranks[0].rate)*100)}%`}} /></span></button>) : <p className={styles.empty}>Airport activity appears when fresh positions are available.</p>}</article></aside></section>
    <section className={styles.tableCard}><div className={styles.sectionHeading}><div><p className={styles.eyebrow}>LIVE OBSERVATIONS</p><h2>Flight emissions</h2></div><span>{fresh ? visible.length : 0} matching aircraft</span></div>{fresh && detail && <div className={styles.detail}><strong>{detail.callsign}</strong> · {detail.id} · {detail.airport_id ? `${detail.distance_km} km from ${detail.airport_code}` : "No airport within 25 km"} · Position {clock(detail.position_time)}<button onClick={() => setSelected(null)} aria-label="Close aircraft details">×</button></div>}<div className={styles.tableScroll}><table><thead><tr><th>Aircraft</th><th>Nearest airport</th><th>Flight phase</th><th>Altitude</th><th>Ground speed</th><th>Est. CO₂ rate</th></tr></thead><tbody>{fresh && pageFlights.map(f => <tr key={f.id} className={selected === f.id ? styles.selected : ""}><td><button onClick={() => setSelected(f.id)}>{f.callsign}</button><small>{f.id}</small></td><td>{f.airport_code || "En route / unassociated"}<small>{f.distance_km == null ? "No airport within 25 km" : `${f.distance_km} km away`}</small></td><td><span className={styles.phase}>{f.phase}</span></td><td>{fmt(f.altitude_m)} m</td><td>{fmt(f.speed_kmh)} km/h</td><td className={styles.rate}>{fmt(f.co2_kg_min,1)} <small>{f.co2_kg_min == null ? "not modelled" : "kg/min"}</small></td></tr>)}</tbody></table>{(!fresh || !visible.length) && <div className={styles.tableEmpty}>{busy && !data ? "Connecting to live flight observations…" : !fresh ? "Waiting for a fresh live feed. No estimated emissions are shown as current." : "No fresh aircraft match this view. Try another airport or clear your search."}</div>}</div>{fresh && visible.length > 0 && <div className={styles.pagination}><span>Showing {currentPage*100+1}–{Math.min((currentPage+1)*100,visible.length)} of {fmt(visible.length)} aircraft · all matches remain on the map</span><button disabled={currentPage === 0} onClick={() => setPage(currentPage-1)}>Previous</button><button disabled={currentPage+1 >= pageCount} onClick={() => setPage(currentPage+1)}>Next</button></div>}</section>
    <section className={styles.method}><div><p className={styles.eyebrow}>READ THE NUMBERS RESPONSIBLY</p><h2>Observed flights.<br />Modelled emissions.</h2></div><div><p>CO₂ rate = ground speed in km/min × 3 kg fuel/km × 3.16 kg CO₂/kg fuel. This carries forward the historical project’s generic distance model, without repeating its fixed trip fuel allowance.</p><p>Aircraft type, engine, wind and actual fuel flow are unknown. This proxy is not calibrated for individual aircraft; ground and missing-speed observations have no estimate. Nearby aircraft may be overflights or outside the state boundary. Airport association is geographic proximity, not confirmed arrivals or departures.</p><p>Sources: <a href="https://opensky-network.org/data/api" target="_blank" rel="noreferrer">OpenSky Network</a> · <a href="https://ourairports.com/data/" target="_blank" rel="noreferrer">OurAirports</a>. Scheduled-service and general aviation airports {worldwide ? "worldwide" : "in US-NY"}; heliports and closed facilities excluded. No non-CO₂ effects or full-flight totals. OpenSky receiver coverage is incomplete, particularly over oceans and remote regions. These totals represent observed aircraft, not all aviation worldwide. Country filters use nearby-airport location. Provider updates every {data?.refresh_seconds || 120} seconds; expired observations are hidden.</p></div></section>
    <footer className={styles.footer}><span>MODEL.EARTH / AVIATION EMISSIONS</span><span>Live extension · Historical data stays available</span></footer>
  </main>;
}
