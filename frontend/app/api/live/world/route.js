export const dynamic = "force-dynamic";
export async function GET() {
  try {
    const base = process.env.AVIATION_API_URL || "http://127.0.0.1:8000";
    const res = await fetch(`${base}/live/world`, { cache: "no-store", signal: AbortSignal.timeout(60000) });
    if (!res.ok) throw new Error("Backend unavailable");
    return Response.json(await res.json(), { headers: { "Cache-Control": "no-store" } });
  } catch {
    return Response.json({ error: "Cannot reach the live data service. Start the aviation backend and try again." }, { status: 503 });
  }
}
