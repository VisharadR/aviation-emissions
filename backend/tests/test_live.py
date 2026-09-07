import time
import unittest
from unittest.mock import patch
import requests
from fastapi.testclient import TestClient
from app.live import LiveService, normalize, load_ny_airports
from app.main import app

AIRPORT = dict(id="KJFK", code="JFK", lat=40.6413, lon=-73.7781)
NOW = 1800000000
def state(**kw):
    row = ["abc123", "TEST1 ", "United States", NOW, NOW, -73.7781, 40.6413, 2000, False, 200, 90, 2, None, 2100]
    for key, value in kw.items():
        row[int(key)] = value
    return row

class LiveTests(unittest.TestCase):
    def test_unit_conversion_and_no_trip_overhead(self):
        f = normalize(dict(time=NOW,states=[state()]),[AIRPORT],NOW)[0]
        self.assertEqual(f["co2_kg_min"],113.76)
        self.assertEqual(f["speed_kmh"],720)
        self.assertEqual(f["phase"],"Climb")

    def test_ground_missing_and_invalid_speed_are_not_zero_emissions(self):
        for changes in ({"8":True},{"9":None},{"9":-1},{"9":float("nan")},{"8":None}):
            self.assertIsNone(normalize(dict(time=NOW,states=[state(**changes)]),[AIRPORT],NOW)[0]["co2_kg_min"])

    def test_stale_missing_far_future_and_malformed_positions_excluded(self):
        rows = [[],state(**{"3":NOW-121}),state(**{"4":NOW-121}),state(**{"5":None}),state(**{"6":float("nan")}),state(**{"3":NOW+100}),state(**{"5":-80})]
        self.assertEqual(normalize(dict(time=NOW,states=rows),[AIRPORT],NOW),[])

    def test_duplicates_choose_latest_and_assign_once(self):
        rows = [state(**{"3":NOW-5}),state()]
        found = normalize(dict(time=NOW,states=rows),[AIRPORT,dict(AIRPORT,id="OTHER")],NOW)
        self.assertEqual(len(found),1)
        self.assertEqual(found[0]["position_time"],NOW)

    def test_null_states_is_valid_empty_snapshot(self):
        self.assertEqual(normalize(dict(time=NOW,states=None),[AIRPORT],NOW),[])
        with self.assertRaises(ValueError): normalize(dict(time=NOW,states={}),[AIRPORT],NOW)

    def test_catalog_is_new_york_and_excludes_newark(self):
        airports=load_ny_airports()
        self.assertIn("KJFK",[a["id"] for a in airports])
        self.assertNotIn("KEWR",[a["id"] for a in airports])
        self.assertGreater(len(airports),20)

    def test_cache_expiry_and_outage_never_fabricate_data(self):
        s=LiveService(); s.airports=[AIRPORT]
        with patch("app.live.time.time",return_value=NOW), patch.object(s,"fetch",return_value=dict(time=NOW,states=[state()])) as fetch:
            self.assertEqual(s.get()["status"],"live")
            s.get(); self.assertEqual(fetch.call_count,1)
        with patch("app.live.time.time",return_value=NOW+130),patch.object(s,"fetch",side_effect=requests.Timeout):
            d=s.get(); self.assertEqual(d["status"],"stale"); self.assertEqual(d["flights"],[])
            self.assertEqual(d["observed_at"],NOW)

    def test_api_and_historical_routes_remain(self):
        with TestClient(app) as client, patch("app.live.service.get",return_value=dict(status="unavailable",flights=[])):
            self.assertEqual(client.get("/live/ny").json()["status"],"unavailable")
            self.assertEqual(client.get("/health").status_code,200)
            self.assertIn("/co2/summary/{date_yyyymmdd}",client.get("/openapi.json").json()["paths"])

if __name__ == "__main__": unittest.main()
