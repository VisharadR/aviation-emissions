import random
import unittest
from unittest.mock import patch, Mock
from app.live import AirportIndex, LiveService, distance, normalize, RADIUS_KM, live_new_york
from test_live import state, NOW, AIRPORT

class WorldTests(unittest.TestCase):
    def test_world_retains_aircraft_far_from_airports(self):
        payload = dict(time=NOW, states=[state(**{"5":0,"6":0})])
        flights = normalize(payload, [AIRPORT], NOW, worldwide=True, index=AirportIndex([AIRPORT]))
        self.assertEqual(len(flights),1)
        self.assertIsNone(flights[0]["airport_id"])
        self.assertIsNone(flights[0]["distance_km"])
        self.assertEqual(flights[0]["co2_kg_min"],113.76)
        self.assertEqual(normalize(payload,[AIRPORT],NOW),[])

    def test_dateline_and_polar_nearest_airports(self):
        for lat,lon,alat,alon in [(0,179.95,0,-179.95),(89.99,0,89.99,170),(-89.99,-50,-89.99,110)]:
            a=dict(AIRPORT,lat=alat,lon=alon)
            airport,km=AirportIndex([a]).nearest(lat,lon)
            self.assertEqual(airport,a)
            self.assertLess(km,RADIUS_KM)

    def test_index_matches_brute_force_at_bucket_edges(self):
        rng=random.Random(11)
        for _ in range(150):
            lat,lon=rng.uniform(-89.9,89.9),rng.uniform(-180,180)
            airports=[dict(AIRPORT,id=str(i),lat=max(-90,min(90,lat+rng.uniform(-.4,.4))),lon=(lon+rng.uniform(-3,3)+180)%360-180) for i in range(25)]
            expected=min(airports,key=lambda a:distance(lat,lon,a))
            actual,km=AirportIndex(airports).nearest(lat,lon)
            if distance(lat,lon,expected)>RADIUS_KM:
                self.assertIsNone(actual)
            else:
                self.assertEqual(actual["id"],expected["id"])

    def test_global_fetch_has_no_bounding_box_and_conservative_interval(self):
        s=LiveService("world")
        s.client=Mock(client_id="test",client_secret="test")
        s.client._ensure_token.return_value="test"
        response=Mock(status_code=200)
        response.json.return_value=dict(time=NOW,states=[])
        with patch("app.live.requests.get",return_value=response) as get:
            s.fetch()
            self.assertEqual(get.call_args.kwargs["params"],{})
            self.assertEqual(s.interval,90)

    def test_world_rate_limit_backoff_is_not_bypassed(self):
        import requests
        s=LiveService("world"); s.airports=[AIRPORT]
        s.client=Mock(client_id="test",client_secret="test")
        s.client._ensure_token.return_value="test"
        response=Mock(status_code=429,headers={"X-Rate-Limit-Retry-After-Seconds":"600"})
        response.raise_for_status.side_effect=requests.HTTPError(response=response)
        with patch("app.live.time.time",return_value=NOW),patch("app.live.requests.get",return_value=response) as get:
            self.assertEqual(s.get()["status"],"unavailable")
            self.assertEqual(s.next_fetch,NOW+600)
            s.get(); self.assertEqual(get.call_count,1)

    def test_ny_route_filters_shared_global_snapshot(self):
        flights=[dict(airport_id="KJFK"),dict(airport_id="EGLL"),dict(airport_id=None)]
        with patch("app.live.service.get",return_value=dict(flights=flights)),patch("app.live.load_ny_airports",return_value=[AIRPORT]):
            data=live_new_york()
            self.assertEqual(data["flights"],[dict(airport_id="KJFK")])
            self.assertEqual(data["scope"],"ny")

if __name__ == "__main__": unittest.main()
