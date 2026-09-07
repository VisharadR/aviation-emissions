import unittest
from fastapi.testclient import TestClient
from app.live_main import app

class LiveDeploymentTests(unittest.TestCase):
    def test_only_read_only_live_routes_are_deployed(self):
        with TestClient(app) as client:
            paths = client.get("/openapi.json").json()["paths"]
            self.assertEqual(set(paths), {"/health", "/live/world", "/live/ny"})
            self.assertTrue(all(set(methods) == {"get"} for methods in paths.values()))
            self.assertEqual(client.get("/health").json(), {"ok": True})

if __name__ == "__main__":
    unittest.main()
