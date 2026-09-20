"""核对剧场播控服务的基础身份。"""

import unittest
from service import SERVICE_ID, health


class HealthTest(unittest.TestCase):
    def test_identity(self):
        self.assertEqual(health(), {"status": "ok", "service": SERVICE_ID})


if __name__ == "__main__":
    unittest.main()
