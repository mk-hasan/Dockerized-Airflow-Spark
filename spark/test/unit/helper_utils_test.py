from spark.src.app.extract_job import HelperUtils
import unittest


class HelperUtilsTest(unittest.TestCase):

    def test_valid_ip_function(self):
        ips = ['192.168.0.1', '192.168.0.1000']
        self.assertEqual(HelperUtils().validIPAddress(ips[0]), 'valid')
        self.assertEqual(HelperUtils().validIPAddress(ips[1]), 'invalid')
        print("Everything passed")


if __name__ == "__main__":
    unittest.main()
