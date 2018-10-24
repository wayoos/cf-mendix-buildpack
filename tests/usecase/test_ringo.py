import json
import os
import sys
import unittest
from time import sleep

import requests
import requests_mock

BUILDPACK_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
)
sys.path.insert(0, BUILDPACK_DIR)
from lib.ringo import Ringo


@requests_mock.Mocker(kw="mocked_requests")
class RingoTest(unittest.TestCase):
    def setUp(self, **kwargs):
        self.fifo_filename = "/tmp/ringo.log"
        try:
            os.unlink(self.fifo_filename)
        except FileNotFoundError:
            pass
        os.mkfifo(self.fifo_filename)

    def tearDown(self, **kwargs):
        os.unlink(self.fifo_filename)

    def test_ringo(self, **kwargs):
        mocked_requests = kwargs["mocked_requests"]
        mocked_requests.get("http://example.com", text="hello")
        ringo = Ringo(
            input_filename=self.fifo_filename, target_url="http://example.com/"
        )
        # self.assertTrue(False)
        try:
            ringo.run()
        except KeyboardInterrupt:
            raise RuntimeError

        with open(self.fifo_filename, "w") as fifo:
            desired_lines = []
            for x in range(0, 10):
                line = "test line {}\n".format(x)
                desired_lines.append(line)
                fifo.write(line)
            sleep(3)
            for x in range(11, 20):
                line = "test line {}\n".format(x)
                desired_lines.append(line)
                fifo.write(line)
            sleep(3)
            desired_output = json.dumps({"log_lines": [desired_lines]})
            self.assertTrue(mocked_requests.called)
            self.assertEqual(1, mocked_requests.call_count)
            last_req = mocked_requests.last_request
            self.assertEqual(last_req.json, desired_output)

        # resp = requests.get("http://example.com")

        # self.assertEqual("hello", resp.text)
        print(mocked_requests.request_history)
