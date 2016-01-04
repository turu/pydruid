# -*- coding: UTF-8 -*-
import pytest
import tornado
import tornado.ioloop
import tornado.web
from mock import patch, Mock
from six.moves import urllib
from tornado.testing import AsyncHTTPTestCase
from tornado.httpclient import HTTPError

from pydruid.client import PyDruid, AsyncPyDruid
from pydruid.utils.aggregators import doublesum
from pydruid.utils.filters import Dimension


def create_client():
    return PyDruid("http://localhost:8083", "druid/v2/")


class TestPyDruid:
    @patch('pydruid.client.urllib.request.urlopen')
    def test_druid_returns_error(self, mock_urlopen):
        # given
        ex = urllib.error.HTTPError(None, 500, "Druid error", None, None)
        mock_urlopen.side_effect = ex
        client = create_client()

        # when / then
        with pytest.raises(IOError):
            client.topn(
                    datasource="testdatasource",
                    granularity="all",
                    intervals="2015-12-29/pt1h",
                    aggregations={"count": doublesum("count")},
                    dimension="user_name",
                    metric="count",
                    filter=Dimension("user_lang") == "en",
                    threshold=1,
                    context={"timeout": 1000})

    @patch('pydruid.client.urllib.request.urlopen')
    def test_druid_returns_results(self, mock_urlopen):
        # given
        response = Mock()
        response.read.return_value = """
            [ {
  "timestamp" : "2015-12-30T14:14:49.000Z",
  "result" : [ {
    "dimension" : "aaaa",
    "metric" : 100
  } ]
            } ]
        """.encode("utf-8")
        mock_urlopen.return_value = response
        client = create_client()

        # when
        top = client.topn(
                datasource="testdatasource",
                granularity="all",
                intervals="2015-12-29/pt1h",
                aggregations={"count": doublesum("count")},
                dimension="user_name",
                metric="count",
                filter=Dimension("user_lang") == "en",
                threshold=1,
                context={"timeout": 1000})

        # then
        assert top is not None
        assert len(top.result) == 1
        assert len(top.result[0]['result']) == 1


class FailureHandler(tornado.web.RequestHandler):
    def post(self):
        raise HTTPError(500, "Druid error", response="Druid error")


class SuccessHandler(tornado.web.RequestHandler):
    def post(self):
        self.write("""
            [ {
  "timestamp" : "2015-12-30T14:14:49.000Z",
  "result" : [ {
    "dimension" : "aaaa",
    "metric" : 100
  } ]
            } ]
        """)


class TestAsyncPyDruid(AsyncHTTPTestCase):
    def get_app(self):
        return tornado.web.Application([
            (r"/druid/v2/fail_request", FailureHandler),
            (r"/druid/v2/return_results", SuccessHandler)
        ])

    @tornado.testing.gen_test
    def test_druid_returns_error(self):
        # given
        client = AsyncPyDruid("http://localhost:%s" % (self.get_http_port(), ),
                              "druid/v2/fail_request")

        # when / then
        with pytest.raises(IOError):
            yield client.topn(
                    datasource="testdatasource",
                    granularity="all",
                    intervals="2015-12-29/pt1h",
                    aggregations={"count": doublesum("count")},
                    dimension="user_name",
                    metric="count",
                    filter=Dimension("user_lang") == "en",
                    threshold=1,
                    context={"timeout": 1000})

    @tornado.testing.gen_test
    def test_druid_returns_results(self):
        # given
        client = AsyncPyDruid("http://localhost:%s" % (self.get_http_port(), ),
                              "druid/v2/return_results")

        # when
        top = yield client.topn(
                datasource="testdatasource",
                granularity="all",
                intervals="2015-12-29/pt1h",
                aggregations={"count": doublesum("count")},
                dimension="user_name",
                metric="count",
                filter=Dimension("user_lang") == "en",
                threshold=1,
                context={"timeout": 1000})

        # then
        self.assertIsNotNone(top)
        self.assertEqual(len(top.result), 1)
        self.assertEqual(len(top.result[0]['result']), 1)
