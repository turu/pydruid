#
# Copyright 2013 Metamarkets Group Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import division
from __future__ import absolute_import

import json
from pydruid.client import BaseDruidClient

try:
    from tornado import gen
    from tornado.httpclient import AsyncHTTPClient, HTTPError
except ImportError:
    print('Warning: unable to import Tornado. The asynchronous client will not work.')


class AsyncPyDruid(BaseDruidClient):
    """
    Asynchronous implementation of Druid client.
    """

    def __init__(self, url, endpoint):
        super(AsyncPyDruid, self).__init__(url, endpoint)

    @gen.coroutine
    def _post(self, query):
        http_client = AsyncHTTPClient()
        try:
            headers, querystr, url = self._prepare_url_headers_and_body(query)
            response = yield http_client.fetch(url, method='POST', headers=headers, body=querystr)
        except HTTPError as e:
            err = None
            if e.code == 500:
                # has Druid returned an error?
                try:
                    err = json.loads(e.response.body.decode("utf-8"))
                except ValueError:
                    pass
                else:
                    err = err.get('error', None)

            raise IOError('{0} \n Druid Error: {1} \n Query is: {2}'.format(
                    e, err, json.dumps(query.query_dict, indent=4)))
        else:
            query.parse(response.body.decode("utf-8"))
            raise gen.Return(query)

    @gen.coroutine
    def topn(self, **kwargs):
        query = self.query_builder.topn(kwargs)
        result = yield self._post(query)
        raise gen.Return(result)

    @gen.coroutine
    def timeseries(self, **kwargs):
        query = self.query_builder.timeseries(kwargs)
        result = yield self._post(query)
        raise gen.Return(result)

    @gen.coroutine
    def groupby(self, **kwargs):
        query = self.query_builder.groupby(kwargs)
        result = yield self._post(query)
        raise gen.Return(result)

    @gen.coroutine
    def segment_metadata(self, **kwargs):
        query = self.query_builder.segment_metadata(kwargs)
        result = yield self._post(query)
        raise gen.Return(result)

    @gen.coroutine
    def time_boundary(self, **kwargs):
        query = self.query_builder.time_boundary(kwargs)
        result = yield self._post(query)
        raise gen.Return(result)

    @gen.coroutine
    def select(self, **kwargs):
        query = self.query_builder.select(kwargs)
        result = yield self._post(query)
        raise gen.Return(result)
