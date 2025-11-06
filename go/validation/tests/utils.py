# Copyright (c) 2025 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import functools
import time

import adbc_driver_manager.dbapi


def retry_rate_limit(f):
    @functools.wraps(f)
    def retried(*args, **kwargs):
        # BigQuery tends to be flaky when we reuse the same table
        for i in range(10):
            try:
                f(*args, **kwargs)
            except adbc_driver_manager.dbapi.Error as e:
                if "Exceeded rate limits" in str(e):
                    delay = min(60, 2 ** (i + 2))
                    print("backing off and trying again after", delay, "seconds")
                    time.sleep(delay)
                    continue
                else:
                    raise
            else:
                break

    return retried
