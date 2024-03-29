# (C) Copyright 2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#


class StringExpression:
    """It represents a string constant expression, e.g. 'Hello, world!'."""

    def __init__(self, value, quote):
        self.value = value
        self.quote = quote

    def __repr__(self):
        return self.quote + self.value + self.quote

    def evaluate(self, context):
        return self.value
