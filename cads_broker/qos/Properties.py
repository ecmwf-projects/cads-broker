# (C) Copyright 2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#


class Properties:
    """It is a placeholder to hold all rules that matches a given request."""

    def __init__(self):
        self.starting_priority = 0
        self.limits = []
        self.priorities = []
        self.dynamic_priorities = []
        self.permissions = []
