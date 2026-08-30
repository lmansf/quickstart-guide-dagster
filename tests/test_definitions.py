"""One test that catches every wiring error: the Definitions object must load."""

import dagster as dg


def test_definitions_are_loadable():
    from cadence.definitions import defs

    dg.Definitions.validate_loadable(defs)
