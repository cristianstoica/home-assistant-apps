"""The cutover (daily-close switch-over plan §6; test IDs CV1-CV10 of
§17.3).

CV1-CV7, CV9 and CV10 pinned ``wxverify.worker.cutover``, the one-time
switch-over machinery that built each site's first publication and set the
switch time. The daily close is now permanent and there is no
``wxverify.worker.cutover`` module, so those tests have no behaviour left
to pin and are deleted with it.

CV8 is unrelated to the cutover module itself -- it only pins the
methodology version, which did not change here -- so it stays.

Synthetic data only: no real station id, coordinate, site or zone name.
"""

from __future__ import annotations


def test_cv8_methodology_version_is_5() -> None:
    from wxverify.verification.methodology import METHODOLOGY_VERSION

    assert METHODOLOGY_VERSION == 5, (
        f"M14 -> at this assertion: correct = 5, mutant = {METHODOLOGY_VERSION}"
    )
