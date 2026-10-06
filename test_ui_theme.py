"""Unit tests for ui_theme helpers (pure logic only — no display required)."""
import re

import pytest

from ui_theme import STATUS_COLORS, palette_colors, pick_font, status_style


def test_pick_font_returns_first_installed_candidate():
    assert pick_font(["A", "B", "C"], ["C", "B"], "Z") == "B"


def test_pick_font_falls_back_when_none_installed():
    assert pick_font(["A", "B"], ["X"], "Z") == "Z"


def test_pick_font_empty_candidates():
    assert pick_font([], ["A"], "Z") == "Z"


@pytest.mark.parametrize("kind", sorted(STATUS_COLORS))
def test_status_style_known_kinds(kind):
    assert status_style(kind) == f"{kind.capitalize()}.Status.TLabel"


def test_status_style_rejects_unknown_kind():
    with pytest.raises(ValueError, match="unknown status kind"):
        status_style("bogus")


def test_palette_values_are_hex_colors():
    hex_color = re.compile(r"^#[0-9a-fA-F]{6}$")
    assert all(hex_color.match(c) for c in palette_colors())
