"""
Dark theme + small widget helpers for the MQTT Suite Tkinter GUI.
Built on ttk's 'clam' engine — no third-party dependencies.
"""
from __future__ import annotations

import logging
import sys
import tkinter as tk
from collections.abc import Callable, Iterable
from dataclasses import dataclass, fields
from tkinter import font as tkfont
from tkinter import ttk

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------
#  Design tokens
# --------------------------------------------------------------------------
@dataclass(frozen=True)
class Palette:
    bg: str = "#0f1117"
    surface: str = "#171a24"
    surface_alt: str = "#1f2331"
    border: str = "#2b3042"
    text: str = "#e6e8ef"
    muted: str = "#8b91a7"
    accent: str = "#7c5cff"
    accent_hover: str = "#9478ff"
    accent_press: str = "#6446f0"
    danger: str = "#ff5c7a"
    danger_hover: str = "#ff7a92"
    danger_press: str = "#e84564"
    success: str = "#3ddc97"
    warning: str = "#ffb547"
    info: str = "#4cc9f0"
    select: str = "#2e2754"


PALETTE = Palette()

STATUS_COLORS: dict[str, str] = {
    "ok": PALETTE.success,
    "warn": PALETTE.warning,
    "err": PALETTE.danger,
    "idle": PALETTE.muted,
}

UI_FONT_CANDIDATES = ("Segoe UI Variable Text", "Segoe UI", "Inter",
                      "SF Pro Text", "Helvetica Neue", "Helvetica", "Arial")
MONO_FONT_CANDIDATES = ("Cascadia Mono", "Cascadia Code", "JetBrains Mono",
                        "Consolas", "Menlo", "DejaVu Sans Mono", "Courier New")


@dataclass(frozen=True)
class Fonts:
    ui: tuple
    ui_bold: tuple
    small: tuple
    title: tuple
    stat: tuple
    logo: tuple
    mono: tuple


def pick_font(candidates: Iterable[str], available: Iterable[str], fallback: str) -> str:
    """Return the first candidate font family that is installed, else `fallback`."""
    installed = set(available)
    return next((name for name in candidates if name in installed), fallback)


def status_style(kind: str) -> str:
    """Map a status kind ('ok', 'warn', 'err', 'idle') to its ttk style name."""
    if kind not in STATUS_COLORS:
        raise ValueError(f"unknown status kind {kind!r}; expected one of {sorted(STATUS_COLORS)}")
    return f"{kind.capitalize()}.Status.TLabel"


# --------------------------------------------------------------------------
#  Platform niceties (Windows)
# --------------------------------------------------------------------------
def enable_hidpi() -> None:
    """Render crisp text on high-DPI Windows displays. Call before creating Tk()."""
    if sys.platform != "win32":
        return
    try:
        import ctypes
        ctypes.windll.shcore.SetProcessDpiAwareness(1)
    except (AttributeError, OSError) as exc:
        log.warning("could not enable DPI awareness: %s", exc)


def dark_title_bar(root: tk.Tk) -> None:
    """Ask Windows 10/11 to draw a dark title bar for `root`."""
    if sys.platform != "win32":
        return
    try:
        import ctypes
        root.update_idletasks()
        hwnd = ctypes.windll.user32.GetParent(root.winfo_id())
        enabled = ctypes.c_int(1)
        DWMWA_USE_IMMERSIVE_DARK_MODE = 20
        hr = ctypes.windll.dwmapi.DwmSetWindowAttribute(
            hwnd, DWMWA_USE_IMMERSIVE_DARK_MODE, ctypes.byref(enabled), ctypes.sizeof(enabled))
        if hr != 0:
            log.info("dark title bar not supported (HRESULT=%#x)", hr & 0xFFFFFFFF)
    except (AttributeError, OSError) as exc:
        log.warning("could not enable dark title bar: %s", exc)


# --------------------------------------------------------------------------
#  Theme
# --------------------------------------------------------------------------
def _build_fonts(root: tk.Misc) -> Fonts:
    families = tkfont.families(root)
    ui = pick_font(UI_FONT_CANDIDATES, families, "TkDefaultFont")
    mono = pick_font(MONO_FONT_CANDIDATES, families, "TkFixedFont")
    return Fonts(
        ui=(ui, 10), ui_bold=(ui, 10, "bold"), small=(ui, 9),
        title=(ui, 17, "bold"), stat=(ui, 15, "bold"), logo=(ui, 22, "bold"),
        mono=(mono, 10),
    )


def _flat(color: str) -> dict[str, str]:
    """clam draws 3D bevels with light/dark colors; flatten them to one color."""
    return {"bordercolor": color, "lightcolor": color, "darkcolor": color}


def apply_theme(root: tk.Tk) -> Fonts:
    """Install the dark theme on `root` and return the font set used."""
    p = PALETTE
    f = _build_fonts(root)
    root.configure(bg=p.bg)
    dark_title_bar(root)

    s = ttk.Style(root)
    s.theme_use("clam")
    s.configure(".", background=p.bg, foreground=p.text, font=f.ui,
                fieldbackground=p.surface_alt, troughcolor=p.surface,
                selectbackground=p.select, selectforeground=p.text,
                insertcolor=p.text, focuscolor=p.accent, **_flat(p.border))

    # frames & labels
    s.configure("TFrame", background=p.bg)
    s.configure("Card.TFrame", background=p.surface)
    s.configure("CardOuter.TFrame", background=p.surface, relief="solid",
                borderwidth=1, **_flat(p.border))
    s.configure("TLabel", background=p.bg, foreground=p.text)
    s.configure("Logo.TLabel", foreground=p.accent, font=f.logo)
    s.configure("Title.TLabel", font=f.title)
    s.configure("Subtitle.TLabel", foreground=p.muted)
    s.configure("StatValue.TLabel", font=f.stat)
    s.configure("StatCaption.TLabel", foreground=p.muted, font=f.small)
    s.configure("CardTitle.TLabel", background=p.surface, foreground=p.muted, font=f.ui_bold)
    s.configure("Field.TLabel", background=p.surface, foreground=p.muted, font=f.small)
    s.configure("Hint.TLabel", background=p.surface, foreground=p.muted, font=f.small)
    for kind, color in STATUS_COLORS.items():
        s.configure(status_style(kind), background=p.surface_alt, foreground=color,
                    font=f.ui_bold, padding=(12, 5))

    # inputs
    s.configure("TEntry", fieldbackground=p.surface_alt, foreground=p.text,
                insertcolor=p.text, padding=(8, 6), **_flat(p.border))
    s.map("TEntry", bordercolor=[("focus", p.accent)], lightcolor=[("focus", p.accent)])

    s.configure("TCombobox", fieldbackground=p.surface_alt, background=p.surface_alt,
                foreground=p.text, arrowcolor=p.muted, padding=(8, 5), **_flat(p.border))
    s.map("TCombobox",
          fieldbackground=[("readonly", p.surface_alt)],
          foreground=[("readonly", p.text)],
          selectbackground=[("readonly", p.surface_alt)],
          selectforeground=[("readonly", p.text)],
          background=[("active", p.border)],
          arrowcolor=[("active", p.text)],
          bordercolor=[("focus", p.accent)])
    root.option_add("*TCombobox*Listbox.background", p.surface_alt)
    root.option_add("*TCombobox*Listbox.foreground", p.text)
    root.option_add("*TCombobox*Listbox.selectBackground", p.accent)
    root.option_add("*TCombobox*Listbox.selectForeground", "#ffffff")
    root.option_add("*TCombobox*Listbox.font", f.ui)

    s.configure("TCheckbutton", background=p.surface, foreground=p.text,
                indicatorbackground=p.surface_alt, indicatorforeground="#ffffff",
                upperbordercolor=p.border, lowerbordercolor=p.border, padding=(2, 6))
    s.map("TCheckbutton",
          background=[("active", p.surface)],
          indicatorbackground=[("selected", p.accent), ("active", p.border)])

    # buttons
    s.configure("TButton", background=p.surface_alt, foreground=p.text,
                padding=(16, 7), relief="flat", focusthickness=0, **_flat(p.border))
    s.map("TButton", background=[("pressed", p.border), ("active", p.border)],
          bordercolor=[("focus", p.accent)])
    for name, base, hover, press in (("Accent", p.accent, p.accent_hover, p.accent_press),
                                     ("Danger", p.danger, p.danger_hover, p.danger_press)):
        s.configure(f"{name}.TButton", background=base, foreground="#ffffff",
                    font=f.ui_bold, **_flat(base))
        states = [("pressed", press), ("active", hover)]
        s.map(f"{name}.TButton", background=states, bordercolor=states,
              lightcolor=states, darkcolor=states)
    s.configure("Ghost.TButton", background=p.surface, foreground=p.muted,
                padding=(10, 3), font=f.small, **_flat(p.surface))
    s.map("Ghost.TButton", background=[("active", p.surface_alt)],
          foreground=[("active", p.text)], bordercolor=[("active", p.border)])

    # notebook
    s.configure("TNotebook", background=p.bg, borderwidth=0, tabmargins=(0, 0, 0, 0))
    s.configure("TNotebook.Tab", background=p.bg, foreground=p.muted, padding=(22, 9),
                font=f.ui_bold, **_flat(p.bg))
    s.map("TNotebook.Tab",
          background=[("selected", p.surface), ("active", p.surface)],
          foreground=[("selected", p.text), ("active", p.text)],
          lightcolor=[("selected", p.accent)],
          bordercolor=[("selected", p.border)])

    # treeview
    row_h = tkfont.Font(root=root, font=f.ui).metrics("linespace") + 14
    s.configure("Treeview", background=p.surface_alt, fieldbackground=p.surface_alt,
                foreground=p.text, rowheight=row_h, borderwidth=0, **_flat(p.border))
    s.map("Treeview", background=[("selected", p.select)], foreground=[("selected", p.text)])
    s.configure("Treeview.Heading", background=p.surface, foreground=p.muted,
                font=f.small, relief="flat", padding=(8, 6), **_flat(p.surface))
    s.map("Treeview.Heading", background=[("active", p.surface_alt)])

    # scrollbars
    for orient in ("Vertical", "Horizontal"):
        s.configure(f"{orient}.TScrollbar", background=p.border, troughcolor=p.surface_alt,
                    arrowcolor=p.muted, gripcount=0, arrowsize=11, relief="flat",
                    bordercolor=p.surface_alt, lightcolor=p.border, darkcolor=p.border)
        s.map(f"{orient}.TScrollbar", background=[("active", p.muted)])
    return f


# --------------------------------------------------------------------------
#  Widget helpers
# --------------------------------------------------------------------------
def card(parent: tk.Misc, title: str,
         action: tuple[str, Callable[[], None]] | None = None) -> tuple[ttk.Frame, ttk.Frame]:
    """Create a bordered card. Returns (outer, body); the caller lays out `outer`."""
    outer = ttk.Frame(parent, style="CardOuter.TFrame", padding=(16, 12, 16, 14))
    head = ttk.Frame(outer, style="Card.TFrame")
    head.pack(fill="x", pady=(0, 10))
    ttk.Label(head, text=title.upper(), style="CardTitle.TLabel").pack(side="left")
    if action:
        text, command = action
        ttk.Button(head, text=text, style="Ghost.TButton", command=command).pack(side="right")
    body = ttk.Frame(outer, style="Card.TFrame")
    body.pack(fill="both", expand=True)
    return outer, body


def style_text(widget: tk.Text, fonts: Fonts) -> None:
    """Apply the dark palette to a classic tk.Text widget."""
    p = PALETTE
    widget.configure(font=fonts.mono, bg=p.surface_alt, fg=p.text,
                     insertbackground=p.accent, selectbackground=p.select,
                     selectforeground=p.text, relief="flat", borderwidth=0,
                     highlightthickness=1, highlightbackground=p.border,
                     highlightcolor=p.accent, padx=10, pady=8)


def log_view(parent: tk.Misc, fonts: Fonts, height: int,
             tag_colors: dict[str, str]) -> tk.Text:
    """Read-only, scrollable, color-tagged log. A 'ts' tag is always available."""
    frame = ttk.Frame(parent, style="Card.TFrame")
    frame.pack(fill="both", expand=True)
    text = tk.Text(frame, height=height, wrap="none", state="disabled")
    style_text(text, fonts)
    vbar = ttk.Scrollbar(frame, orient="vertical", command=text.yview)
    hbar = ttk.Scrollbar(frame, orient="horizontal", command=text.xview)
    text.configure(yscrollcommand=vbar.set, xscrollcommand=hbar.set)
    text.grid(row=0, column=0, sticky="nsew")
    vbar.grid(row=0, column=1, sticky="ns")
    hbar.grid(row=1, column=0, sticky="ew")
    frame.rowconfigure(0, weight=1)
    frame.columnconfigure(0, weight=1)
    for tag, color in {"ts": PALETTE.muted, **tag_colors}.items():
        text.tag_configure(tag, foreground=color)
    return text


def palette_colors() -> list[str]:
    """All palette values — handy for validation/tests."""
    return [getattr(PALETTE, fld.name) for fld in fields(PALETTE)]
