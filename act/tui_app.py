#!/usr/bin/env python3
"""
TallyPrime → SQL Server Sync (Phase 1 - Console UI)
- Textual-based TUI replacing input() prompts
"""

from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal
from textual.widgets import Header, Footer, Button, Static, SelectionList, Log
from textual import events
import asyncio
import logging
import main_sync  # <-- uses your simp.py logic (now renamed)

class SyncApp(App):
    CSS_PATH = None
    TITLE = "Tally → SQL Server Sync"
    SUB_TITLE = "Phase 1: Console UI"

    def __init__(self):
        super().__init__()
        self.conn = None
        self.selected_masters = []

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Container(
            Static("Select Run Mode:", id="title"),
            Horizontal(
                Button("1️⃣  Interactive Run", id="interactive", variant="primary"),
                Button("2️⃣  Run Once (All Masters)", id="runonce", variant="success"),
                Button("3️⃣  Scheduler", id="scheduler", variant="warning"),
            ),
            id="menu",
        )
        yield Log(id="log")
        yield Footer()

    async def on_button_pressed(self, event: Button.Pressed):
        log = self.query_one("#log", Log)
        if event.button.id == "interactive":
            log.write("Starting Interactive Mode...")
            await self.run_interactive()
        elif event.button.id == "runonce":
            log.write("Running full sync (all masters)...")
            await self.run_once()
        elif event.button.id == "scheduler":
            log.write("Scheduler mode not yet interactive — launching old mode...")
            main_sync.run_scheduler()

    async def run_interactive(self):
        self.query_one("#menu").remove()
        masters = list(main_sync.MASTERS.keys())

        selection = SelectionList[str]((m, m) for m in masters)
        self.mount(Static("Select Masters to Sync (Press Enter to Start):"))
        self.mount(selection)

        async for msg in selection.messages:
            if isinstance(msg, events.Key) and msg.key == "enter":
                self.selected_masters = [v for v, _ in selection.selected]
                self.query_one("#log").write(f"Selected: {self.selected_masters}")
                await self.sync_selected()
                break

    async def sync_selected(self):
        log = self.query_one("#log", Log)
        conn = main_sync.connect_sql_default()
        for master in self.selected_masters:
            log.write(f"🔄 Fetching {master} ...")
            df = main_sync.fetch_master(master, main_sync.MASTERS[master])
            log.write(f"{master}: Parsed {len(df)} rows")
            main_sync.upsert_dataframe(df, master, conn)
            log.write(f"{master}: ✅ Synced successfully\n")
            await asyncio.sleep(0.1)
        conn.close()
        log.write("✅ Interactive sync complete.")

    async def run_once(self):
        await asyncio.to_thread(main_sync.run_once_all)
        self.query_one("#log").write("✅ Run once complete.")

if __name__ == "__main__":
    app = SyncApp()
    app.run()
