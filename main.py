from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import uvicorn


# -----------------------------
# Models
# -----------------------------
class TVEvent(BaseModel):
    event_index: int = 0
    event: str
    engine: Optional[str] = None
    symbol: Optional[str] = None
    tf: Optional[str] = None
    bar_index: Optional[int] = None
    ts: Optional[int] = None
    id: Optional[str] = None
    side: Optional[str] = None
    entry: Optional[float] = None
    sl: Optional[float] = None
    tp: Optional[float] = None
    expiry_bar: Optional[int] = None
    stage: Optional[str] = None
    reason: Optional[str] = None


class TVBatch(BaseModel):
    schema_version: Optional[str] = None
    batch_id: str
    engine: str
    symbol: str
    tf: str
    bar_index: int
    ts: int
    live_mode: Optional[bool] = None
    position_state: Optional[str] = None
    position_id: Optional[str] = None
    position_entry: Optional[float] = None
    position_stop: Optional[float] = None
    position_tp: Optional[float] = None
    position_moved_to_be: Optional[bool] = None
    live_pending_count: Optional[int] = None
    shadow_pending_count: Optional[int] = None
    pending_longs: Optional[int] = None
    pending_shorts: Optional[int] = None
    events_count: Optional[int] = None
    events: List[TVEvent] = Field(default_factory=list)


# -----------------------------
# App / state
# -----------------------------
app = FastAPI(title="LINK Receiver Stub v2.3")

MAX_ACTIONS = 500
MAX_BATCH_IDS = 1000
KEEP_SHADOW_REASONS = {"filled_other_order_hold_shadow"}
PLACEHOLDER_PREFIX = "__placeholder__"

STATE: Dict[str, Any] = {}
ACTIONS: List[Dict[str, Any]] = []
SEEN_BATCH_IDS: List[str] = []
SEEN_BATCH_LOOKUP = set()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def reset_state(clear_actions: bool = True) -> None:
    global STATE, ACTIONS, SEEN_BATCH_IDS, SEEN_BATCH_LOOKUP
    STATE = {
        "last_batch": None,
        "last_received_at": None,
        "position": {
            "state": "unknown",
            "id": None,
            "entry": None,
            "stop": None,
            "tp": None,
            "moved_to_be": False,
            "side": None,
        },
        "live_pending": {},
        "shadow_pending": {},
        "meta": {
            "schema_version": None,
            "engine": None,
            "symbol": None,
            "tf": None,
            "bar_index": None,
            "ts": None,
            "live_mode": None,
            "events_count": 0,
            "live_pending_count": 0,
            "shadow_pending_count": 0,
            "pending_longs": 0,
            "pending_shorts": 0,
            "tracked_live_pending_count": 0,
            "tracked_shadow_pending_count": 0,
            "tracked_pending_longs": 0,
            "tracked_pending_shorts": 0,
            "explicit_live_pending_count": 0,
            "explicit_shadow_pending_count": 0,
            "explicit_pending_longs": 0,
            "explicit_pending_shorts": 0,
            "placeholder_live_pending_count": 0,
            "placeholder_shadow_pending_count": 0,
            "placeholder_pending_longs": 0,
            "placeholder_pending_shorts": 0,
            "counts_aligned": True,
            "recent_batch_ids": [],
        },
    }
    if clear_actions:
        ACTIONS = []
    SEEN_BATCH_IDS = []
    SEEN_BATCH_LOOKUP = set()


reset_state(clear_actions=True)


def log_action(kind: str, message: str, payload: Optional[Dict[str, Any]] = None) -> None:
    ACTIONS.append(
        {
            "at": utc_now_iso(),
            "kind": kind,
            "message": message,
            "payload": payload or {},
        }
    )
    if len(ACTIONS) > MAX_ACTIONS:
        del ACTIONS[:-MAX_ACTIONS]


# -----------------------------
# Helpers
# -----------------------------
def add_seen_batch(batch_id: str) -> None:
    if batch_id in SEEN_BATCH_LOOKUP:
        return
    SEEN_BATCH_IDS.append(batch_id)
    SEEN_BATCH_LOOKUP.add(batch_id)
    if len(SEEN_BATCH_IDS) > MAX_BATCH_IDS:
        old = SEEN_BATCH_IDS.pop(0)
        SEEN_BATCH_LOOKUP.discard(old)



def event_to_dict(event: TVEvent) -> Dict[str, Any]:
    return event.dict()



def pending_snapshot(event: TVEvent) -> Dict[str, Any]:
    return {
        "id": event.id,
        "side": event.side,
        "entry": event.entry,
        "sl": event.sl,
        "tp": event.tp,
        "expiry_bar": event.expiry_bar,
        "stage": event.stage,
        "reason": event.reason,
        "bar_index": event.bar_index,
        "ts": event.ts,
        "placeholder": False,
    }



def refresh_meta_from_batch(batch: TVBatch) -> None:
    STATE["last_batch"] = batch.batch_id
    STATE["last_received_at"] = utc_now_iso()
    meta = STATE["meta"]
    meta["schema_version"] = batch.schema_version
    meta["engine"] = batch.engine
    meta["symbol"] = batch.symbol
    meta["tf"] = batch.tf
    meta["bar_index"] = batch.bar_index
    meta["ts"] = batch.ts
    meta["live_mode"] = batch.live_mode
    meta["events_count"] = batch.events_count or len(batch.events)
    meta["live_pending_count"] = batch.live_pending_count or 0
    meta["shadow_pending_count"] = batch.shadow_pending_count or 0
    meta["pending_longs"] = batch.pending_longs or 0
    meta["pending_shorts"] = batch.pending_shorts or 0
    add_seen_batch(batch.batch_id)
    meta["recent_batch_ids"] = SEEN_BATCH_IDS[-20:]



def sync_position_from_batch(batch: TVBatch) -> None:
    pos = STATE["position"]
    pos["state"] = batch.position_state or pos["state"]
    pos["id"] = batch.position_id
    pos["entry"] = batch.position_entry
    pos["stop"] = batch.position_stop
    pos["tp"] = batch.position_tp
    pos["moved_to_be"] = bool(batch.position_moved_to_be)

    if batch.position_state in ("long", "short"):
        pos["side"] = batch.position_state
    elif batch.position_state == "flat":
        pos["side"] = None



def ensure_shadow_snapshot(event: TVEvent) -> None:
    if event.id:
        STATE["shadow_pending"][event.id] = pending_snapshot(event)



def is_placeholder(item: Dict[str, Any]) -> bool:
    return bool(item.get("placeholder"))


def normalize_pending_buckets() -> List[str]:
    """Ensure the same pending id cannot exist in both live and shadow buckets.

    Shadow takes precedence because a hold-in-shadow transition is the only
    valid case we've observed when duplicates appear.
    """
    overlaps = sorted(set(STATE["live_pending"].keys()) & set(STATE["shadow_pending"].keys()))
    for pid in overlaps:
        STATE["live_pending"].pop(pid, None)
    return overlaps



def remove_all_placeholders() -> int:
    removed = 0
    for bucket_name in ("live_pending", "shadow_pending"):
        bucket = STATE[bucket_name]
        for key in list(bucket.keys()):
            if is_placeholder(bucket[key]):
                del bucket[key]
                removed += 1
    return removed



def collect_counts(include_placeholders: bool) -> Dict[str, int]:
    live_items = list(STATE["live_pending"].values())
    shadow_items = list(STATE["shadow_pending"].values())
    if not include_placeholders:
        live_items = [item for item in live_items if not is_placeholder(item)]
        shadow_items = [item for item in shadow_items if not is_placeholder(item)]

    live_long = sum(1 for item in live_items if item.get("side") == "long")
    live_short = sum(1 for item in live_items if item.get("side") == "short")
    shadow_long = sum(1 for item in shadow_items if item.get("side") == "long")
    shadow_short = sum(1 for item in shadow_items if item.get("side") == "short")

    return {
        "live_total": len(live_items),
        "shadow_total": len(shadow_items),
        "live_long": live_long,
        "live_short": live_short,
        "shadow_long": shadow_long,
        "shadow_short": shadow_short,
        "total_long": live_long + shadow_long,
        "total_short": live_short + shadow_short,
    }



def make_placeholder(stage: str, side: str, idx: int, meta: Dict[str, Any]) -> Dict[str, Any]:
    pid = f"{PLACEHOLDER_PREFIX}:{stage}:{side}:{idx}"
    return {
        "id": pid,
        "side": side,
        "entry": None,
        "sl": None,
        "tp": None,
        "expiry_bar": None,
        "stage": stage,
        "reason": "meta_placeholder",
        "bar_index": meta.get("bar_index"),
        "ts": meta.get("ts"),
        "placeholder": True,
    }



def rebuild_placeholders_from_meta() -> Tuple[int, bool]:
    removed = remove_all_placeholders()
    meta = STATE["meta"]
    explicit = collect_counts(include_placeholders=False)

    live_target = meta.get("live_pending_count", 0)
    shadow_target = meta.get("shadow_pending_count", 0)
    long_target = meta.get("pending_longs", 0)
    short_target = meta.get("pending_shorts", 0)

    live_def = live_target - explicit["live_total"]
    shadow_def = shadow_target - explicit["shadow_total"]
    long_def = long_target - explicit["total_long"]
    short_def = short_target - explicit["total_short"]

    if min(live_def, shadow_def, long_def, short_def) < 0:
        return removed, False

    live_long_add = min(live_def, long_def)
    live_short_add = live_def - live_long_add
    shadow_long_add = long_def - live_long_add
    shadow_short_add = short_def - live_short_add

    if min(live_short_add, shadow_long_add, shadow_short_add) < 0:
        return removed, False

    if shadow_long_add + shadow_short_add != shadow_def:
        return removed, False

    if live_long_add + shadow_long_add != long_def:
        return removed, False

    if live_short_add + shadow_short_add != short_def:
        return removed, False

    idx = 1
    for _ in range(live_long_add):
        ph = make_placeholder("live", "long", idx, meta)
        STATE["live_pending"][ph["id"]] = ph
        idx += 1
    for _ in range(live_short_add):
        ph = make_placeholder("live", "short", idx, meta)
        STATE["live_pending"][ph["id"]] = ph
        idx += 1
    for _ in range(shadow_long_add):
        ph = make_placeholder("shadow", "long", idx, meta)
        STATE["shadow_pending"][ph["id"]] = ph
        idx += 1
    for _ in range(shadow_short_add):
        ph = make_placeholder("shadow", "short", idx, meta)
        STATE["shadow_pending"][ph["id"]] = ph
        idx += 1

    created = live_long_add + live_short_add + shadow_long_add + shadow_short_add
    return created + removed, True



def refresh_tracked_counts() -> None:
    explicit = collect_counts(include_placeholders=False)
    tracked = collect_counts(include_placeholders=True)

    meta = STATE["meta"]
    meta["explicit_live_pending_count"] = explicit["live_total"]
    meta["explicit_shadow_pending_count"] = explicit["shadow_total"]
    meta["explicit_pending_longs"] = explicit["total_long"]
    meta["explicit_pending_shorts"] = explicit["total_short"]

    meta["tracked_live_pending_count"] = tracked["live_total"]
    meta["tracked_shadow_pending_count"] = tracked["shadow_total"]
    meta["tracked_pending_longs"] = tracked["total_long"]
    meta["tracked_pending_shorts"] = tracked["total_short"]

    meta["placeholder_live_pending_count"] = tracked["live_total"] - explicit["live_total"]
    meta["placeholder_shadow_pending_count"] = tracked["shadow_total"] - explicit["shadow_total"]
    meta["placeholder_pending_longs"] = tracked["total_long"] - explicit["total_long"]
    meta["placeholder_pending_shorts"] = tracked["total_short"] - explicit["total_short"]

    meta["counts_aligned"] = (
        meta.get("live_pending_count", 0) == tracked["live_total"]
        and meta.get("shadow_pending_count", 0) == tracked["shadow_total"]
        and meta.get("pending_longs", 0) == tracked["total_long"]
        and meta.get("pending_shorts", 0) == tracked["total_short"]
    )



def reconcile_pending_to_meta() -> None:
    touched, ok = rebuild_placeholders_from_meta()
    refresh_tracked_counts()
    if touched and ok:
        log_action(
            "pending_reconciled_to_meta",
            "Reconciled tracked pending counts to Pine meta using placeholders when needed.",
            {
                "touched_entries": touched,
                "live_pending_count": STATE["meta"].get("live_pending_count"),
                "shadow_pending_count": STATE["meta"].get("shadow_pending_count"),
                "pending_longs": STATE["meta"].get("pending_longs"),
                "pending_shorts": STATE["meta"].get("pending_shorts"),
                "placeholder_live_pending_count": STATE["meta"].get("placeholder_live_pending_count"),
                "placeholder_shadow_pending_count": STATE["meta"].get("placeholder_shadow_pending_count"),
            },
        )
    elif not ok:
        log_action(
            "pending_reconcile_failed",
            "Could not fully reconcile pending counts to Pine meta. Explicit state likely started mid-stream or is stale.",
            {
                "live_pending_count": STATE["meta"].get("live_pending_count"),
                "shadow_pending_count": STATE["meta"].get("shadow_pending_count"),
                "pending_longs": STATE["meta"].get("pending_longs"),
                "pending_shorts": STATE["meta"].get("pending_shorts"),
                "explicit_live_pending_count": STATE["meta"].get("explicit_live_pending_count"),
                "explicit_shadow_pending_count": STATE["meta"].get("explicit_shadow_pending_count"),
                "explicit_pending_longs": STATE["meta"].get("explicit_pending_longs"),
                "explicit_pending_shorts": STATE["meta"].get("explicit_pending_shorts"),
            },
        )



def handle_upsert_pending(event: TVEvent) -> None:
    snapshot = pending_snapshot(event)
    if event.stage == "shadow":
        STATE["shadow_pending"][event.id] = snapshot
        STATE["live_pending"].pop(event.id, None)
        log_action(
            "shadow_upserted",
            "Updated shadow pending order. No exchange action.",
            event_to_dict(event),
        )
        return

    STATE["live_pending"][event.id] = snapshot
    if event.id in STATE["shadow_pending"]:
        del STATE["shadow_pending"][event.id]
    log_action(
        "would_place_pending",
        "Would place/update pending order on exchange.",
        event_to_dict(event),
    )



def handle_cancel_pending(event: TVEvent) -> None:
    payload = event_to_dict(event)

    if event.stage == "shadow":
        if event.reason in KEEP_SHADOW_REASONS:
            ensure_shadow_snapshot(event)
            removed_live = STATE["live_pending"].pop(event.id, None)
            payload["kept_in_shadow"] = True
            payload["live_duplicate_removed"] = bool(removed_live)
            log_action(
                "shadow_kept",
                "Shadow pending remains stored after other order filled.",
                payload,
            )
            return

        removed_shadow = STATE["shadow_pending"].pop(event.id, None)
        payload["removed_from"] = "shadow" if removed_shadow else "shadow_assumed"
        log_action(
            "shadow_cancelled",
            "Removed shadow pending order. No exchange cancel required.",
            payload,
        )
        return

    removed_live = STATE["live_pending"].pop(event.id, None)
    payload["removed_from"] = "live" if removed_live else "live_assumed"
    log_action(
        "would_cancel_pending",
        "Would cancel pending order on exchange.",
        payload,
    )



def handle_shadow_store(event: TVEvent) -> None:
    STATE["shadow_pending"][event.id] = pending_snapshot(event)
    STATE["live_pending"].pop(event.id, None)
    log_action(
        "stored_in_shadow",
        "Stored pending order in shadow state. No exchange action.",
        event_to_dict(event),
    )



def handle_shadow_drop(event: TVEvent) -> None:
    STATE["shadow_pending"].pop(event.id, None)
    log_action(
        "dropped_from_shadow",
        "Dropped shadow pending order.",
        event_to_dict(event),
    )



def handle_move_stop(event: TVEvent) -> None:
    pos = STATE["position"]
    if pos.get("id") == event.id or pos.get("state") in ("long", "short"):
        pos["stop"] = event.sl
        if event.reason == "be_activated":
            pos["moved_to_be"] = True
    log_action(
        "would_move_stop",
        "Would move stop on exchange.",
        event_to_dict(event),
    )



def handle_entry_fill_expected(event: TVEvent) -> None:
    pos = STATE["position"]
    pos["state"] = event.side or pos.get("state")
    pos["side"] = event.side
    pos["id"] = event.id
    pos["entry"] = event.entry
    pos["stop"] = event.sl
    pos["tp"] = event.tp
    pos["moved_to_be"] = False
    STATE["live_pending"].pop(event.id, None)
    STATE["shadow_pending"].pop(event.id, None)
    log_action(
        "position_marked_open",
        "Marked position as open from expected fill.",
        event_to_dict(event),
    )



def handle_position_exit_expected(event: TVEvent) -> None:
    STATE["position"] = {
        "state": "flat",
        "id": None,
        "entry": None,
        "stop": None,
        "tp": None,
        "moved_to_be": False,
        "side": None,
    }
    log_action(
        "position_marked_closed",
        "Marked position as closed from expected exit.",
        event_to_dict(event),
    )



def route_event(event: TVEvent) -> None:
    if event.event == "upsert_pending":
        handle_upsert_pending(event)
    elif event.event == "cancel_pending":
        handle_cancel_pending(event)
    elif event.event == "shadow_store":
        handle_shadow_store(event)
    elif event.event == "shadow_drop":
        handle_shadow_drop(event)
    elif event.event == "move_stop":
        handle_move_stop(event)
    elif event.event == "entry_fill_expected":
        handle_entry_fill_expected(event)
    elif event.event == "position_exit_expected":
        handle_position_exit_expected(event)
    else:
        log_action(
            "event_unhandled",
            "Received event with no handler.",
            event_to_dict(event),
        )


# -----------------------------
# Routes
# -----------------------------
@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "status": "ok",
        "service": "link_receiver_stub_v2_3",
        "time": utc_now_iso(),
    }


@app.get("/state")
def get_state() -> Dict[str, Any]:
    return STATE


@app.get("/actions")
def get_actions() -> Dict[str, Any]:
    return {"count": len(ACTIONS), "items": ACTIONS}


@app.post("/reset")
def reset_endpoint() -> Dict[str, Any]:
    reset_state(clear_actions=True)
    log_action("state_reset", "State and actions were reset.", {})
    return {"status": "ok", "message": "state reset"}


@app.post("/webhook/tradingview")
def tradingview_webhook(batch: TVBatch) -> Dict[str, Any]:
    if batch.batch_id in SEEN_BATCH_LOOKUP:
        log_action(
            "duplicate_batch_ignored",
            "Ignored duplicate TradingView batch.",
            {
                "batch_id": batch.batch_id,
                "events_count": batch.events_count or len(batch.events),
                "schema_version": batch.schema_version,
            },
        )
        return {
            "status": "duplicate_ignored",
            "batch_id": batch.batch_id,
            "events_received": batch.events_count or len(batch.events),
        }

    refresh_meta_from_batch(batch)
    sync_position_from_batch(batch)

    log_action(
        "batch_received",
        "Received TradingView batch.",
        {
            "batch_id": batch.batch_id,
            "events_count": batch.events_count or len(batch.events),
            "schema_version": batch.schema_version,
        },
    )

    for event in batch.events:
        route_event(event)

    duplicate_ids = normalize_pending_buckets()
    if duplicate_ids:
        log_action(
            "pending_bucket_deduped",
            "Removed duplicate pending ids from live bucket because matching shadow ids were present.",
            {"ids": duplicate_ids},
        )

    reconcile_pending_to_meta()

    return {
        "status": "accepted",
        "batch_id": batch.batch_id,
        "events_received": batch.events_count or len(batch.events),
    }


@app.exception_handler(Exception)
def generic_exception_handler(_, exc: Exception):
    raise HTTPException(status_code=500, detail=str(exc))


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=False)
