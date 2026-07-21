# Copyright (c) 2025 devgagan : https://github.com/devgajanin.
# Licensed under the GNU General Public License v3.0.
# See LICENSE file in the repository root for full license text.

"""
FloodWait Scheduler — pauses batch on FloodWait, schedules automatic resume.

When FloodWait is caught in the batch loop, instead of sleeping inline
(which blocks the entire event loop and prevents /stop from working),
this scheduler:

1. Records the pause state + resume time
2. Cancels the running batch task
3. Schedules an asyncio.Task that sleeps for the wait duration
4. After the timer fires, calls resume_fn which re-enters the batch

Since MongoDB already tracks last_uploaded_source_id, the resumed batch
automatically skips already-uploaded messages — zero data loss.

Usage:
    from scheduler import scheduler

    # At batch start:
    scheduler.register(uid, resume_fn=_resume_batch, ...)

    # On FloodWait:
    await scheduler.handle_flood_wait(uid, wait_seconds, current_task)

    # At batch completion or /stop:
    scheduler.unregister(uid)
"""

import asyncio
import time
import logging
from dataclasses import dataclass, field
from typing import Optional, Callable, Any, Dict

log = logging.getLogger(__name__)


@dataclass
class BatchJob:
    """Represents a batch job that can be paused and resumed."""
    uid: int
    resume_fn: Callable               # async fn to call on resume — restarts the batch
    resume_args: tuple = ()           # positional args for resume_fn
    resume_kwargs: dict = field(default_factory=dict)  # keyword args for resume_fn
    flood_wait_until: float = 0       # epoch seconds when flood clears
    wait_seconds: int = 0             # original FloodWait duration
    paused: bool = False              # True while waiting for FloodWait to clear
    task: Optional[asyncio.Task] = None  # the resume timer task
    user_chat_id: Optional[int] = None   # for sending status messages


class FloodWaitScheduler:
    """
    Detects FloodWait, pauses the active batch, waits the exact duration,
    then automatically resumes from last_uploaded_source_id via MongoDB.

    The scheduler does NOT own the batch state — it only manages pause/resume
    timing. All persistent state (upload maps, progress) stays in MongoDB and
    ACTIVE_USERS as before.
    """

    def __init__(self):
        self._jobs: Dict[int, BatchJob] = {}           # uid -> BatchJob
        self._resume_tasks: Dict[int, asyncio.Task] = {}  # uid -> resume timer task

    def register(self, uid: int, resume_fn: Callable,
                 resume_args: tuple = (), resume_kwargs: dict = None):
        """
        Call this at batch start so the scheduler knows how to resume.

        resume_fn should be the same coroutine that starts/resumes the batch.
        It will be called with resume_args and resume_kwargs after FloodWait clears.

        The function should handle its own resume detection via MongoDB
        (last_uploaded_source_id) to skip already-uploaded messages.
        """
        if resume_kwargs is None:
            resume_kwargs = {}
        self._jobs[uid] = BatchJob(
            uid=uid,
            resume_fn=resume_fn,
            resume_args=resume_args,
            resume_kwargs=resume_kwargs,
        )
        log.info(f"[SCHEDULER] Registered job uid={uid}")

    def unregister(self, uid: int):
        """Call at batch completion or /stop — removes job and cancels any pending resume."""
        self._jobs.pop(uid, None)
        self._cancel_resume_task(uid)
        log.info(f"[SCHEDULER] Unregistered job uid={uid}")

    async def handle_flood_wait(self, uid: int, wait_seconds: int,
                                 current_task: asyncio.Task = None):
        """
        Called when FloodWait is caught in the batch loop.

        Stores pause state, cancels the running task, schedules auto-resume.
        The batch loop should `return` after calling this — the scheduler
        takes over and will call resume_fn after the wait period.

        Args:
            uid: User ID
            wait_seconds: FloodWait duration from the exception
            current_task: The asyncio.Task running the batch (to cancel it)
        """
        job = self._jobs.get(uid)
        if not job:
            log.warning(f"[SCHEDULER] FloodWait for unknown uid={uid} — "
                        f"no job registered, sleeping inline as fallback")
            await asyncio.sleep(wait_seconds + 3)
            return

        buffer = 3  # extra seconds buffer on top of Telegram's wait
        job.wait_seconds = wait_seconds
        job.flood_wait_until = time.time() + wait_seconds + buffer
        job.paused = True

        wait_mins = wait_seconds // 60
        wait_secs = wait_seconds % 60
        duration_str = f"{wait_mins}m {wait_secs}s" if wait_mins > 0 else f"{wait_seconds}s"

        log.warning(
            f"[SCHEDULER] uid={uid} hit FloodWait={duration_str} — "
            f"pausing batch, will auto-resume after {duration_str} + {buffer}s buffer"
        )

        # Notify user that batch is paused
        if job.user_chat_id:
            try:
                from shared_client import app as X
                await X.send_message(
                    job.user_chat_id,
                    f"⏳ **Batch paused — Flood Wait {duration_str}**\n\n"
                    f"Will resume automatically when the wait clears.\n"
                    f"Use /stop to cancel permanently."
                )
            except Exception as e:
                log.warning(f"[SCHEDULER] Could not notify uid={uid}: {e}")

        # Cancel the running batch task — it will exit via CancelledError
        if current_task and not current_task.done():
            log.info(f"[SCHEDULER] uid={uid} — cancelling running batch task")
            current_task.cancel()

        # Schedule the resume timer
        self._cancel_resume_task(uid)  # clear any old resume timer
        self._resume_tasks[uid] = asyncio.create_task(
            self._resume_after_wait(uid, wait_seconds + buffer)
        )

    async def _resume_after_wait(self, uid: int, delay: float):
        """Sleeps for the flood wait duration, then calls resume_fn."""
        log.info(f"[SCHEDULER] uid={uid} — sleeping {delay:.0f}s before auto-resume")

        # Sleep in chunks so we can be cancelled by /stop at any time
        try:
            remaining = delay
            while remaining > 0:
                chunk = min(remaining, 60)  # wake every 60s to check if still active
                await asyncio.sleep(chunk)
                remaining -= chunk

                # If job was removed (user stopped), abort
                if uid not in self._jobs:
                    log.info(f"[SCHEDULER] uid={uid} — job removed during sleep, aborting resume")
                    return
        except asyncio.CancelledError:
            log.info(f"[SCHEDULER] uid={uid} — resume timer cancelled (user stopped)")
            return

        job = self._jobs.get(uid)
        if not job:
            log.info(f"[SCHEDULER] uid={uid} — job gone before resume (cancelled?)")
            return

        if not job.paused:
            log.info(f"[SCHEDULER] uid={uid} — job no longer paused, skipping resume")
            return

        job.paused = False
        log.info(f"[SCHEDULER] uid={uid} — FloodWait cleared, resuming batch automatically")

        # Notify user
        if job.user_chat_id:
            try:
                from shared_client import app as X
                await X.send_message(
                    job.user_chat_id,
                    "✅ **Flood wait cleared — resuming batch automatically...**"
                )
            except Exception as e:
                log.warning(f"[SCHEDULER] Could not notify uid={uid} on resume: {e}")

        try:
            # resume_fn is expected to re-enter the batch from last_uploaded_source_id
            await job.resume_fn(*job.resume_args, **job.resume_kwargs)
        except asyncio.CancelledError:
            log.info(f"[SCHEDULER] uid={uid} — resume task cancelled (user stopped)")
        except Exception as e:
            log.error(f"[SCHEDULER] uid={uid} — resume_fn raised: {e}", exc_info=True)
            # Try to notify user of the failure
            if job.user_chat_id:
                try:
                    from shared_client import app as X
                    await X.send_message(
                        job.user_chat_id,
                        f"❌ **Batch resume failed**: {str(e)[:200]}\n\n"
                        f"Use /batch to restart manually."
                    )
                except Exception:
                    pass

    def _cancel_resume_task(self, uid: int):
        """Cancel any pending resume timer for this user."""
        t = self._resume_tasks.pop(uid, None)
        if t and not t.done():
            t.cancel()
            log.info(f"[SCHEDULER] uid={uid} — cancelled pending resume timer")

    def is_paused(self, uid: int) -> bool:
        """Check if a batch is currently paused for FloodWait."""
        job = self._jobs.get(uid)
        return job.paused if job else False

    def time_remaining(self, uid: int) -> int:
        """Seconds remaining on flood wait (for status messages)."""
        job = self._jobs.get(uid)
        if not job or not job.paused:
            return 0
        return max(0, int(job.flood_wait_until - time.time()))

    def get_job(self, uid: int) -> Optional[BatchJob]:
        """Get the BatchJob for a user, if any."""
        return self._jobs.get(uid)

    @property
    def active_count(self) -> int:
        """Number of currently registered jobs."""
        return len(self._jobs)

    @property
    def paused_count(self) -> int:
        """Number of currently paused jobs."""
        return sum(1 for job in self._jobs.values() if job.paused)


# Singleton instance — import this from other modules
scheduler = FloodWaitScheduler()
