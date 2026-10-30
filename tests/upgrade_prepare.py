"""Bounded setup of an upgrade witness after confirmed admission refusals.

This is fixture orchestration, not a general transaction retry facility.
Transport errors and every response except the three confirmed Vinyl
admission NOs escape immediately, retaining the unfinished owned fixture.
"""
import time


ADMISSION_MESSAGES = {
    "Vinyl writes are throttled while dump or compaction catches up",
    "Vinyl write exceeds the current pressure burst allowance",
    "Vinyl pressure statistics are unavailable",
}


def prepare_witness(base, nodes, prepare, abort_and_verify, event, *,
                    seconds=45, attempts=32, now=time.monotonic, sleep=time.sleep):
    assert nodes and seconds > 0 and attempts > 0
    deadline = now() + seconds

    def remaining():
        left = deadline - now()
        if left <= 0:
            raise TimeoutError("prepared witness admission deadline exceeded")
        return min(12, left)

    for attempt in range(attempts):
        remaining()
        txid = base if attempt == 0 else f"{base}-admission-{attempt}"
        event("prepare_attempt_started", txid=txid, attempt=attempt, nodes=nodes)
        try:
            for node in nodes:
                result = prepare(node, txid, remaining())
                event("prepare_attempt_reply", txid=txid, node=node, result=result)
                reply = result["reply"]
                if reply == ["yes"] and result["state"] == "prepared":
                    continue
                if (isinstance(reply, list) and len(reply) == 3 and reply[:2] == ["no", 1205]
                        and reply[2] in ADMISSION_MESSAGES and result["state"] == "aborted"):
                    break
                raise AssertionError(f"unexpected prepared-witness response: {result!r}")
            else:
                remaining()
                event("durable_YES_before_restart", txid=txid)
                return txid
            # A NO is terminal for this identity. Fence the decision, abort
            # every participant (including those not contacted), and prove
            # absent effects/locks before admitting a different identity.
            event("prepare_refusal_cleanup_started", txid=txid)
            evidence = abort_and_verify(txid, remaining)
            event("prepare_refusal_cleanup_verified", txid=txid, evidence=evidence)
            if attempt + 1 < attempts:
                sleep(min(.05, remaining()))
        except BaseException as error:
            event("prepare_attempt_failed", txid=txid, error=repr(error))
            raise
    raise TimeoutError("prepared witness admission attempt limit exceeded")
