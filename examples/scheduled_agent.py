"""Zamanlanmış agent: aralıkla tetiklenen, çakışmayan koşular.

``rt.schedule(...)`` bir workflow'u aralıkla (``every=``), cron ile (``cron=``)
veya tek seferlik (``at=``) başlatır. Demo: 0,5 sn'lik aralık, ama her koşu
~0,8 sn sürer — varsayılan ``overlap="skip"`` ile önceki koşu bitmeden yeni
tick başlatılmaz (atlanan tick'ler loglanır). Koşular
``sicim.schedule=<id>`` etiketiyle aranabilir.

Çalıştır:  .venv/bin/python examples/scheduled_agent.py
"""

import asyncio
import logging
import os
import tempfile

import sicim

logging.basicConfig(level=logging.INFO, format="%(message)s")


async def check_inbox(mailbox: str):
    await asyncio.sleep(0.8)  # yavaş bir araç çağrısı
    return f"{mailbox}: 3 yeni mesaj"


@sicim.workflow
async def inbox_agent(ctx: sicim.WorkflowContext, mailbox: str):
    summary = await ctx.step(check_inbox, mailbox)
    ctx.log("özet -> %s", summary)
    return summary


async def main():
    db = os.path.join(tempfile.mkdtemp(prefix="sicim-"), "sched.db")
    store = sicim.SQLiteStore(db)
    rt = sicim.Runtime(store, schedule_poll_interval=0.1)

    await rt.schedule(
        inbox_agent, "destek@ornek.dev",
        schedule_id="inbox", every=0.5, tags={"team": "destek"},
    )
    await asyncio.sleep(3.0)
    await rt.pause_schedule("inbox")

    runs = await rt.list_runs(tags={"sicim.schedule": "inbox"})
    for run in runs:  # son koşunun bitmesini bekle
        await (await rt.resume(run.run_id)).result()
    print(f"\n{len(runs)} koşu başladı (aralık 0,5 sn, koşu süresi 0,8 sn -> çakışan tick'ler atlandı):")
    for run in await rt.list_runs(tags={"sicim.schedule": "inbox"}):
        print(f"  {run.run_id}  {run.status.value}  {run.tags}")
    print(f"\nİncelemek için: .venv/bin/python -m sicim --db {db} schedule list")
    await rt.shutdown()
    store.close()


if __name__ == "__main__":
    asyncio.run(main())
