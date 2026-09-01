"""Sipariş sagası ve çökmeden devam demosu.

1. bölüm — saga: ödeme → stok → kargo. Kargo kalıcı olarak başarısız olur ve
   telafiler LIFO sırayla koşar (önce stok bırakılır, sonra ödeme iade edilir).

2. bölüm — checkpoint: paketleme akışı adım ortasında "çöker"
   (``Runtime.shutdown()`` çökme-eşdeğeridir). Yeni bir Runtime aynı veritabanı
   üzerinden run'ı replay edip kaldığı yerden bitirir; tamamlanmış adımlar
   tekrar çalışmaz.

Çalıştır:  .venv/bin/python examples/order_saga.py
"""

import asyncio
import os
import tempfile

import sicim

# --- Sahte dış sistemler -----------------------------------------------------

payments: dict[str, dict] = {}
stock = {"widget": 5}


async def charge_payment(order_id, idempotency_key):
    payments[order_id] = {"key": idempotency_key, "amount": 100}
    print(f"  [adım]   ödeme alındı        ({order_id}, key={idempotency_key[:8]}…)")
    return {"charge_id": f"ch_{order_id}"}


async def refund_payment(order_id):
    payments.pop(order_id, None)
    print(f"  [telafi] ödeme iade edildi   ({order_id})")


async def reserve_stock(order_id):
    stock["widget"] -= 1
    print(f"  [adım]   stok rezerve edildi (kalan: {stock['widget']})")
    return {"reserved": 1}


async def release_stock(order_id):
    stock["widget"] += 1
    print(f"  [telafi] stok bırakıldı      (kalan: {stock['widget']})")


async def ship(order_id):
    raise sicim.NonRetryable("kargo firması bu adrese teslimat yapmıyor")


@sicim.workflow
async def order_flow(ctx: sicim.WorkflowContext, order_id: str):
    # Journal'lanan uuid: retry/replay'de aynı kalır -> güvenli idempotency anahtarı.
    key = await ctx.uuid4()
    await ctx.step(charge_payment, order_id, key,
                   compensate=refund_payment, compensate_args=(order_id,))
    await ctx.step(reserve_stock, order_id,
                   compensate=release_stock, compensate_args=(order_id,))
    await ctx.step(ship, order_id)
    return {"order": order_id, "status": "shipped"}


# --- 2. bölüm için paketleme akışı -------------------------------------------

step_runs = {"hazirla": 0, "paketle": 0, "etiketle": 0}


def slow_step(name, seconds):
    async def step():
        step_runs[name] += 1
        await asyncio.sleep(seconds)
        return name

    step.__name__ = name
    return step


@sicim.workflow
async def packing_flow(ctx: sicim.WorkflowContext, order_id: str):
    a = await ctx.step(slow_step("hazirla", 0.1))
    b = await ctx.step(slow_step("paketle", 0.5))
    c = await ctx.step(slow_step("etiketle", 0.1))
    return [a, b, c]


async def main():
    db = os.path.join(tempfile.mkdtemp(prefix="sicim-"), "orders.db")
    store = sicim.SQLiteStore(db)

    print("=== 1) Saga: kargo başarısız -> telafiler LIFO koşar ===")
    rt = sicim.Runtime(store)
    handle = await rt.start(order_flow, "order-42", run_id="order-42")
    try:
        await handle.result()
    except sicim.WorkflowFailed as exc:
        print(f"  sonuç: {exc}")
    record = await rt.status("order-42")
    print(f"  run durumu: {record.status.value}  |  ödemeler: {payments}  |  stok: {stock}")

    print("\n=== 2) Checkpoint: adım ortasında çökme ve devam ===")
    rt1 = sicim.Runtime(store)
    await rt1.start(packing_flow, "order-43", run_id="order-43")
    await asyncio.sleep(0.25)  # 'hazirla' bitti, 'paketle' uçuşta
    await rt1.shutdown()
    print(f"  süreç çöktü… (adım koşuları: {step_runs})")

    rt2 = sicim.Runtime(store)
    [resumed] = await rt2.recover()
    result = await resumed.result()
    print(f"  devam etti ve bitti: {result}")
    print(f"  adım koşuları: {step_runs}")
    print("  -> 'hazirla' journal'dan geldi (1 koşu), yarım kalan 'paketle' tekrar koştu (2).")

    print("\n=== order-42 journal'ı ===")
    for event in await rt2.events("order-42"):
        detail = event.payload.get("name") or event.payload.get("error", {}).get("message", "")
        print(f"  [{event.seq:>2}] {event.kind:<20} {detail}")

    print(f"\nİncelemek için: .venv/bin/python -m sicim --db {db} show order-42")
    store.close()


if __name__ == "__main__":
    asyncio.run(main())
