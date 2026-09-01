# sicim

**Durable agent runtime** — uzun süren (LLM agent) workflow'ları için deterministik replay, checkpoint'ten devam ve saga-style compensation. Çekirdek sıfır bağımlılık; depolama SQLite (WAL), PostgreSQL (`sicim[postgres]`) veya bellek içi.

Bir agent workflow'u saatlerce sürebilir, pahalı LLM çağrıları yapar, insan onayı bekler ve süreç her an ölebilir. sicim'in vaadi: **süreç çökse bile workflow kaldığı yerden devam eder; tamamlanmış hiçbir adım (hiçbir LLM çağrısı) tekrar çalıştırılmaz; başarısızlıkta yan etkiler saga telafileriyle geri alınır.**

## Nasıl çalışır?

Temel ilke: **kod, yapının kaynağıdır; journal, sonuçların kaynağıdır.**

- Her run'ın append-only bir **journal**'ı (olay günlüğü) vardır. Adım sonuçları, timer'lar, sinyaller ve deterministik değerler (`now/random/uuid`) buraya yazılır. Journal aynı zamanda checkpoint'tir — ayrıca snapshot alınmaz.
- Devam ederken workflow fonksiyonu **baştan** çalıştırılır (deterministik replay). Journal'da sonucu kayıtlı her operasyon *çalıştırılmadan* kayıttan döner; ilk kayıtsız operasyondan itibaren canlı yürütmeye geçilir.
- Kod ile journal uyuşmazsa (deploy arası kod değişmişse) `NonDeterminismError` fırlatılır ve run **dokunulmadan** bırakılır: kodu düzeltip aynı run'ı yeniden devam ettirebilirsiniz.

## Kurulum

```bash
python -m venv .venv && .venv/bin/pip install -e '.[dev]'   # Python >= 3.11
```

> Not: Proje iCloud'a senkronlanan bir dizindeyse (Desktop/Documents), macOS senkron
> servisi venv dosyalarına `hidden` bayrağı basabilir ve Python 3.13 gizli `.pth`
> dosyalarını atladığı için editable kurulum kırılır. Çözüm: venv'i dışarıda tutun —
> `python -m venv ~/.venvs/sicim && ln -s ~/.venvs/sicim .venv`.

## Hızlı başlangıç

```python
import asyncio
import sicim

async def charge_payment(order_id, idempotency_key):
    ...  # ödeme API'si — sync fonksiyonlar da olur (thread'de koşar)
    return {"charge_id": "ch_1"}

async def refund_payment(order_id):
    ...

async def reserve_stock(order_id): ...
async def release_stock(order_id): ...
async def ship(order_id): ...

@sicim.workflow
async def order_flow(ctx: sicim.WorkflowContext, order_id: str):
    # Journal'lanan uuid: retry/replay'de aynı kalır -> güvenli idempotency anahtarı
    key = await ctx.uuid4()

    await ctx.step(
        charge_payment, order_id, key,
        compensate=refund_payment, compensate_args=(order_id,),
        retry=sicim.RetryPolicy(max_attempts=5, initial_interval=1.0),
    )
    await ctx.step(
        reserve_stock, order_id,
        compensate=release_stock, compensate_args=(order_id,),
    )
    await ctx.step(ship, order_id)   # burada patlarsa: release_stock, sonra refund_payment (LIFO)
    return {"order": order_id, "status": "shipped"}

async def main():
    rt = sicim.Runtime(sicim.SQLiteStore("sicim.db"))
    handle = await rt.start(order_flow, "order-42", run_id="order-42")
    print(await handle.result())

asyncio.run(main())
```

Süreç ölürse — yeni süreçte:

```python
rt = sicim.Runtime(sicim.SQLiteStore("sicim.db"))
handles = await rt.recover()        # yarım kalan tüm run'lar replay edilip devam eder
```

## Kavramlar

### Adımlar (`ctx.step`)

Tüm gerçek iş (I/O, LLM çağrısı, API isteği) adımlarda yaşar. Bir adım **en-az-bir-kez** çalışır; JSON-serileştirilebilir dönüş değeri journal'lanır ve replay'de asla yeniden hesaplanmaz. Canlı yürütmede bile dönüş değeri JSON'dan geçirilerek verilir — ilk çalıştırmada gördüğünüz, replay'de göreceğinizin aynısıdır (ör. tuple → list).

- Adım *argümanları* journal'lanmaz (replay'de kod yeniden sağlar), bu yüzden adımlara canlı nesneler (LLM istemcisi, bağlantı) geçebilirsiniz.
- Sync fonksiyonlar otomatik olarak worker thread'de çalışır.
- `retry=RetryPolicy(...)` üstel backoff'la dener; başarısız denemeler journal'landığı için deneme sayacı çökmelerden etkilenmez. `sicim.NonRetryable` fırlatan (veya `non_retryable` listesindeki) hatalar anında `StepFailed` olur.
- `timeout=` tek denemeyi sınırlar.

### Saga telafileri

```python
await ctx.step(reserve, compensate=release, compensate_args=(...,))
await ctx.add_compensation(cleanup, path)   # adıma bağlı olmayan telafi
```

Workflow yakalanmamış bir hatayla düşerse (veya iptal edilirse) kayıtlı telafiler **LIFO** sırayla çalışır. Telafi yürütmesi de journal'lanır: telafinin ortasında çökerseniz, devamında tamamlanmış telafiler atlanır, kalanlar koşar. Bir telafi kalıcı olarak başarısız olursa kalanlar yine denenir ve run, elle müdahale için `COMPENSATION_FAILED` durumuna geçer.

### Zamanlayıcılar ve sinyaller

```python
await ctx.sleep(3600)                                  # dayanıklı uyku
payload = await ctx.wait_event("approval", timeout=86400)  # insan onayı bekle
```

`sleep` son tarihi journal'lar: uykunun ortasında çöken run, devam ettiğinde yalnızca *kalan* süreyi bekler; süresi zaten geçmişse anında uyanır. `wait_event`, `rt.signal(run_id, "approval", {...})` ile beslenir; run kapalıyken gelen sinyaller store'da kuyruklanır ve `signal()` kalıcı bir run'ı otomatik uyandırır. `timeout` dolarsa `sicim.WaitTimeout` fırlatılır.

### Deterministik değerler

Workflow gövdesinde `time.time()`, `random`, `uuid` **kullanmayın** — bunların journal'lanan karşılıklarını kullanın:

```python
t = await ctx.now()        # replay'de orijinal gözlemlenen zaman döner
r = await ctx.random()
key = await ctx.uuid4()    # idempotency anahtarı için ideal
```

### Paralellik

```python
a, b, c = await ctx.gather(ctx.step(f1), ctx.step(f2), ctx.step(f3))
```

Operasyon kimlikleri `ctx.step(...)` *çağrıldığı anda* (senkron, kod sırasıyla) atandığı için paralel dallar replay'de de kararlıdır.

### Child workflow'lar

```python
@sicim.workflow
async def research_agent(ctx, question):
    plan = await ctx.step(llm, "plan", question)
    reports = await ctx.gather(
        *(ctx.child(sub_agent, topic) for topic in plan["topics"])
    )
    return await ctx.step(llm, "synthesize", reports)
```

`ctx.child(wf, ...)` kayıtlı başka bir workflow'u **kendi journal'ına sahip bağımsız bir run** olarak başlatır ve sonucunu bekler — agent/sub-agent hiyerarşileri için. Çocuğun run id'si deterministiktir (`<parent>.c<op>`), bu yüzden çökme sonrası replay çocuğu yeniden başlatmaz, mevcut run'a yeniden bağlanır; ebeveyn ve çocuk bağımsız devam eder. Çocuğun terminal hatası ebeveynde `sicim.ChildFailed` fırlatır (yakalanabilir); çocuğu beklerken ebeveyn iptal edilirse çocuk da iptal edilir ve kendi telafilerini koşar. `compensate=` adımlardaki gibi çalışır.

### Çoklu worker ve lease

Her `Runtime` bir *worker*'dır: bir run'ı sürmeden önce onun kirasını (lease) alır ve `lease_ttl / 3` aralıklı heartbeat ile yeniler. Aynı store'u birden çok worker güvenle paylaşır:

- `recover()` başka worker'ın kiraladığı run'ları çalmaz, atlar; `start`/`resume` ise `LeaseUnavailable` fırlatır.
- Bir worker ölürse kiraları TTL sonunda düşer ve run'ları başka worker tarafından devralınır; `shutdown()` kiraları hemen bırakır (hızlı devir).
- Başka worker'ın sürdüğü run'a `signal()` gönderilebilir: sinyal store'a kuyruklanır, bekleyen run `signal_poll_interval` (varsayılan 1 sn) içinde alır. `cancel()` de aynı şekilde çalışır: bayrak store'a yazılır, süren worker heartbeat'inde görüp kooperatif iptali başlatır.

```python
rt = sicim.Runtime(store, worker_id="api-1", lease_ttl=30.0, signal_poll_interval=1.0)
```

### Versiyonlama

Uçuştaki run'ları kırmadan workflow kodunu evrimleştirmek için versiyon sabitleme:

```python
@sicim.workflow(name="order_flow", version=2)
async def order_flow(ctx, order_id):
    if ctx.version >= 2:
        await ctx.step(new_fraud_check, order_id)   # yalnız yeni run'lar
    ...
```

Versiyon run başlarken kayda yazılır ve **run ömrü boyunca sabittir**: v1 ile başlamış bir run, v2 kodu deploy edildikten sonra devam ettirildiğinde `ctx.version == 1` görür ve eski dalı izler — journal'la uyum bozulmaz. Eski dalları, o versiyondaki tüm run'lar bitince silebilirsiniz.

### Continue-as-new: sonsuz döngülü agent'lar

Journal append-only olduğu için sonsuza dek dönen bir agent'ın journal'ı sınırsız büyür. Çözüm, run'ı periyodik olarak taze bir run'a zincirlemek:

```python
@sicim.workflow
async def agent_loop(ctx, state):
    for _ in range(100):                       # run başına sınırlı iterasyon
        state = await ctx.step(do_work, state)
        if state.get("done"):
            return state
    await ctx.continue_as_new(state)           # asla geri dönmez
```

`continue_as_new(*args)` mevcut run'ı `CONTINUED` durumuyla bitirir ve aynı workflow'un **boş journal'lı** yeni bir run'ını (`run#2`, `run#3`, …) başlatır — taşımak istediğiniz durumu argümanlarda taşırsınız. `handle.result()` zinciri sonuna kadar şeffafça takip eder; `signal()`/`cancel()` zincirdeki herhangi bir id ile çağrıldığında canlı run'a yönlenir. Ardıl run, *o an kayıtlı* workflow versiyonunu sabitler — yani continue noktası aynı zamanda doğal yükseltme noktasıdır. Kayıtlı telafiler ardıla taşınmaz (continue, başarılı dönüş gibi sayılır). Zincirin eski halkaları `prune` ile silinebilir.

### PostgreSQL store

```bash
pip install 'sicim[postgres]'
```

```python
from sicim.pg import PostgresStore
store = await PostgresStore.connect("postgresql://user@host/db")
rt = sicim.Runtime(store, worker_id="api-1")
```

Şema ilk bağlantıda oluşturulur. Lease'lerle birlikte bu backend, aynı veritabanını paylaşan **birden çok worker sürecinin** hedeflenen kurulumudur. Test paketi, PostgreSQL kuruluysa tüm senaryoları geçici bir yerel cluster'a karşı da koşar (`SICIM_PG_DSN` ile mevcut bir sunucuya yönlendirilebilir).

### Gözlemlenebilirlik

```python
rt = sicim.Runtime(store, on_event=lambda run_id, event: metrics.emit(run_id, event.kind))
```

Her journal append'inde çağrılır; hook'un fırlattığı hatalar loglanır ve run'ı asla etkilemez.

**OpenTelemetry** (`pip install 'sicim[otel]'`): hazır bir observer, journal olaylarını span'lere çevirir — run başına bir üst span, tamamlanan her adım/timer/bekleme/çocuk/telafi için journal zaman damgalarını taşıyan bir alt span; hatalar ERROR statüsüyle işaretlenir. Replay span üretmez (replay journal'a yazmaz).

```python
from sicim.otel import otel_observer
rt = sicim.Runtime(store, on_event=otel_observer())
```

### İptal

`rt.cancel(run_id)` **kooperatiftir**: run bir sonraki *canlı* operasyon sınırında durur (asla replay'in veya bir adımın ortasında değil), telafilerini koşar ve `CANCELLED` biter. Uçuştaki bir adımın bitmesi beklenir — sınırlamak için adım `timeout=`'u kullanın.

### Çökme modeli

`rt.shutdown()` bilinçli olarak **çökme-eşdeğeridir**: sürücü task'ları durdurur, run durumlarına dokunmaz. Yarım run'lar `RUNNING` kalır ve `recover()` ile devam eder. Bu sayede "SIGTERM aldım" ile "elektrik kesildi" aynı (test edilen) yoldan geçer.

## Determinizm kuralları

Workflow **gövdesi** için (adımlar için değil):

1. I/O yok, ağ yok, dosya yok — hepsi adımlarda.
2. `time`/`random`/`uuid` yerine `ctx.now()/ctx.random()/ctx.uuid4()`.
3. Operasyon sırasını etkileyen sırasız koleksiyon iterasyonu yok (`set` üzerinde döngüyle `ctx.step` çağırmayın).
4. Kod değişikliği yarım run'ları etkileyebilir: uzun ömürlü workflow'lara `@workflow(name=...)` ile sabit isim verin ve akış değişikliklerini yeni run'lara uygulayın. Uyuşmazlık `NonDeterminismError` ile yakalanır ve run zarar görmez.

## API özeti

| Öğe | Ne yapar |
|---|---|
| `@sicim.workflow` / `@sicim.workflow(name=..., version=N)` | `async def` fonksiyonu workflow olarak kaydeder; versiyon run'a sabitlenir |
| `Runtime(store, default_retry=, worker_id=, lease_ttl=, signal_poll_interval=, on_event=)` | Worker; `InMemoryStore()` varsayılan |
| `rt.start(wf, *args, run_id=...)` | Run başlatır (`run_id` üzerinde idempotent) → `RunHandle` |
| `rt.recover()` / `rt.resume(run_id)` | Yarım run'ları replay edip devam ettirir (kiralılar: atla / `LeaseUnavailable`) |
| `rt.signal(run_id, name, payload)` | Olay teslim eder (gerekirse run'ı uyandırır; worker'lar arası çalışır) |
| `rt.cancel(run_id)` | Kooperatif iptal + telafiler (worker'lar arası çalışır) |
| `rt.status(run_id)` / `rt.events(run_id)` | Run kaydı / journal |
| `rt.shutdown()` | Çökme-eşdeğeri durdurma (kiraları bırakır) |
| `await handle` / `handle.result()` | Sonuç, ya da `WorkflowFailed` / `WorkflowCancelled` / `CompensationFailed` / `NonDeterminismError` |
| `ctx.step / child / sleep / wait_event / now / random / uuid4 / gather / add_compensation / continue_as_new / log / is_replaying / version` | Workflow içi API |

Run durumları: `RUNNING → COMPLETED | FAILED | CANCELLED | COMPENSATION_FAILED | CONTINUED`.

## CLI

```bash
python -m sicim --db sicim.db list
python -m sicim --db sicim.db show <run_id>            # run kaydı + journal + sinyaller
python -m sicim --db sicim.db prune --older-than-days 30 --dry-run
```

`prune`, verilen eşikten eski terminal run'ları (journal'larıyla birlikte) siler; `RUNNING` ve insan müdahalesi bekleyen `COMPENSATION_FAILED` run'lara asla dokunmaz. Tüm komutlar `--db` yerine `--pg DSN` ile PostgreSQL'e karşı da çalışır.

## Örnekler ve test

```bash
.venv/bin/python examples/order_saga.py      # saga + telafi + çökmeden devam
.venv/bin/python examples/agent_research.py  # LLM agent: paralel araçlar, insan onayı, çökme
.venv/bin/pytest -q                          # tüm senaryolar iki backend'de de koşar
```

## v0.3 kısıtları ve yol haritası

- Sinyal, var olmayan run'a gönderilemez (önce `start`).
- Retry backoff *bekleyişi* journal'lanmaz (deneme sayısı journal'lanır); çökme sonrası sıradaki deneme hemen yapılır.
- Lease devralma TTL çözünürlüğündedir: ölen worker'ın run'ı en fazla `lease_ttl` sonra devralınır.
- Ebeveyn *başarısız olduğunda* (iptal değil), o an `gather` içinde koşan çocuklar bağımsız devam eder — gerekirse telafide `cancel` edin.
- PostgresStore worker süreci başına tek bağlantı kullanır; kopan bağlantıyı yeniden kurmaz (süreci yeniden başlatın, `recover()` devralır).
- Yol haritası: sinyal aboneliği (poll yerine LISTEN/NOTIFY), otomatik zincir budama, bağlantı havuzu/yeniden bağlanma, dağıtık trace bağlamı (ebeveyn-çocuk span köprüsü).
