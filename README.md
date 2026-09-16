# sicim

**Durable agent runtime** — uzun süren (LLM agent) workflow'ları için deterministik replay, checkpoint'ten devam ve saga-style compensation. Çekirdek sıfır bağımlılık (zamanlayıcı, çoklu worker ve web izleme arayüzü dahil); depolama SQLite (WAL), PostgreSQL (`sicim[postgres]`) veya bellek içi.

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
- `retry=RetryPolicy(...)` üstel backoff'la dener; başarısız denemeler *ve bir sonraki denemenin zamanı* journal'landığı için ne deneme sayacı ne de backoff çökmelerden etkilenir: backoff'un ortasında çöken run, devam ettiğinde yalnızca kalan süreyi bekler (süre zaten geçmişse hemen dener). `sicim.NonRetryable` fırlatan (veya `non_retryable` listesindeki) hatalar anında `StepFailed` olur.
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

### Etiketler ve arama

```python
await rt.start(order_flow, "order-42", run_id="order-42", tags={"customer": "42", "env": "prod"})

runs = await rt.list_runs(tags={"customer": "42"})                        # verilen tüm çiftler eşleşmeli
runs = await rt.list_runs(sicim.RunStatus.RUNNING, workflow="order_flow")
runs = await rt.list_runs(parent_run_id="arge-1")                          # bir run'ın çocukları
runs = await rt.list_runs(limit=20, newest_first=True)
```

Etiketler düz `str -> str` çiftleridir: run kaydında ve `RUN_STARTED` olayında taşınır, workflow gövdesinde `ctx.tags` ile okunur. Çocuk workflow'lar ebeveynin etiketlerini devralır (`ctx.child(..., tags=...)` ile geçersiz kılınır), continue-as-new ardılları da taşır — böylece bir müşteriye ait tüm agent ağacı tek sorguyla bulunur. SQL backend'lerinde etiketler ayrı, indeksli bir tabloda tutulur. `sicim.` önekli anahtarlar runtime'a ayrılmıştır (`sicim.schedule`, aşağıda); kullanıcı API'leri bunları reddeder.

**Sonradan etiketleme.** Etiketler run başladıktan sonra da değiştirilebilir, iki yoldan:

```python
await rt.tag("order-42", {"priority": "high", "env": None})   # dışarıdan: ekle/değiştir, None siler
await ctx.tag({"stage": "review"})                              # workflow gövdesinden: journal'lanır
```

`rt.tag()` her durumdaki run'da çalışır (biten bir run'ı "incelendi" diye işaretlemek gibi), zinciri canlı run'a kadar takip eder ve sonradan yaratılan ardıllar etiketi taşır; ama workflow gövdesi bunu **görmez** — `ctx.tags` deterministik kalmalıdır. `ctx.tag()` ise bir operasyondur: canlı yürütmede store güncellenir ve `TAGS_UPDATED` olayı yazılır, replay'de yalnızca journal'dan uygulanır; `ctx.tags` başlangıçta sabitlenen küme + bu güncellemelerdir. Bir agent'ın kendi aşamasını etiketlemesi (`stage=plan → research → review`) ve arayüzden bu etikete göre süzülmesi tipik kullanımdır. CLI: `python -m sicim --db sicim.db tag order-42 priority=high --remove env`.

### Çoklu worker ve lease

Her `Runtime` bir *worker*'dır: bir run'ı sürmeden önce onun kirasını (lease) alır ve `lease_ttl / 3` aralıklı heartbeat ile yeniler. Aynı store'u birden çok worker güvenle paylaşır:

- `recover()` başka worker'ın kiraladığı run'ları çalmaz, atlar; `start`/`resume` ise `LeaseUnavailable` fırlatır.
- Bir worker ölürse kiraları TTL sonunda düşer ve run'ları başka worker tarafından devralınır; `shutdown()` kiraları hemen bırakır (hızlı devir).
- Başka worker'ın sürdüğü run'a `signal()` gönderilebilir: sinyal store'a kuyruklanır, bekleyen run `signal_poll_interval` (varsayılan 1 sn) içinde alır. `cancel()` de aynı şekilde çalışır: bayrak store'a yazılır, süren worker heartbeat'inde görüp kooperatif iptali başlatır.
- Store bir **push kanalı** sunuyorsa (PostgreSQL LISTEN/NOTIFY), süren worker ilk run'ında otomatik abone olur: başka worker'ın gönderdiği sinyal ve iptal, poll'ü veya heartbeat'i beklemeden anında ulaşır. Poll/heartbeat güvenlik ağı olarak kalır — dinleyici bağlantısı kopsa bile doğruluk bozulmaz, sadece gecikme poll aralığına döner.
- Çökme sonrası bir çocuk run'ı veya continue-as-new ardılını *başka* worker devralmışsa, ebeveyn (`ctx.child`) ve `handle.result()` o run'ı store üzerinden izleyip sonucunu alır; `LeaseUnavailable` ile düşmez.
- Zamanlamalar da worker'lar arasında paylaşılır; her tick'i yalnız bir worker tetikler (bkz. *Zamanlanmış başlatma*).
- `recover_interval=` verilen worker, `recover()`'ı arka planda periyodik olarak yineler: ölen worker'ın run'ları kiraları düşer düşmez otomatik devralınır, kimsenin yeniden `recover()` çağırması gerekmez. Verilmezse devralma yalnız açık `recover()` çağrısıyla olur.
- Workflow kodu yüklü olmayan bir süreç (izleme arayüzü, yalnız sinyal gönderen istemci) run'ın kirasını asla almaz: `signal()`/`cancel()` isteği store'a yazılır, run'ı süren (veya bir sonraki devralan) worker uygular; `recover()` kodu olmayan workflow'ların run'larını uyararak atlar.

```python
rt = sicim.Runtime(store, worker_id="api-1", lease_ttl=30.0, signal_poll_interval=1.0, recover_interval=30.0)
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

`continue_as_new(*args)` mevcut run'ı `CONTINUED` durumuyla bitirir ve aynı workflow'un **boş journal'lı** yeni bir run'ını (`run#2`, `run#3`, …) başlatır — taşımak istediğiniz durumu argümanlarda taşırsınız. `handle.result()` zinciri sonuna kadar şeffafça takip eder; `signal()`/`cancel()` zincirdeki herhangi bir id ile çağrıldığında canlı run'a yönlenir. Ardıl run, *o an kayıtlı* workflow versiyonunu sabitler — yani continue noktası aynı zamanda doğal yükseltme noktasıdır. Kayıtlı telafiler ardıla taşınmaz (continue, başarılı dönüş gibi sayılır).

**Otomatik zincir budama** — `Runtime(store, chain_keep=1)`: her continue'da canlı run'ın gerisindeki en yeni `chain_keep` halka journal'ını korur, daha eski halkaların journal'ı ve sinyal kutusu silinir. Run kayıtlarının kendisi (birkaç yüz bayt) yönlendirme için yerinde kalır: eski id'lerle `signal()`/`cancel()`/`result()` çalışmaya devam eder. Sonsuz döngülü bir agent'ın depolamasını sınırlayan budur; `chain_keep=None` (varsayılan) hiçbir şeye dokunmaz. Kayıtları tamamen silmek için `prune` CLI komutu duruyor.

### Zamanlanmış başlatma

```python
await rt.schedule(inbox_agent, "destek@ornek.dev", schedule_id="inbox", every="5m")         # 300 veya timedelta da olur
await rt.schedule(nightly_report, schedule_id="rapor", cron="0 2 * * *", tz="Europe/Istanbul")
await rt.schedule(health_probe, schedule_id="nabiz", cron="*/10 * * * * *")                  # 6 alan: baştaki saniye
await rt.schedule(cleanup, schedule_id="tek-sefer", at=time.time() + 3600)                   # bir kez

await rt.pause_schedule("inbox"); await rt.resume_schedule("inbox"); await rt.unschedule("inbox")
runs = await rt.list_runs(tags={"sicim.schedule": "inbox"})   # bu zamanlamanın başlattığı run'lar
```

Bir zamanlama, workflow'un belirli anlarda **yeni bir run'ını başlatma** talimatıdır ve store'da yaşar. Üç tür vardır: `every=` (saniye, `timedelta` veya `"30s"`/`"5m"`/`"1h30m"` gibi bir süre dizgisi; önceki tetiklemeye hizalı, kaymaz), `cron=` (klasik beş alan — `*`, liste, aralık, adım, ay/gün adları ve `@daily` gibi kısayollar; başa altıncı bir alan eklenirse saniye hassasiyeti; `"@every 90s"` aralık kısayolu; `tz` verilmezse UTC) ve `at=` (tek seferlik; zaman damgası veya `datetime`).

- Her tick, `<schedule_id>@<zaman>` biçiminde **deterministik id'li** bir run başlatır; run'a `sicim.schedule=<id>` etiketi (artı `tags=` ile verdikleriniz) yazılır ve argümanlar her run'a aynen geçer.
- **Çoklu worker'da tam-bir-kez:** her worker (varsayılan `scheduler=True`) bir şey sürmeye başladığında `schedule_poll_interval` (varsayılan 1 sn) aralıklarla vadesi gelen zamanlamalara bakar. Aynı tick'i gören worker'lar aynı run id'sini türetir — ikinci `create` birincil anahtara takılır ve kaybeden mevcut run'a bağlanır — ve zamanlama, tetikleme zamanı üzerinde compare-and-set ile ilerletilir. Garantiyi koordinasyon değil veritabanı verir. Yalnız sinyal gönderen istemciler `scheduler=False` geçebilir.
- `overlap="skip"` (varsayılan): önceki run hâlâ çalışıyorsa tick atlanır (loglanır); `"allow"` her tick'te başlatır.
- **Kaçırılan tick'ler birleşir:** hiçbir worker ayakta değilken geçen tetiklemeler için tek bir telafi run'ı başlatılır, sonraki tetikleme *şimdiden* hesaplanır. `pause` sırasında kaçanlar `resume` ile atlanır.
- `schedule()` `schedule_id` üzerinde idempotenttir (mevcut zamanlama olduğu gibi döner); değiştirmek için `unschedule` + `schedule`. Workflow kodu yüklü olmayan bir worker tick'i ilerletmeden bırakır; kodu olan bir worker tetikler.

### PostgreSQL store

```bash
pip install 'sicim[postgres]'
```

```python
from sicim.pg import PostgresStore
store = await PostgresStore.connect("postgresql://user@host/db")
rt = sicim.Runtime(store, worker_id="api-1")
```

Şema ilk bağlantıda oluşturulur. Lease'lerle birlikte bu backend, aynı veritabanını paylaşan **birden çok worker sürecinin** hedeflenen kurulumudur:

- **Yeniden bağlanma:** kopan bağlantı bir sonraki operasyonda şeffafça yeniden kurulur (sınırlı backoff'lu deneme). Denemeleri de aşan bir kesinti hata olarak yüzeye çıkar ve runtime bunu çökme-eşdeğeri sayar: run `RUNNING` kalır, sonraki `recover()` devam ettirir.
- **LISTEN/NOTIFY:** her `signal()` ve `cancel()` ortak kanala NOTIFY yazar; süren worker'lar adanmış bir bağlantıyla dinler ve poll beklenmeden uyanır. Dinleyici bağlantısı da kopunca kendini yeniden kurar; aradaki boşluğu poll kapatır.

Test paketi, PostgreSQL kuruluysa tüm senaryoları geçici bir yerel cluster'a karşı da koşar (`SICIM_PG_DSN` ile mevcut bir sunucuya yönlendirilebilir).

### Gözlemlenebilirlik

```python
rt = sicim.Runtime(store, on_event=lambda run_id, event: metrics.emit(run_id, event.kind))
```

Her journal append'inde çağrılır; hook'un fırlattığı hatalar loglanır ve run'ı asla etkilemez.

**OpenTelemetry** (`pip install 'sicim[otel]'`): hazır bir observer, journal olaylarını span'lere çevirir — run başına bir üst span, tamamlanan her adım/timer/bekleme/çocuk/telafi için journal zaman damgalarını taşıyan bir alt span; hatalar ERROR statüsüyle işaretlenir. Run etiketleri (başlangıçtakiler ve `ctx.tag()` güncellemeleri) run span'ine `sicim.tag.<anahtar>` attribute'ları olarak yazılır. Replay span üretmez (replay journal'a yazmaz).

Çocuk workflow'un run span'i, ebeveynin run span'inin **altına bağlanır** — agent/sub-agent ağacının tamamı tek trace olarak görünür — ve `sicim.parent_run_id` attribute'unu taşır (ebeveyn-çocuk bağı `RunRecord.parent_run_id` olarak store'a da yazılır; CLI `show` gösterir). Çökme sonrası çocuğu *başka bir süreç* devralırsa yeni span orijinal trace'e katılamaz; attribute üzerinden korelasyon orada da kalır.

```python
from sicim.otel import otel_observer
rt = sicim.Runtime(store, on_event=otel_observer())
```

### İzleme arayüzü

```bash
python -m sicim --db sicim.db ui                 # http://127.0.0.1:8787
python -m sicim --pg postgresql://… ui --port 9000
```

Sıfır bağımlılıklı, standart kütüphaneyle yazılmış bir web arayüzü: durum sayaçları ve durum/workflow/etiket/ebeveyn süzgeçli run listesi; run ayrıntısı (kayıt, etiketler, girdi/çıktı, kira, çocuklar, sinyaller ve tıklayınca payload'ı açılan journal); zamanlama görünümü. Arayüzden run iptal edilir, sinyal gönderilir, etiket eklenip silinir, run bir op'a geri sarılır (journal satırındaki ⟲ düğmesi ya da başlıktaki *Reset run*) ve zamanlama duraklatılır/silinir; sayfa iki saniyede bir kendini yeniler.

Bir worker'ın içine de gömülebilir — o zaman işlemler o worker üzerinden, süreç içinde yürür:

```python
from sicim.ui import start_ui
server = await start_ui(rt, port=8787)      # rt yerine çıplak bir store da verilebilir
...
await server.aclose()
```

Arkasındaki JSON API (`/api/summary`, `/api/runs?status=&workflow=&tag=k=v`, `/api/runs/<id>`, `/api/runs/<id>/cancel|reset|signal|tags`, `/api/schedules`, …) kendi araçlarınızdan da kullanılabilir; yol parçaları yüzde-kodlanır (`#` içeren run id'leri gibi). Kimlik doğrulama **yoktur**: varsayılan olarak yalnız localhost'a bağlanır; dışarı açacaksanız önüne kimlik doğrulayan bir proxy koyun. Kodu yüklü olmayan bir süreçten (`python -m sicim ui`) gönderilen sinyal ve iptal store'a yazılır, run'ı süren worker uygular.

### İptal

`rt.cancel(run_id)` **kooperatiftir**: run bir sonraki *canlı* operasyon sınırında durur (asla replay'in veya bir adımın ortasında değil), telafilerini koşar ve `CANCELLED` biter. Uçuştaki bir adımın bitmesi beklenir — sınırlamak için adım `timeout=`'u kullanın.

### Reset: bir op'tan yeniden oynatma

```python
await rt.reset("arge-1")                 # ilk başarısız op'a geri sar ve oradan devam et
await rt.reset("arge-1", to_op=7)        # 7. op'tan itibaren yeniden oynat
await rt.reset("arge-1", to_op=0)        # run'ı baştan
await rt.reset("arge-1", resume=False)   # yalnız geri sar; sürmeyi devralacak worker'a bırak
```

`reset`, journal'ın seçilen op'tan itibaren olan kısmını siler ve run'ı yeniden sürer: o op ve
sonrası **canlı** çalışır, öncesi journal'dan replay edilir (yeniden çalıştırılmaz). LLM'in bozuk
cevap verdiği adımı — ya da bu arada düzelttiğiniz bir hatayı — baştan başlamadan yeniden
denemenin yolu budur. `to_op` verilmezse ilk başarısız operasyona, hiç yoksa 0'a sarılır.

Her durumdaki run resetlenebilir: biten bir run yeniden açılır (durum `RUNNING`, sonuç ve hata
temizlenir) ve `resume=True` (varsayılan) ile hemen burada sürülür. Run'ı **başka** bir worker
sürüyorsa kirası alınamaz ve `LeaseUnavailable` fırlar; *bu* worker sürüyorsa sürücü önce
çökme-eşdeğeri durdurulur — eski `RunHandle` böylece emekliye ayrılır (beklemek `CancelledError`
verir), run'ı `reset()`'in döndürdüğü handle'dan (ya da `resume()`'dan) izleyin. Reset reddedilirse
durdurulan sürücü geri başlatılır: run bulunduğu hâlde bırakılır.

**Journal geri sarılır, dünya sarılmaz.** `to_op`'tan önceki adımlar tamamlanmış sayılır ve yan
etkileri yerinde kalır. İki sonucu vardır:

- Silinen op'larda başlatılmış **çocuk run'lar** (ve onların altındaki ağaç) silinir ki replay
  onları sıfırdan başlatsın; kiraları ebeveynin kirası gibi alınır.
- **Telafi kayıtları** da silinir, böylece geri sarılan run telafi yığınını yeniden kurar ve
  gerekirse yeniden telafi edebilir. Telafiler zaten koşmuşsa etkileri geri getirilmez ve onların
  geri aldığı adımlar journal'dan "yapılmış" gibi replay edilir; bu belirsizlik yüzünden o durumda
  `force=True` istenir.

Silinen `wait_event` op'larının tükettiği **sinyaller kutuya geri döner** ve replay'de yeniden
tüketilirler. Geri sarmanın kendisi `run_reset` olayı olarak journal'lanır ve sonraki resetlerde
de korunur — denetim izi olarak kalır.

Workflow kodu yüklü olmayan bir süreçten (CLI, izleme arayüzü) resetlenen run `RUNNING` bırakılır;
kodu olan worker `recover()` ile devralır. Bir çocuk run'ı tek başına resetlemek ebeveynin
journal'ındaki `child_completed` sonucunu değiştirmez — ağacı ebeveyninden resetleyin.

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
| `Runtime(store, default_retry=, worker_id=, lease_ttl=, signal_poll_interval=, chain_keep=, scheduler=, schedule_poll_interval=, recover_interval=, on_event=)` | Worker; `InMemoryStore()` varsayılan |
| `rt.start(wf, *args, run_id=..., tags=...)` | Run başlatır (`run_id` üzerinde idempotent) → `RunHandle` |
| `rt.recover()` / `rt.resume(run_id)` | Yarım run'ları replay edip devam ettirir (kiralılar: atla / `LeaseUnavailable`) |
| `rt.signal(run_id, name, payload)` | Olay teslim eder (gerekirse run'ı uyandırır; worker'lar arası çalışır) |
| `rt.cancel(run_id)` | Kooperatif iptal + telafiler (worker'lar arası çalışır) |
| `rt.reset(run_id, to_op=, resume=, force=)` | Journal'ı bir op'a geri sarar ve run'ı yeniden sürer |
| `rt.status(run_id)` / `rt.events(run_id)` | Run kaydı / journal |
| `rt.list_runs(status, workflow=, tags=, parent_run_id=, limit=, newest_first=)` | Run arama (tüm süzgeçler AND) |
| `rt.tag(run_id, {k: v, eski: None})` | Etiket ekler/değiştirir/siler (her durumda; zinciri takip eder) |
| `rt.list_workflows()` / `rt.count_runs()` | Workflow adları / duruma göre run sayıları |
| `rt.schedule(wf, *args, schedule_id=, every= \| cron= \| at=, tz=, tags=, overlap=)` | Zamanlama oluşturur → `ScheduleRecord` |
| `rt.get_schedule / list_schedules / pause_schedule / resume_schedule / unschedule` | Zamanlama yönetimi |
| `rt.shutdown()` | Çökme-eşdeğeri durdurma (kiraları bırakır) |
| `await handle` / `handle.result()` | Sonuç, ya da `WorkflowFailed` / `WorkflowCancelled` / `CompensationFailed` / `NonDeterminismError` |
| `ctx.step / child / sleep / wait_event / now / random / uuid4 / gather / add_compensation / continue_as_new / tag / log / is_replaying / version / tags` | Workflow içi API |
| `sicim.ui.start_ui(rt_veya_store, host=, port=)` | Web izleme arayüzü → `UIServer` (`url`, `aclose()`) |

Run durumları: `RUNNING → COMPLETED | FAILED | CANCELLED | COMPENSATION_FAILED | CONTINUED`.

## CLI

```bash
python -m sicim --db sicim.db list --status running --workflow order_flow --tag customer=42
python -m sicim --db sicim.db list --limit 20                  # en yeni 20 run
python -m sicim --db sicim.db show <run_id>                    # run kaydı + etiketler + journal + sinyaller
python -m sicim --db sicim.db tag <run_id> stage=review --remove env
python -m sicim --db sicim.db reset <run_id> [--to-op 7] [--force]   # geri sar; worker devralır
python -m sicim --db sicim.db prune --older-than-days 30 --dry-run
python -m sicim --db sicim.db schedule list
python -m sicim --db sicim.db schedule pause|resume|delete <schedule_id>
python -m sicim --db sicim.db ui --port 8787                   # web izleme arayüzü (Ctrl-C ile durur)
```

`list`, `--status`, `--workflow` ve tekrarlanabilir `--tag k=v` süzgeçlerinin hepsini birlikte uygular; `--limit N` en yeni N run'ı gösterir. `reset` journal'ı geri sarar ve run'ı `RUNNING` bırakır: CLI'da workflow kodu olmadığı için sürmeyi, kodu olan worker `recover()` ile devralır. `prune`, verilen eşikten eski terminal run'ları (journal'larıyla birlikte) siler; `RUNNING` ve insan müdahalesi bekleyen `COMPENSATION_FAILED` run'lara asla dokunmaz. Tüm komutlar `--db` yerine `--pg DSN` ile PostgreSQL'e karşı da çalışır.

## Örnekler ve test

```bash
.venv/bin/python examples/order_saga.py       # saga + telafi + çökmeden devam
.venv/bin/python examples/agent_research.py   # LLM agent: paralel araçlar, insan onayı, çökme
.venv/bin/python examples/scheduled_agent.py  # aralıkla tetiklenen agent, çakışan tick'ler atlanır
.venv/bin/python examples/reset_agent.py      # bozuk LLM adımını reset ile yeniden oynatma
.venv/bin/python examples/monitor_ui.py       # örnek run'lar + zamanlama ile web izleme arayüzü (Ctrl-C)
.venv/bin/pytest -q                           # tüm senaryolar üç backend'de de koşar
```

## v0.7 kısıtları ve yol haritası

- Sinyal, var olmayan run'a gönderilemez (önce `start`).
- Lease devralma TTL çözünürlüğündedir: ölen worker'ın run'ı en fazla `lease_ttl` sonra devralınır.
- Ebeveyn *başarısız olduğunda* (iptal değil), o an `gather` içinde koşan çocuklar bağımsız devam eder — gerekirse telafide `cancel` edin.
- Push kanalı yalnız PostgreSQL'de; SQLite/bellek store'larında worker'lar arası sinyal/iptal poll ve heartbeat ile taşınır (tek süreç içinde zaten anındadır).
- Çökme sonrası çocuğu başka süreç devralırsa span'i orijinal trace'e katılamaz; `sicim.parent_run_id` ile korelasyon kalır.
- Dışarıdan eklenen etiketler (`rt.tag`) workflow gövdesine yansımaz; gövdenin görmesi gereken etiketler `ctx.tag()` ile eklenmelidir.
- Otomatik devralma (`recover_interval`) varsayılan olarak kapalıdır; her tarama `RUNNING` run'ları listeler, çok büyük store'larda aralığı geniş tutun.
- İzleme arayüzünde kimlik doğrulama yoktur (yalnız localhost'a bağlayın ya da proxy arkasına alın); sayfa poll ile yenilenir, canlı akış yoktur.
- Zamanlama çözünürlüğü `schedule_poll_interval`'dır (saniyeli cron için onu da küçültün); DST geçişlerinde var olmayan/yinelenen duvar saatleri bir sonraki geçerli ana kayar. Adlandırılmış saat dilimleri (`tz=`) sistem tz veritabanını (yoksa `tzdata` paketini) ister; UTC için gerekmez.
- `overlap="skip"` çoklu worker'da en-iyi-çabadır: "önceki run bitti mi" kontrolü ile başlatma arasındaki dar yarışta nadiren bir fazla run başlayabilir. Aynı tick'in iki kez başlaması ise deterministik id sayesinde imkânsızdır.
- `reset` journal'ı geri sarar, yan etkileri değil: `to_op`'tan önceki adımlar yapılmış sayılır. Telafileri koşmuş bir run'da `force=True` ister ve silinen op'ların çocuk run'larını (ağacıyla) siler.
- Bir continue-as-new halkası tek başına resetlenemez (ardılını öksüz bırakırdı); zincirin canlı ucunu resetleyin.
- Özel `Store` uygulamaları: v0.7 ile `replace_events` eklendi ve `mark_signal_consumed` bir `consumed` parametresi aldı (paketle gelen üç backend güncel).
- Yol haritası: arayüzde canlı akış (SSE) ve run zaman çizelgesi, büyük adım sonuçları için harici blob depolama, workflow sorgu işleyicileri (`ctx.query`), resetlenen run'ın girdilerini/argümanlarını da değiştirebilme.
