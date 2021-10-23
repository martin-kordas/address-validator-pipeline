# address-validator-pipeline

PHP CLI skript pro validaci poštovních adres, využívající pro urychlení víceprocesorovou architekturu na bázi knihovny **ZeroMQ**. Poštovní adresy jsou čteny a ukládány do SQL databáze. Pro IPC komunikaci a přístup k databázi skript využívá asynchronní promise-based volání frameworku **ReactPHP**.

Záměrem repozitáře je kódově demonstrovat použití komunikačních vzorců knihovny ZeroMQ. **Záměrem není poskynout spustitelný kód**, jelikož jde o výsek rozsáhlejšího projektu s chybějícími návaznými funkcionalitami. Součástí repozitáře mmj. není implementace samotného validování adres.



## Architektura
- v základu lze ilustrovat nákresem: https://zguide.zeromq.org/docs/chapter1/#Divide-and-Conquer
 - **Ventilator**: načte data o adresách z databáze a rozdělí je mezi Workery
   - počet: 1 (základní proces)
   - PUSH socket: posílá data Workerům
   - PUB socket: signalizuje Workery/Sinky, aby skončily
 - **Worker**: provádí validaci jednotlivých adres
   - počet: `Runner::$workerCount`
   - PULL socket: přijímá data z Ventilatoru
   - PUSH socket: přijímá signál z Ventilatoru
 - **Sink**: ukládá údaje o validovaných adresách do db.
   - počet: pro každou AddressEntity je 1 Sink
   - PULL socket: přijímá data z Workerů
   - PUSH socket: přijímá signál z Ventilatoru
 - urychlení spočívá v tom, že:
   - více Workerů souběžně provádí validaci adres
   - více Sinků souběžně provádí uložení do db.

## Závislosti
- OS: Linux (kvůli reactphp/child-process)
- PHP extensions: zmq, pcntl (volitelně pro IPC signal handling)
- PHP Composer: react/child-process, react/mysql, react/filesystem, react/zmq, react/cache
