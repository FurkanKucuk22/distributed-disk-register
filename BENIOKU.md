# HaToKuSe (Hata-Tolere Kuyruk Servisi)
HaToKuSe, Java tabanlı, dağıtık (distributed), hataya dayanıklı (fault-tolerant) ve ölçeklenebilir bir anahtar-değer (key-value) depolama sistemidir.

İstemciler ile TCP Soket üzerinden haberleşen bir Lider sunucu ve verileri depolayan Aile Üyeleri (Nodes) arasında gRPC protokolü kullanan hibrit bir mimariye sahiptir.

## 🚀 Özellikler
 **Dağıtık Mimari:** Veriler sisteme katılan düğümler arasında dengeli bir şekilde (Load Balancing) dağıtılır.

**Hata Toleransı (Fault Tolerance):** tolerance.conf dosyasında belirtilen sayı kadar (N) yedekleme yapılır. Düğümlerden biri çökse bile veri kaybolmaz.

### Kalıcılık (Persistence):

**Lider:** Mesaj dağılım haritasını (distribution.log) diske yazar. Kapanıp açılsa bile kimde ne olduğunu hatırlar.

**Üyeler:** Mesajları diskte (messages_PORT klasörlerinde) saklar. Yeniden başladıklarında verileri hafızaya yüklerler.

**Thread-Safe & Concurrency:** Lider sunucu aynı anda birden fazla istemciye (Multi-client) hizmet verebilir. Yarış durumlarına (Race Condition) karşı korumalıdır.

**Dinamik Üyelik:** Sisteme çalışma zamanında yeni üyeler katılabilir.

**İzole Disk Yapısı:** Aynı makinede test edilebilmesi için her düğüm kendi portuna özel klasör kullanır (messages/messages_5556 vb.).

## 🛠️ Kurulum ve Gereksinimler
Java JDK 11 veya üzeri.

Maven veya Gradle (gRPC bağımlılıkları için).

Protobuf Compiler (Proje derlenirken otomatik çalışır).

## Yapılandırma (tolerance.conf)
Proje ana dizininde tolerance.conf adında bir dosya oluşturun ve hata tolerans seviyesini belirleyin:

```
TOLERANCE=2
```

(Bu ayar, her mesajın kaç farklı sunucuda yedekleneceğini belirler.)

## ▶️ Nasıl Çalıştırılır?
Sistemi ayağa kaldırmak için aşağıdaki sırayı takip ediniz:

### 1. Lider Sunucuyu Başlatın
   Lider sunucu varsayılan olarak 5555 portunu kullanır ve istemcileri 6666 portundan dinler.
   
```
mvn exec:java -Dexec.mainClass=com.example.family.NodeMain
```

**Çıktı:** Node started on 127.0.0.1:5555

**Çıktı:** Leader listening for text on TCP 127.0.0.1:6666

### 2. Aile Üyelerini (Nodes) Başlatın
   Farklı terminallerde aynı komutu çalıştırarak sisteme yeni üyeler ekleyebilirsiniz. Sistem otomatik olarak boş bir port (5556, 5557...) bulacaktır.
```
mvn exec:java -Dexec.mainClass=com.example.family.NodeMain
```
 **Çıktı:** Node started on 127.0.0.1:5556

 **Çıktı:** Joined through 127.0.0.1:5555...

## 3. İstemci Bağlantısı (Client)
   Sisteme veri göndermek için Telnet veya proje içindeki Test Araçlarını kullanabilirsiniz.

Telnet ile Manuel Test:
```
telnet 127.0.0.1 6666
```
Komutlar:

**`SET <id> <mesaj>` : Veri kaydeder.**

**`GET <id>` : Veri okur.**

**`STATS` : Liderden yük dağılım raporunu ister.**

Örnek:
```
SET 100 MerhabaDunya
GET 100
```
## 🧪 Test Araçları
Proje içerisinde sistemin performansını ve dayanıklılığını ölçmek için hazır test sınıfları bulunmaktadır:

**TestClient.java:** Sisteme seri halde 1000 adet mesaj gönderir. Temel fonksiyonellik testi içindir.

**ConcurrencyTest.java:** Aynı anda 3 (veya daha fazla) istemci ile bağlanıp sisteme yük bindirir. Thread-safety kontrolü yapar.

**PerformanceTest.java:** 1 MB boyutunda büyük veriyi SET ve GET ederek süreyi (milisaniye) ölçer.

Çalıştırmak için IDE üzerinden ilgili dosyayı Run ediniz.

### 📁 Proje Dizin Yapısı
```
DISTRIBUTED-DISK-REGISTER/
├── src/
│ └── main/
│ ├── java/com/example/family/
│ │ ├── SetGetCommand/
│ │ │ ├── Command.java
│ │ │ ├── CommandParser.java
│ │ │ ├── DataStore.java
│ │ │ ├── GetCommand.java
│ │ │ └── SetCommand.java
│ │ ├── Tests/
│ │ │ ├── ConcurrencyTest.java
│ │ │ ├── PerformanceTest.java
│ │ │ └── TestClient.java
│ │ ├── FamilyServiceImpl.java
│ │ ├── MessageReplicaTracker.java
│ │ ├── NodeMain.java
│ │ ├── NodeRegistry.java
│ │ ├── StorageServiceImpl.java
│ │ └── ToleranceConfig.java
│ └── proto/
│ │ └── family.proto
├── messages/
│ ├── messages_5555/
│ ├── messages_5556/
│ ├── messages_5557/
│ ├── messages_5558/
│ └── messages_5559/
├── target/
├── pom.xml
├── tolerance.conf
└── distribution.log
```

## ⚠️ Kritik Notlar
**Crash Testi: Bir üye (Node) kapatıldığında, Lider bunu fark eder. Eğer tolerans seviyesi uygunsa, veri diğer yedek üyeden çekilir.**

**Restart: Lider sunucu kapatılıp açıldığında distribution.log dosyasını okuyarak hafızasını tazeler. Veri kaybı yaşanmaz.**

**Strict Consistency: Bir SET işlemi, ancak tolerance.conf dosyasındaki sayı kadar üyeye başarıyla yazıldığında istemciye "OK" döner. Aksi halde "ERROR" döner.**
