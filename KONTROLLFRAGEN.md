# Kontrollfragen

### 1. Welche Klasse sendet Nachrichten an ein Topic, und welche Annotation markiert die Empfängermethode?

Nachrichten sendet der `KafkaTemplate<K, V>` mit `send(topic, value)`. Im Projekt steckt er im Service `KafkaProducer`. Die Empfängermethode trägt die Annotation `@KafkaListener(topics = "...", groupId = "...")`, und Spring ruft sie für jede neue Nachricht auf.

### 2. Wie wird die Adresse des Kafka-Servers in Spring Boot konfiguriert?

Die Adresse steht in der `application.properties` unter `spring.kafka.bootstrap-servers=localhost:9092`. Spring Boot erzeugt daraus automatisch die Producer- und Consumer-Factories, sodass im Code keine Adresse vorkommt.

### 3. Warum ist die JSON-Serialisierung für strukturierte Nachrichten nötig?

Kafka speichert und überträgt nur Byte-Arrays und weiss nichts über Java-Objekte. Ein Serializer muss die `CustomMessage` deshalb vor dem Senden in ein Format wie JSON umwandeln, und ein Deserializer baut beim Empfangen wieder ein Objekt daraus. JSON ist dafür praktisch, weil es lesbar ist und auch Programme in anderen Sprachen es verstehen.

### 4. Was geschah in Teil 4 bei gleicher und bei unterschiedlicher group-id?

Bei **gleicher** group-id teilen sich beide Consumer die Arbeit: Kafka ordnet die Partitionen neu zu (Rebalancing), und jede Nachricht kommt nur bei einem der beiden an. Weil `testTopic` nur eine Partition hat, bekam der zweite Consumer alle Nachrichten, und der erste lief leer mit. Bei **unterschiedlicher** group-id hat jede Gruppe eigene Offsets, deshalb bekommen beide Consumer jede Nachricht. Wegen `earliest` hat der neue Consumer zuerst noch alle alten Nachrichten gelesen.

Der Unterschied: Innerhalb einer Gruppe wird die Last verteilt (Queue-Prinzip), zwischen Gruppen wird jede Nachricht an alle verteilt (Publish/Subscribe). Screenshots dazu stehen im [README](README.md#teil-4-zweiter-consumer).

### 5. Welche Strategien gibt es bei Fehlern beim Senden oder Empfangen?

- **Senden:** Der Producer wiederholt fehlgeschlagene Requests automatisch (`retries`, `delivery.timeout.ms`). Mit `acks=all` und Idempotenz (`enable.idempotence=true`) gehen dabei keine Nachrichten verloren, und es entstehen keine Duplikate. Das Ergebnis von `send()` lässt sich über das zurückgegebene `CompletableFuture` auswerten, zum Beispiel zum Loggen oder um Fehler zurückzumelden.
- **Empfangen:** Der `DefaultErrorHandler` versucht eine Nachricht mit einem `BackOff` mehrmals. Scheitert sie dauerhaft, schiebt ein `DeadLetterPublishingRecoverer` sie in ein **Dead Letter Topic** (z. B. `testTopic-dlt`), damit sie den Rest nicht blockiert. Mit `@RetryableTopic` gibt es auch nicht blockierende Retries über eigene Retry-Topics. Für kaputte Daten fängt ein `ErrorHandlingDeserializer` Deserialisierungsfehler ab, so wie es in diesem Projekt mit den alten String-Nachrichten passiert.

### 6. Wie lässt sich eine Kafka-Anwendung skalieren?

Man erhöht die Anzahl **Partitionen** eines Topics und startet mehrere Instanzen des Consumers mit **derselben group-id**. Kafka verteilt die Partitionen dann auf die Instanzen, wobei höchstens so viele Consumer aktiv arbeiten, wie es Partitionen gibt. Innerhalb einer Instanz hilft zusätzlich `concurrency` am Listener-Container. Auf der Broker-Seite skaliert man mit mehr Brokern im Cluster und einem Replikationsfaktor grösser als 1, was gleichzeitig die Ausfallsicherheit erhöht.
