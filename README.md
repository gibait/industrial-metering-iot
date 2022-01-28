# industrial-metering-iot
## Smart Object Industrial Metering Scenario  

Il progetto prevede tre tipologie di Smart Object, ognuna dotata di un **sensore** e un **attuatore**.  
Ogni SO è associato a una data fornitura (*Acqua, Gas, Elettricità*)
presente in un **impianto** all'interno di una **location**. Ogni location ha più impianti e ogni impianto ha più forniture ma solo un device per tipo. 
Gli SO leggono i dati tramite il sensore e agiscono sul controllo (ON/OFF) della fornitura attraverso l'attuatore. Gli SO comunicano tramite protocollo applicativo *MQTT*, 
pubblicando i dati su un topic così composto:

*/metering/[location]/[plant]/[resource]/device/[unique_id]/telemetry*

I messaggi di telemetria vengono serializzati in **JSON** e utilizzano il formato **SenML**

Contestualmente all'emulazione degli SO agisce anche un **Policy Manager and Data Collector** che ha due funzioni principali:

- Controllare che le forniture gestite dagli SO non raggiungano la soglia critica
- Tenere traccia di tutti i dati ricevuti dai sensori per poterli analizzare successivamente

Nel caso in venga raggiunta la soglia critica il Policy Manager si occuperà di inviare un messagio di controllo sul topic 

*/metering/[location]/[plant]/[resource]/device/[unique_id]/control*

corrispondente al device che monitora tale fornitura, per azionare il controllo dell'attuatore e **interrompere** la fornitura. Durante il periodo in cui la fornitura 
è interrotta lo SO non effettua più misurazioni e, perciò, non invia dati.  
Una volta terminato il tempo di emulazione prefissato il PM&DC andrà a scrivere i record degli SO utilizzando la formattazione **CSV**  

All'interno del folder analysis è disponibile un **notebook jupyter** per graficare l'analisi dei **consumi** e dei **costi** relativi ad ogni device
e ad ogni famiglia di device.  

> I parametri di configurazione delle risorse, della configurazione per la connessione al broker MQTT e della configurazione del policy manager sono 
tutti situati all'interno della folder conf. I dati sui prezzi delle risorse sono invece definiti direttamente nel notebook.
