# Sobre la Arquitectura del ejercicio

La arquitectura usada para el proyecto era una de tipo lambda, que se describe como una que se compone de una parte en job streaming y una parte de job de batch.

# Job de Streaming

Para simular la llegada de datos en tiempo real, se levantó un contenedor de docker que fue diseñado por el profesor del curso. Este contenedor fue levantado en una maquina
virtual en la nube y enviaba datos hacia una instancia de Kafka en la misma máquina. El docker podría haber estado en una maquina diferente o en local y aún así hubiera funcionado.

Kafka recibe lo datos separándolos por tópicos, el ejercicio usaba el topico "devices". El job de Streaming se trabajó en spark en local, recibiendo los datos desde el kafka en la 
máquina virtual.

Adicionalmente con el job provisioner en spark, se creó una tabla llena de metadatos y otras tablas a ser llenadas como resultado del ejercicio. Previamente se había levantado una
instancia de postgre en GCP para hacer esto. 

El job de Streaming en spark recibe los datos que está emitiendo kafka (topic devices), y los combina o enriquece con los datos en la tabla user_metadatos en el postgres de la nube.
El job realiza los calculos que se le piden, como por ejemplo el calculo de bytes transmitidos por antena, cada 5 minutos. Estos calculos los escribe en una tabla diferente en el
postgres de la nube, y además los escribe en un storage en local, que también podría haberse alojado en la nube.

# Job de Batch

El job de Batch toma los datos que dejó el job de Streaming, en en este caso en una carpeta en local, y vuelve a enriquecerlos con los datos del posgres, para luego realizar los
calculos que se le piden. El resultado se sube a postgres y además se graba en una carpeta diferente en local.

## Sobre el Ejercicio

Solo logré responder a la primera pregunta para ambos jobs (la agregación de bytes por antena). Tenía la idea de que hacerlo por usuario y por app en el caso del Job de Streaming
era solo cuestión de repetir la misma función que generaba los datos cambiando antena, por usuario y por app, pero no logré hacer que el job corra correctamente a partir de ahí.





