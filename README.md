# BookStore end-to-end Azure data engineering pipeline
Pipeline end-to-end con ingesta incremental desde Azure SQL hacia un Lakehouse en ADLS Gen2, utilizando Azure Databricks y Delta Lake bajo arquitectura Medallion (Landing, Raw, Silver, Gold).
Incluye gobierno de datos con Unity Catalog y Azure Key Vault, y expone capas Gold para explotacion de los datos.

<img width="1396" height="560" alt="Diagrama en blanco" src="https://github.com/user-attachments/assets/61f81688-4ba2-427b-aed5-c5959980f1f0" />

Este diseño comienza con la identificación de las fuentes externas de datos, en este caso Azure SQL Database donde se consideran tres tablas principales: Customers, Orders y Addresses.
La ingesta de datos se realiza de forma incremental, identificando registros nuevos y actualizados en cada una de las tablas (Change Data Capture), y es orquestada mediante Azure Data Factory, que transfiere la información hacia Azure Data Lake Storage Gen2 (ADLS).

La capa Landing actúa como zona de aterrizaje inicial, recibiendo archivos en formatos JSON y CSV. 

En el caso del procesamiento para las siguientes capas se utiliza Lakeflow con Spark Declarative Pipelines en Azure Databricks mediante Streaming, lo que permite definir flujos de datos de manera declarativa, asegurando trazabilidad, escalabilidad y una orquestación eficiente. El acceso a los datos almacenados en el Storage Account se gestiona mediante External Locations, utilizando Storage Credentials para la autenticación, lo que permite un acceso seguro y controlado bajo las políticas de gobierno definidas en Unity Catalog. 

Posteriormente, para la carga hacia la capa Raw, se utiliza Databricks Auto Loader, definiendo el schemaLocation y permitiendo almacenar los datos de forma eficiente y mantener un histórico completo de la información ingerida.

En la capa Silver, los datos son limpiados, transformados y enriquecidos, aplicando reglas de calidad de datos mediante Expectations, lo que garantiza consistencia y confiabilidad. Adicionalmente, en esta capa se implementan estrategias de Slowly Changing Dimensions (SCD), tanto Tipo 1 como Tipo 2, permitiendo actualizar registros y, cuando es necesario, preservar el historial de cambios.

En la capa Gold consume las tablas dimensionales y hechos, aplicando un modelo dimensional tipo estrella, dejando los datos listos para su consumo analítico, generación de reportes y visualización en herramientas de BI.

Finalmente, el gobierno y seguridad de los datos se gestionan de manera centralizada mediante Unity Catalog y Azure Key Vault, lo que garantiza control de permisos, trazabilidad y cumplimiento de buenas prácticas de seguridad.
