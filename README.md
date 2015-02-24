# Apache Kafka - Consumer/Producer

Caso não tenha um Apache Kafka instalado já, teremos que criar o nosso próprio.

Passo 1: Faça o download do Kafka versão 0.8.2.0 <a href="https://www.apache.org/dyn/closer.cgi?path=/kafka/0.8.2.0/kafka_2.10-0.8.2.0.tgz"> nesse link </a>

Após o Download descompacte o arquivo utilizando o comando:
> tar -xzf kafka_2.10-0.8.2.0.tgz

Entre na pasta do Kafka descompactada:
> cd kafka_2.10-0.8.2.0

Passo 2: Inicie o Servidor

O kafka utiliza o ZooKeeper então antes você precisa iniciar o ZooKeeper Server, caso não tenha um utilize o que vem no kafka para ter uma instancia dele.

Execute o comando (dentro da pasta do kafka)
> bin/zookeeper-server-start.sh config/zookeeper.properties

Agora inicie o seridor Kafka:
> bin/kafka-server-start.sh config/server.properties

Passo 3: Crie um topico

Nesse exemplo criei um topico com o nome "testeKafka" com uma partição simples e apenas uma replica:

> bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic testeKafka

Utilize o comando abaixo para listar o nosso topico criado:
> bin/kafka-topics.sh --list --zookeeper localhost:2181
testeKafka
