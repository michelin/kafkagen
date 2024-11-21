# Kafkagen

Kafkagen is a CLI that aims to ease testing by providing a simple way to create test scenarios and populate topics with records.
Records can be defined in JSON/YAML format or generated randomly for Avro schema using [avro-random-generator](https://github.com/confluentinc/avro-random-generator)

Records are automatically serialized according to the schema associated to the topic. Supported serializer are:
- KafkaAvroSerializer
- KafkaJsonSchemaSerializer
- KafkaProtobufSerializer
- StringSerializer

# Table of contents

# Install
Download the latest Windows/Linux release

Kafkagen requires some information to be defined globally:
- Comma-separated list of Kafka broker
- (Optional) Kafka user and password to setup the producer and be able to produce records
- (Optional) Schema registry URL to be able to retrieve subjects linked to the topic you want to produce in.


These variables are defined in a dedicated configuration file. This file can contain several clusters to let you choose the right one.

Create a ```.kafkagen/config.yml``` file in your home directory:

- Windows: C:\Users\Name\\.kafkagen\config.yml
- Linux: ~/.kafkagen/config.yml

Fill the config.yml file with the following content and update the environments configuration:
```yaml
kafkagen:
  contexts:
    - name: dev
      context:
        bootstrap-servers: localhost:9092
        registry-url: http://localhost:9095
    - name: env1
      context:
        bootstrap-servers: broker1,broker2,broker3
        security-protocol: SASL_SSL
        sasl-mechanism: SCRAM-SHA-512
        sasl-jaas-config: org.apache.kafka.common.security.scram.ScramLoginModule required username="myUsername" password="myPassword";
        registry-url: http://schema-registry:9095
        registry-username: myUsername
        registry-password: myPassword
        group-id-prefix: myGroupPrefix.
    - name: env2
      context:
        bootstrap-servers: broker1,broker2,broker3
        security-protocol: SASL_SSL
        sasl-mechanism: PLAIN
        sasl-jaas-config: org.apache.kafka.common.security.plain.PlainLoginModule required username="myUsername" password="myPassword";
        registry-url: http://schema-registry:9095
        partitioner-class: com.michelin.kafkagen.kafka.Crc32Partitioner
  current-context: dev
```
The application can be configured using the following properties:

- `bootstrap-servers`: The list of Kafka bootstrap servers.
- `security-protocol`: The security protocol to use, if any.
- `sasl-mechanism`: The SASL mechanism to use, if any.
- `sasl-jaas-cnfig`: The JAAS configuration for SASL, if any.
- `registry-url`: The URL of the schema registry, if any.
- `registry-username`: The username for the schema registry, if any.
- `registry-password`: The password for the schema registry, if any.
- `group-id-prefix`: The prefix for consumer group to use, if any.
- `partitioner-class`: The fully qualified class name of the custom partitioner to use, if any. This is particularly useful for .NET clients, as they use the CRC32 algorithm by default. In this case, set `com.michelin.kafkagen.kafka.Crc32Partitioner`

Please note that only `bootstrap-servers` is mandatory. All the other keys are optional

# Usage

# Sample
This command allows you to get a sample record of a topic and ease the dataset writing. Output format can be JSON or YAML

```
Usage: kafkagen sample [-dhpvV] [-f=<format>] [-s=<file>] <topic>

Description:

Create a sample record for the given topic

Parameters:
      <topic>             Name of the topic

Options:
  -d, --doc               Include fields doc as comment in JSON sample record
  -f, --format=<format>   Format (json or yaml) of the sample record
  -h, --help              Show this help message and exit.
  -p, --pretty            Include type specification for union field
  -s, --save=<file>       File to save the sample record
  -v, --verbose           Show more information about the execution
  -V, --version           Print version information and exit.
```

Example(s):
```console
kafkagen sample myTopic -f yaml
kafkagen sample myTopic -f yaml -s ./sample.yaml
kafkagen sample myTopic -d -p
```

# Dataset
This command allows you to get a dataset from an existing topic. Output format can be JSON or YAML

```
Usage: kafkagen dataset [-chpvV] [-f=<format>] [-s=<file>] -o=<String=Optional>... [-o=<String=Optional>...]... <topic>

Description:

Create a dataset for the given topic

Parameters:
      <topic>             Name of the topic

Options:
  -c, --compact           Compact the dataset to keep only the most recent
                            record for each key, removing tombstones
  -f, --format=<format>   Format (json or yaml) of the sample record
  -h, --help              Show this help message and exit.
  -k, --key=<key>
  -o, --offset=<String=Optional>...
                          Details of the messages offset to export. Ex: 0=0-10
                            1=8,10 2 will export messages from the partition 0
                            from offset 0 to 10, plus messages with offset 8
                            and 10 from the partition 1 and all the messages
                            from partition 2
  -p, --pretty            Include type specification for union field
  -s, --save=<file>       File to save the sample record
  -v, --verbose           Show more information about the execution
  -V, --version           Print version information and exit.
```

Example(s):
```console
kafkagen dataset myTopic -o 0=0-10 1=8,10 2=3 3 -f yaml
kafkagen dataset myTopic -o 0=0-10 -s ./sample.json
kafkagen dataset myTopic -c -o 0 -p -s ./sample.json
```

# Produce
This command allows you to produce records from a JSON/YAML dataset file.
The topic name can be passed:
- In the command line if records have to be published in a single topic (see usage below)
- In the dataset file if records have to be published in different topic (see `topic` field in the dataset definition)

You can either send avro or plain-text messages by using the `-p` (`--plain`) option. By default, if no registry in specified in the configuration, messages will be "String serialized"
```
Usage: kafkagen produce [-hvV] [-f=<file>] [<topic>]

Description:

Produce a dataset into a given topic

Parameters:
      [<topic>]       Name of the topic

Options:
  -f, --file=<file>   YAML/JSON File containing the dataset to insert
  -h, --help          Show this help message and exit.
  -v, --verbose       Show more information about the execution
  -V, --version       Print version information and exit.
```

Example(s):
```console
kafkagen produce myTopic -f ./datasets/datasetMyTopic.json
kafkagen produce myTopic -f ./datasets/datasetMyTopic.yaml
kafkagen produce -f ./datasets/datasetForMultipleTopics.yaml
```
# Play

This command allows you to play a scenario defined in a YAML file. A scenario contains one or several document based on a:
* Dataset file: produces raw datasets
* Template file: produce dataset from a template and variables to set
* Avro schema: random dataset generation ([avro-random-generator schema compatible](https://github.com/confluentinc/avro-random-generator#schema-annotations) to defined rules on each field)

You can either send avro or plain-text messages by using the `-p` (`--plain`) option. By default, if no registry in specified in the configuration, messages will be "String serialized".
```
Usage: kafkagen play [-hvV] [-f=<file>]

Description:

Play a scenario to insert dataset into a given topic

Options:
  -f, --file=<file>   YAML File or Directory containing YAML resources
  -h, --help          Show this help message and exit.
  -v, --verbose       Show more information about the execution
  -V, --version       Print version information and exit.
```

Example(s):
```console
kafkagen play -f ./scenarios/scenario1.yaml
```
# Assert
> **Disclaimer:** 
> Pretty format needed for the expected dataset. It means that, for instance, union field `{ "name": "field",
> "type": ["null", "string"], "default": null }` must be declared as 
> `"field": "value"` instead of `"field": { "string": "value" }`

This command allows you to check if records from a dataset are found in a topic.

The comparison can be done strictly or not. 
A strict assert checks that all the fields and the associated value are the same. 
A "lazy" assert only checks the fields defined in  the dataset (to focus the comparison on some interesting fields for instance)

```
Usage: kafkagen assert [-hsvV] [-t[=<startTimestamp>]] [-f=<file>] [<topic>]

Description:

Assert that a dataset exists in a topic

Parameters:
      [<topic>]       Name of the topic

Options:
  -f, --file=<file>   YAML/JSON File containing the dataset to assert
  -h, --help          Show this help message and exit.
  -s, --strict        True when message fields should be strictly checked (false to ignore unset fields)
  -t, --timestamp[=<startTimestamp>]
                      Timestamp milliseconds to start asserting from
  -v, --verbose       Show more information about the execution
  -V, --version       Print version information and exit.
```

Example(s):
```console
kafkagen assert myTopic -f ./datasets/datasetToFound.json
```

# Config
This command allows you to manage your Kafka contexts.
```
Usage: kafkagen config [-hV] <action> <context>

Description:

Manage configuration.

Parameters:
      <action>    Action to perform (get-contexts, current-context,
                    use-context).
      <context>   Context to use.

Options:
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Press [space] to restart, [e] to edit command line args (currently 'config -h'), [r] to resume testing, [o] Toggle test output, [h] for more options>

```
Example(s):

```console
kafkagen config get-contexts
kafkagen config use-context local
kafkagen config current-context
```

# Docker usage
Kafkagen provides also a Docker image. To run Kafkagen commands:
- Put the Kafkagen configuration in a variable with the target context
```bash
config=$(<~/.kafkagen/config.yml)
```
- Create a container and give the KAFKAGEN_CONFIG + your scenarios/datasets folder as volume
```bash
docker run -e KAFKAGEN_CONFIG="$config" -v ${PWD}/scenarios:/work/scenarios michelin/kafkagen:0.5.0 \
  ./kafkagen play -f ./scenarios/myScenario.yaml
```


# Scenario definition
Scenarios are defined in a yaml file (kubectl-like). Information needed in the scenario definition are:
- ```topic```: topic to populate
- ```datasetFile```, ```templateFile``` or ```avroFile```: datasetFile contains raw dataset to insert. templateFile contains template for the records.  avroFile contains the avro schema to use for random records generation
- ```maxInterval``` (optional): max interval for records production
- ```iterations``` (optional): number of records to produce for random generation

Examples:
- YAML dataset to insert in kafkagen-topic with a max interval of 500ms between each record
```yaml
---
apiVersion: v1
metadata:
  description: YAML dataset in kafkagen-topic
spec:
  topic: kafkagen-topic
  datasetFile: scenarios/datasets/data.yaml
  maxInterval: 500
```
- Mutiple documents:
  - YAML dataset to insert in kafkagen-topic
  - JSON dataset to insert in kafkagen-topic2 with both a max interval of 500ms between each record
```yaml
---
apiVersion: v1
metadata:
  description: kafkagen-topic YAML dataset
spec:
  topic: kafkagen-topic
  datasetFile: scenarios/datasets/data.yaml
  maxInterval: 500
---
apiVersion: v1
metadata:
  description: kafkagen-topic2 JSON dataset
spec:
  topic: kafkagen-topic2
  datasetFile: scenarios/datasets/data.json
  maxInterval: 500
```

- JSON template with variables given in the scenario definition.
```json
[
  {
    "key" : null,
    "value" : {
      "field1" : null,
      "field2" : null,
      "field3": 10
    }
  },
  {
    "key" : null,
    "value" : {
      "field1" : null,
      "field2" : null,
      "field3": 15
    }
  }
]
```
```yaml
---
apiVersion: v1
metadata:
  description: kafkagen-topic JSON template
spec:
  topic: kafkagen-topic
  templateFile: scenarios/template/template.json
  maxInterval: 500
  variables:
    key1:
      field1: value1_1
      field2: value2_1
    key2:
      field1: value1_2
      field2: value2_2
```

- 100 random records to produce in kafkagen-topic with a max interval of 500ms based on the myAvro.avsc avro schema (enhanced with avro-random-generator annotations)
```yaml
---
apiVersion: v1
metadata:
  description: Random record in kafkagen-topic
spec:
  topic: kafkagen-topic
  avroFile: scenarios/avro/myAvro.avro
  maxInterval: 500
  iterations: 100
```

# Datasets definition
Datasets can be defined in JSON or YAML files. A dataset file must contain an array of records.
A dataset record can:

| Field     | Description                                                                           | Mandatory |
|-----------|---------------------------------------------------------------------------------------|-----------|
| headers   | Record headers                                                                        | No        |
| key       | Record key                                                                            | No        |
| value     | Record value                                                                          | Yes       |
| topic     | The targeted topic (if the dataset contains record to be publish in different topics) | No        |
| timestamp | The record timestamp (ms). Default is the current timestamp                           | No        |


To ease the datasets writing, you can use the ```kafkagen sample``` command to get a sample for the topic based on the topic schema.

Examples:
- JSON record with all the possible fields (headers, record key, value, topic and timestamp)
```json
[
  {
    "topic": "myTopic",
    "headers": {
      "header1": "valueHeader1"
    },
    "timestamp": 709491600000,
    "key": {
      "keyField1": "keyValue1",
      "keyField2": "keyValue2"
    },
    "value": {
      "field1": "value1",
      "field2": "value2",
      "field3": 42
    }
  }
]
```

- JSON record with a String key and value
```json
[
  {
    "key": "keyValue",
    "value": {
      "field1": "value1",
      "field2": "value2",
      "field3": 42
    }
  }
]
```

- JSON records with only a value
```json
[
  {
    "value": {
      "field1": "value1",
      "field2": "value2",
      "field3": 42
    }
  },
  {
    "value": {
      "field1": "value1",
      "field2": "value2",
      "field3": 42
    }
  }
]
```

- YAML records with record key and value
```yaml
-
  key:
    keyField1: keyValue1
    keyField2: keyValue2
  value:
    field1: value1
    field2: value2
    field3: 42
-
  key:
    keyField1: keyValue1_1
    keyField2: keyValue2_1
  value:
    field1: value1_1
    field2: value2_1
    field3: 42
```
- JSON records targeting different topics
```json
[
  {
    "topic": "myTopic",
    "key": "keyValue",
    "value": {
      "field1": "value1",
      "field2": "value2",
      "field3": 42
    }
  },
  {
    "topic": "myOtherTopic",
    "value": {
      "field4": "value4",
      "field5": 25,
      "field6": 50
    }
  }
]
```
# AVRO timestamp
AVRO timestamp (date, time and dateTime) are defined as:
- int for date and time-millis logical types
- long for time-micros, timestamp-millis, timestamp-micros, local-timestamp-millis and local-timestamp-micros logical types

With Kafkagen, you can choose to use timestamp as human-readable strings. Kafkagen does the conversion to the expected int/long format.
Here is the expected format for the timestamp:
- date: "yyyy-MM-dd"
- time-millis: "HH:mm:ss.SSS" (11:12:01.123, 11:12:01, 11:12)
- time-micros: "HH:mm:ss.SSSSSS" (11:12:01.123456, 11:12:01, 11:12)
- timestamp-millis: "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" ("2007-12-03T10:15:30.100Z", "2007-12-03T10:15:30Z")
- timestamp-micros: "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'" ("2007-12-03T10:15:30.100123Z", "2007-12-03T10:15:30Z")
- local-timestamp-millis: "yyyy-MM-dd'T'HH:mm:ss.SSS" ("2007-12-03T10:15:30.100", "2007-12-03T10:15:30")
- local-timestamp-micros: "yyyy-MM-dd'T'HH:mm:ss.SSSSSS" ("2007-12-03T10:15:30.100123", "2007-12-03T10:15:30")
