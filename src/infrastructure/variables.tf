variable project { default = "data-dev"}
variable region { default = "europe-west2" }
variable zone { default = "europe-west2-a" }

variable env { default = "dev" }

variable pubsub_topics {
  default = [
    "kafka-user-info"
  ]
  type    = set(string)
}

variable bq_raw_table_names {
  default = [
  ]
  type    = set(string)
}