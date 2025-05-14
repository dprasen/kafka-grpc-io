# src/kafka_grpc_io/config.py
import os
from typing import Optional, List

class Config:
    """
    Application configuration class.  This class loads configuration
    from environment variables, providing sensible defaults.
    """
    def __init__(self):
        """
        Initializes the configuration.
        """
        self.kafka_bootstrap_servers = self._get_env_list(
            "KAFKA_BOOTSTRAP_SERVERS", ["localhost:9092"]
        )
        self.grpc_server_port = int(os.environ.get("GRPC_SERVER_PORT", 50051))
        self.topic_prefix = os.environ.get("TOPIC_PREFIX", "my_app_")
        self.default_serialization_format = os.environ.get("DEFAULT_SERIALIZATION_FORMAT", "json").lower()  # Added default
        self.security_protocol = os.environ.get("SECURITY_PROTOCOL")  # e.g., SASL_PLAINTEXT, SSL
        self.sasl_mechanism = os.environ.get("SASL_MECHANISM")  # e.g., PLAIN, SASL-SCRAM-SHA-256
        self.sasl_username = os.environ.get("SASL_USERNAME")
        self.sasl_password = os.environ.get("SASL_PASSWORD")
        self.ssl_cafile = os.environ.get("SSL_CAFILE")
        self.ssl_certfile = os.environ.get("SSL_CERTFILE")
        self.ssl_keyfile = os.environ.get("SSL_KEYFILE")
        # Add a way to pass additional Kafka properties.  These will not have
        # their own attributes, but will be in a dictionary.
        self.kafka_config = self._get_kafka_config()

        # Validate the serialization format
        self._validate_serialization_format()

    def _get_env_list(self, env_var: str, default: List[str]) -> List[str]:
        """
        Helper method to get a list from an environment variable.
        Handles comma-separated values.
        """
        value = os.environ.get(env_var)
        if value:
            return [v.strip() for v in value.split(',')]
        return default

    def _get_kafka_config(self) -> dict:
        """
        Extracts Kafka-related configurations from environment variables
        into a dictionary.  This allows for more flexible configuration.
        """
        kafka_config = {}
        # Add any environment variables that start with 'KAFKA_'
        for k, v in os.environ.items():
            if k.startswith('KAFKA_'):
                # Convert to lower case and remove the prefix
                kafka_config_key = k.lower().replace('kafka_', '', 1)
                kafka_config[kafka_config_key] = v
        return kafka_config

    def _validate_serialization_format(self):
        """
        Validates that the configured serialization format is supported.
        """
        supported_formats = ["json", "protobuf", "avro", "flatbuffer", "sbe"]
        if self.default_serialization_format not in supported_formats:
            raise ValueError(f"Invalid serialization format: {self.default_serialization_format}.  Must be one of {supported_formats}")

    def get_kafka_config(self) -> dict:
        """
        Returns kafka config.
        """
        return {
            "bootstrap.servers": ",".join(self.kafka_bootstrap_servers),
            "security.protocol": self.security_protocol,
            "sasl.mechanism": self.sasl_mechanism,
            "sasl.username": self.sasl_username,
            "sasl.password": self.sasl_password,
            "ssl.ca.location": self.ssl_cafile,
            "ssl.certificate.location": self.ssl_certfile,
            "ssl.key.location": self.ssl_keyfile,
            **self.kafka_config
        }

    def __repr__(self):
        """
        Returns a string representation of the configuration.  This is helpful
        for debugging and logging.  It's important to exclude sensitive
        information like passwords.
        """
        return (f"Config(kafka_bootstrap_servers={self.kafka_bootstrap_servers}, "
                f"grpc_server_port={self.grpc_server_port}, "
                f"topic_prefix={self.topic_prefix}, "
                f"default_serialization_format={self.default_serialization_format}, "
                f"security_protocol={self.security_protocol}, "
                f"sasl_mechanism={self.sasl_mechanism}, "
                f"sasl_username={self.sasl_username}, "
                f"ssl_cafile={self.ssl_cafile}, "
                f"ssl_certfile={self.ssl_certfile}, "
                f"ssl_keyfile={self.ssl_keyfile}, "
                f"kafka_config={self.kafka_config})") # Exclude password