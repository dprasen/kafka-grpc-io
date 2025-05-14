# src/kafka_grpc_io/grpc_service.py
import logging
from typing import Optional, AsyncIterator
import grpc
from confluent_kafka import Producer, Consumer, KafkaError
from . import kafka_grpc_io_pb2 as pb2
from . import kafka_grpc_io_pb2_grpc as pb2_grpc
from .config import Config
from .kafka_client import KafkaClient  # Import the KafkaClient
from concurrent.futures import ThreadPoolExecutor

class KafkaService(pb2_grpc.KafkaServiceServicer):
    """
    Implementation of the gRPC service for interacting with Kafka.
    """
    def __init__(self, config: Config, kafka_client: KafkaClient):
        """
        Initializes the KafkaService with a configuration and a Kafka client.
        """
        self.config = config
        self.kafka_client = kafka_client # Use the KafkaClient instance
        self.logger = logging.getLogger(__name__)

    async def Produce(self, request: pb2.ProduceRequest, context: grpc.ServicerContext) -> pb2.ProduceResponse:
        """
        Handles the Produce RPC call.
        """
        try:
            # Produce the message using the KafkaClient
            self.kafka_client.produce_message(
                topic=request.topic,
                key=request.key,
                value=request.value,
                headers=dict(request.headers) # convert to dict
            )
            # Construct a successful response
            response = pb2.ProduceResponse(offset=-1, partition=-1, success=True) # Set default values
            return response
        except KafkaError as e:
            self.logger.error(f"Error producing message: {e}")
            # Construct an error response
            response = pb2.ProduceResponse(error_message=str(e), success=False)
            return response
        except Exception as e:
            self.logger.exception("Unexpected error in Produce")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Unexpected error: {e}")
            return pb2.ProduceResponse(error_message="Internal server error", success=False)

    async def Consume(self, request: pb2.ConsumeRequest, context: grpc.ServicerContext) -> AsyncIterator[pb2.ConsumeResponse]:
        """
        Handles the Consume RPC call.  This is a server-streaming RPC.
        """
        def message_handler(msg_value: Any):
            """
            Handles each consumed message.  This inner function is called by
            the KafkaClient's consume_messages method.
            """
            try:
                # Construct the ConsumeResponse message
                response = pb2.ConsumeResponse(
                    key=msg_value.key(),
                    value=msg_value.value(),
                    offset=msg_value.offset(),
                    partition=msg_value.partition(),
                    headers=dict(msg_value.headers())
                )
                # Use yield to stream the response
                yield response
            except Exception as e:
                self.logger.error(f"Error processing consumed message: {e}")
                # Log the error and continue.  In a production system, you might
                # want to send an error message to a dead-letter topic or
                # use a different error handling strategy.
                yield pb2.ConsumeResponse(error_message="Error processing message") # send error
        try:
            # Consume messages using the Kafka client
            for msg in self.kafka_client.consume_messages(
                    topic=request.topic,
                    message_handler=message_handler,
                    num_messages=request.num_messages,
                    timeout=request.timeout_ms / 1000.0 if request.timeout_ms else None # convert ms to seconds
            ):
                yield msg

        except KafkaError as e:
            self.logger.error(f"Kafka error during consumption: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Kafka error: {e}")
            yield pb2.ConsumeResponse(error_message=str(e))  # Send error
        except Exception as e:
            self.logger.exception("Unexpected error in Consume")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Unexpected error: {e}")
            yield pb2.ConsumeResponse(error_message="Internal server error") # send error

async def serve(config: Config, kafka_client: KafkaClient) -> None:
    """
    Starts the gRPC server.
    """
    server = grpc.aio.server(ThreadPoolExecutor(max_workers=10)) # Use async server
    pb2_grpc.add_KafkaServiceServicer_to_server(KafkaService(config, kafka_client), server)
    server.add_insecure_port(f"[::]:{config.grpc_server_port}")
    await server.start()
    logging.info(f"gRPC server started on port {config.grpc_server_port}")
    await server.wait_for_termination()
    logging.info("gRPC server stopped")