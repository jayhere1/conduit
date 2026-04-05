//! Property-based tests for protobuf message decoding.
//!
//! Ensures that attempting to decode random bytes as protobuf messages
//! never panics — it must always return `Ok` or `Err`.

use prost::Message;
use proptest::prelude::*;

use conduit_distributed::generated_proto;

proptest! {
    #[test]
    fn test_proto_decode_never_panics(data in proptest::collection::vec(any::<u8>(), 0..500)) {
        // Try decoding random bytes as several proto message types.
        // Each call must return Ok or Err, never panic.
        let bytes = prost::bytes::Bytes::from(data);

        let _ = generated_proto::RegisterRequest::decode(bytes.clone());
        let _ = generated_proto::TaskAssignment::decode(bytes.clone());
        let _ = generated_proto::TaskResult::decode(bytes.clone());
        let _ = generated_proto::WorkerHeartbeat::decode(bytes.clone());
        let _ = generated_proto::TaskLogEntry::decode(bytes.clone());
        let _ = generated_proto::ClusterStatusResponse::decode(bytes.clone());
        let _ = generated_proto::Ack::decode(bytes);
    }
}
