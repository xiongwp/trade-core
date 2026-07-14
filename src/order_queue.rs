//! Kafka ingress routing and the fixed binary envelope stored in queue topics.

use crate::types::InstrumentId;
use crate::wire::{WireView, MSG_LEN};

const MAGIC: [u8; 4] = *b"TQ01";
pub const ENVELOPE_LEN: usize = 16 + MSG_LEN;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueRoute {
    pub topic: String,
    pub partition: i32,
    pub version: u32,
}

#[derive(Clone, Debug)]
pub struct QueueRouter {
    topics: Vec<String>,
    partitions_per_topic: u32,
    version: u32,
}

impl QueueRouter {
    pub fn new(topics: Vec<String>, partitions_per_topic: u32, version: u32) -> Self {
        assert!(
            !topics.is_empty(),
            "at least one Kafka queue topic is required"
        );
        assert!(partitions_per_topic > 0, "Kafka topics need partitions");
        Self {
            topics,
            partitions_per_topic,
            version,
        }
    }

    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    /// Stable category routing across multiple queue groups. A category stays
    /// on exactly one partition; changing this mapping requires a fenced route
    /// version transition.
    pub fn route(&self, category_id: u32) -> QueueRoute {
        let topic_count = self.topics.len() as u32;
        let topic = (category_id % topic_count) as usize;
        let partition = (category_id / topic_count) % self.partitions_per_topic;
        QueueRoute {
            topic: self.topics[topic].clone(),
            partition: partition as i32,
            version: self.version,
        }
    }
}

pub fn encode_envelope(user: u64, route_version: u32, frame: &[u8; MSG_LEN]) -> [u8; ENVELOPE_LEN] {
    let mut out = [0u8; ENVELOPE_LEN];
    out[..4].copy_from_slice(&MAGIC);
    out[4..8].copy_from_slice(&route_version.to_le_bytes());
    out[8..16].copy_from_slice(&user.to_le_bytes());
    out[16..].copy_from_slice(frame);
    out
}

pub struct QueueEnvelope<'a> {
    pub user: u64,
    pub route_version: u32,
    pub frame: &'a [u8; MSG_LEN],
}

impl<'a> QueueEnvelope<'a> {
    pub fn decode(bytes: &'a [u8]) -> Option<Self> {
        let bytes: &'a [u8; ENVELOPE_LEN] = bytes.try_into().ok()?;
        if bytes[..4] != MAGIC {
            return None;
        }
        let frame: &'a [u8; MSG_LEN] = (&bytes[16..]).try_into().ok()?;
        WireView::parse(frame)?;
        Some(Self {
            route_version: u32::from_le_bytes(bytes[4..8].try_into().ok()?),
            user: u64::from_le_bytes(bytes[8..16].try_into().ok()?),
            frame,
        })
    }

    pub fn instrument(&self) -> InstrumentId {
        WireView::parse(self.frame)
            .map(|view| view.instrument())
            .unwrap_or(InstrumentId(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::Order;
    use crate::types::{OrderId, Side};
    use crate::wire;

    #[test]
    fn categories_are_stable_across_multiple_queue_groups() {
        let router = QueueRouter::new(
            vec!["orders-g0".into(), "orders-g1".into(), "orders-g2".into()],
            64,
            7,
        );
        assert_eq!(router.route(0).topic, "orders-g0");
        assert_eq!(router.route(1).topic, "orders-g1");
        assert_eq!(router.route(3).partition, 1);
        assert_eq!(router.route(3), router.route(3));
        assert_eq!(router.route(3).version, 7);
    }

    #[test]
    fn envelope_round_trips_user_version_and_wire_frame() {
        let mut frame = [0; MSG_LEN];
        wire::encode_new(
            &Order::limit(OrderId(9), Side::Buy, 100, 2)
                .on(InstrumentId(42))
                .by(100_000),
            &mut frame,
        );
        let bytes = encode_envelope(100_000, 11, &frame);
        let decoded = QueueEnvelope::decode(&bytes).unwrap();
        assert_eq!(decoded.user, 100_000);
        assert_eq!(decoded.route_version, 11);
        assert_eq!(decoded.instrument(), InstrumentId(42));
        assert_eq!(decoded.frame, &frame);
    }
}
