use std::collections::{HashSet, VecDeque};

use super::packets as protocol;

pub(super) struct PacketChain {
    pending: VecDeque<protocol::decode::DecodedServerMessage>,
    processed_message_ids: HashSet<protocol::MessageId>,
}

impl PacketChain {
    pub(super) fn new() -> Self {
        Self { pending: VecDeque::new(), processed_message_ids: HashSet::new() }
    }

    pub(super) fn push(&mut self, message: protocol::decode::DecodedServerMessage) {
        self.pending.push_back(message);
    }

    pub(super) fn drain_ready<F>(
        &mut self,
        mut has_resource: F,
    ) -> Vec<protocol::decode::DecodedServerMessage>
    where
        F: FnMut(protocol::MessageId) -> bool,
    {
        let mut ready = Vec::new();
        let mut scheduled_resources = HashSet::new();

        loop {
            let mut progressed = false;
            let mut remaining = VecDeque::new();

            while let Some(message) = self.pending.pop_front() {
                if !self.is_ready(&message, &scheduled_resources, &mut has_resource) {
                    remaining.push_back(message);
                    continue;
                }

                if let protocol::decode::DecodedServerMessage::Resource(resource) = &message {
                    scheduled_resources.insert(resource.id);
                }
                if let Some(message_id) = message_id(&message) {
                    self.processed_message_ids.insert(message_id);
                }
                ready.push(message);
                progressed = true;
            }

            self.pending = remaining;
            if !progressed {
                break;
            }
        }

        ready
    }

    fn is_ready<F>(
        &self,
        message: &protocol::decode::DecodedServerMessage,
        scheduled_resources: &HashSet<protocol::MessageId>,
        has_resource: &mut F,
    ) -> bool
    where
        F: FnMut(protocol::MessageId) -> bool,
    {
        dependency_id(message).is_none_or(|id| self.processed_message_ids.contains(&id))
            && required_resource_ids(message)
                .all(|resource_id| scheduled_resources.contains(&resource_id) || has_resource(resource_id))
    }
}

fn message_id(message: &protocol::decode::DecodedServerMessage) -> Option<protocol::MessageId> {
    match message {
        protocol::decode::DecodedServerMessage::Envelope(envelope) => envelope.id,
        protocol::decode::DecodedServerMessage::Resource(resource) => Some(resource.id),
    }
}

fn dependency_id(
    message: &protocol::decode::DecodedServerMessage,
) -> Option<protocol::MessageId> {
    match message {
        protocol::decode::DecodedServerMessage::Envelope(envelope) => envelope.dependency_id,
        protocol::decode::DecodedServerMessage::Resource(resource) => resource.dependency_id,
    }
}

fn required_resource_ids(
    message: &protocol::decode::DecodedServerMessage,
) -> impl Iterator<Item = protocol::MessageId> + '_ {
    match message {
        protocol::decode::DecodedServerMessage::Envelope(envelope) => {
            EitherRequiredResources::Packets(envelope.packets.iter().filter_map(required_resource_id))
        },
        protocol::decode::DecodedServerMessage::Resource(_) => {
            EitherRequiredResources::Empty(std::iter::empty())
        },
    }
}

fn required_resource_id(packet: &protocol::S2CPacket) -> Option<protocol::MessageId> {
    match packet {
        protocol::S2CPacket::ElementSetTexture { resource_id, .. } => Some(*resource_id),
        _ => None,
    }
}

enum EitherRequiredResources<I, E> {
    Packets(I),
    Empty(E),
}

impl<I, E> Iterator for EitherRequiredResources<I, E>
where
    I: Iterator<Item = protocol::MessageId>,
    E: Iterator<Item = protocol::MessageId>,
{
    type Item = protocol::MessageId;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Packets(iter) => iter.next(),
            Self::Empty(iter) => iter.next(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(
        id: Option<protocol::MessageId>,
        dependency_id: Option<protocol::MessageId>,
        packets: Vec<protocol::S2CPacket>,
    ) -> protocol::decode::DecodedServerMessage {
        protocol::decode::DecodedServerMessage::Envelope(protocol::decode::DecodedEnvelope {
            id,
            dependency_id,
            receipt_id: None,
            packets,
        })
    }

    fn resource(
        id: protocol::MessageId,
        dependency_id: Option<protocol::MessageId>,
    ) -> protocol::decode::DecodedServerMessage {
        protocol::decode::DecodedServerMessage::Resource(protocol::decode::DecodedResource {
            id,
            dependency_id,
            receipt_id: None,
            resource_type: "image/png".to_string(),
            usage_count: -1,
            blob: Vec::new(),
        })
    }

    #[test]
    fn resource_dependent_envelope_waits_until_resource_arrives() {
        let mut chain = PacketChain::new();
        chain.push(envelope(
            Some(10),
            None,
            vec![protocol::S2CPacket::ElementSetTexture { element_id: 1, resource_id: 99 }],
        ));
        chain.push(resource(99, None));

        let ready = chain.drain_ready(|_| false);
        assert_eq!(ready.len(), 2);
        assert!(matches!(
            ready[0],
            protocol::decode::DecodedServerMessage::Resource(_)
        ));
        assert!(matches!(
            ready[1],
            protocol::decode::DecodedServerMessage::Envelope(_)
        ));
    }

    #[test]
    fn blocked_chain_does_not_stop_unrelated_messages() {
        let mut chain = PacketChain::new();
        chain.push(envelope(
            Some(10),
            None,
            vec![protocol::S2CPacket::ElementSetTexture { element_id: 1, resource_id: 99 }],
        ));
        chain.push(envelope(Some(11), Some(10), vec![protocol::S2CPacket::Join {}]));
        chain.push(envelope(Some(12), None, vec![protocol::S2CPacket::Ping { nonce: 7 }]));

        let ready = chain.drain_ready(|_| false);
        assert_eq!(ready.len(), 1);
        assert!(matches!(
            ready[0],
            protocol::decode::DecodedServerMessage::Envelope(protocol::decode::DecodedEnvelope {
                id: Some(12),
                ..
            })
        ));
    }
}
