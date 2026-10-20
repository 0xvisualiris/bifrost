use std::collections::{BTreeMap, BTreeSet};

use serde_json::json;

use hue::stream::HueStreamLightsV2;
use hue::zigbee::{
    EntertainmentZigbeeStream, HueEntFrameLightRecord, LightRecordMode,
    PHILIPS_HUE_ZIGBEE_VENDOR_ID,
};
use z2m::request::Z2mRequest;
use zcl::attr::ZclDataType;

use crate::backend::z2m::websocket::Z2mWebSocket;
use crate::error::ApiResult;

pub struct EntStream {
    pub stream: EntertainmentZigbeeStream,
    pub addrs: BTreeMap<String, Vec<u16>>,
    pub channels: BTreeMap<u8, (u16, LightRecordMode)>,
}

impl EntStream {
    #[must_use]
    pub fn new(
        counter: u32,
        addrs: BTreeMap<String, Vec<u16>>,
        channels: BTreeMap<u8, (u16, LightRecordMode)>,
    ) -> Self {
        Self {
            stream: EntertainmentZigbeeStream::new(counter),
            addrs,
            channels,
        }
    }

    /// Pick the virtual zigbee address for segment `index` of a light.
    ///
    /// Segment 0 uses the light's own network address. Other segments start
    /// at `network_address + index` and skip every address in `used` (other
    /// devices on the network, or segments already allocated), since a light
    /// ignores a segment mapping that contains an address already in use.
    #[must_use]
    pub fn allocate_segment_addr(
        network_address: u16,
        index: u16,
        used: &mut BTreeSet<u16>,
    ) -> u16 {
        if index == 0 {
            used.insert(network_address);
            return network_address;
        }

        let mut addr = network_address.wrapping_add(index);
        // 0x0000 is the coordinator, 0xFFF8.. are reserved/broadcast addresses
        while addr == 0x0000 || addr >= 0xFFF8 || used.contains(&addr) {
            log::debug!("Segment address {addr:04x} is in use, skipping");
            addr = addr.wrapping_add(1);
        }

        used.insert(addr);
        addr
    }

    #[must_use]
    pub fn z2m_set_entertainment_brightness(brightness: u8) -> Z2mRequest<'static> {
        Z2mRequest::Write {
            cluster: EntertainmentZigbeeStream::CLUSTER,
            payload: json!({
                EntertainmentZigbeeStream::CMD_LIGHT_BALANCE.to_string(): {
                    "manufacturerCode": PHILIPS_HUE_ZIGBEE_VENDOR_ID,
                    "type": ZclDataType::ZclU8 as u8,
                    "value": brightness,
                }
            }),
        }
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    #[must_use]
    pub fn generate_frame(&self, frame: &HueStreamLightsV2) -> Vec<HueEntFrameLightRecord> {
        let mut blks = vec![];
        match frame {
            HueStreamLightsV2::Rgb(rgb) => {
                for light in rgb {
                    let (xy, bright) = light.rgb.to_xy();

                    let brightness = (bright / 255.0 * 2047.0).clamp(1.0, 2047.0) as u16;
                    if let Some((chan, mode)) = self.channels.get(&light.channel) {
                        let raw = xy.to_quant();
                        let lrec = HueEntFrameLightRecord::new(*chan, brightness, *mode, raw);
                        blks.push(lrec);
                    }
                }
            }
            HueStreamLightsV2::Xy(xy) => {
                for light in xy {
                    let (xy, bright) = light.xy.to_xy();

                    let brightness = (bright / 255.0 * 2047.0).clamp(1.0, 2047.0) as u16;
                    if let Some((chan, mode)) = self.channels.get(&light.channel) {
                        let raw = xy.to_quant();
                        let lrec = HueEntFrameLightRecord::new(*chan, brightness, *mode, raw);
                        blks.push(lrec);
                    }
                }
            }
        }

        blks
    }

    pub async fn start_stream(&mut self, z2mws: &mut Z2mWebSocket) -> ApiResult<()> {
        log::debug!("Entertainment addrs: {:#?}", &self.addrs);
        log::debug!("Entertainment channels: {:#?}", &self.channels);
        for (dev, segments) in &self.addrs {
            let z2mreq = Self::z2m_set_entertainment_brightness(0xFE);
            z2mws.send(dev, &z2mreq).await?;

            if segments.len() <= 1 {
                continue;
            }

            let mapping = self.stream.segment_mapping(segments)?;
            z2mws.send_zigbee_message(dev, &mapping).await?;
        }

        Ok(())
    }

    pub async fn reset_stream(&mut self, z2mws: &mut Z2mWebSocket) -> ApiResult<()> {
        let reset = self.stream.reset()?;
        for topic in self.addrs.keys() {
            log::debug!("Sending reset to {topic}");
            z2mws.send_zigbee_message(topic, &reset).await?;
        }

        Ok(())
    }

    pub async fn frame(
        &mut self,
        z2mws: &mut Z2mWebSocket,
        frame: &HueStreamLightsV2,
    ) -> ApiResult<()> {
        let blks = self.generate_frame(frame);

        // Hue lights repeat every entertainment frame they receive in
        // broadcast mode, and drop frames with a counter they have already
        // seen. Each device therefore needs its own counter value, otherwise
        // only the first device in a cycle picks up its frame.
        for (dev, segments) in &self.addrs {
            let dev_blks: Vec<_> = blks
                .iter()
                .filter(|blk| segments.contains(&blk.addr()))
                .cloned()
                .collect();

            if !dev_blks.is_empty() {
                let message = self.stream.frame(dev_blks)?;
                z2mws.send_entertainment_frame(dev, &message).await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::EntStream;

    #[test]
    fn segment_addr_uses_network_address_for_first_segment() {
        let mut used = BTreeSet::from([0x9288]);

        let addr = EntStream::allocate_segment_addr(0x9288, 0, &mut used);
        assert_eq!(addr, 0x9288);
    }

    #[test]
    fn segment_addr_follows_network_address_when_free() {
        let mut used = BTreeSet::from([0x0bfb]);

        let addr = EntStream::allocate_segment_addr(0x0bfb, 1, &mut used);
        assert_eq!(addr, 0x0bfc);
        let addr = EntStream::allocate_segment_addr(0x0bfb, 2, &mut used);
        assert_eq!(addr, 0x0bfd);
    }

    #[test]
    fn segment_addr_skips_addresses_in_use() {
        // 0x9289 belongs to another device on the network
        let mut used = BTreeSet::from([0x9288, 0x9289]);

        let addr = EntStream::allocate_segment_addr(0x9288, 1, &mut used);
        assert_eq!(addr, 0x928a);
        let addr = EntStream::allocate_segment_addr(0x9288, 2, &mut used);
        assert_eq!(addr, 0x928b);
    }

    #[test]
    fn segment_addr_skips_reserved_and_coordinator_addresses() {
        let mut used = BTreeSet::from([0xfff6]);

        let addr = EntStream::allocate_segment_addr(0xfff6, 1, &mut used);
        assert_eq!(addr, 0xfff7);
        let addr = EntStream::allocate_segment_addr(0xfff6, 2, &mut used);
        assert_eq!(addr, 0x0001);
    }
}
