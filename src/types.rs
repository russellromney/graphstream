//! Typed parameter values for journal entries and query binding.
//!
//! Maps directly to protobuf GraphValue — no JSON intermediary.

use crate::graphd as proto;
use serde::{Deserialize, Serialize};

/// Strongly-typed parameter value for journal entries and query binding.
/// Maps directly to protobuf GraphValue.
///
/// The variant set is designed to faithfully represent every Bolt-native
/// value type that can appear as a mutation parameter, so a follower replaying
/// from the journal binds the same logical value the leader executed with.
/// Temporal variants carry ISO 8601 strings; the replay layer is responsible
/// for parsing these back into the target backend's native temporal type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ParamValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<ParamValue>),
    Map(Vec<(String, ParamValue)>),
    /// ISO 8601 date: `YYYY-MM-DD`.
    Date(String),
    /// Time with UTC offset: `HH:MM:SS[.fff]±HH:MM`.
    Time(String),
    /// Time without zone: `HH:MM:SS[.fff]`.
    LocalTime(String),
    /// Date-time with offset/zone: `YYYY-MM-DDTHH:MM:SS[.fff]±HH:MM`.
    DateTime(String),
    /// Date-time without zone: `YYYY-MM-DDTHH:MM:SS[.fff]`.
    LocalDateTime(String),
    /// ISO 8601 duration: `P1Y2M3DT4H5M6S`.
    Duration(String),
    /// 2D or 3D spatial point (z is None for 2D, Some for 3D).
    Point {
        srid: u32,
        x: f64,
        y: f64,
        z: Option<f64>,
    },
}

// ─── ParamValue ↔ protobuf conversions ───

pub fn param_value_to_graph_value(v: &ParamValue) -> proto::GraphValue {
    use proto::graph_value::Value;
    let value = match v {
        ParamValue::Null => Value::NullValue(proto::NullValue {}),
        ParamValue::Bool(b) => Value::BoolValue(*b),
        ParamValue::Int(i) => Value::IntValue(*i),
        ParamValue::Float(f) => Value::FloatValue(*f),
        ParamValue::String(s) => Value::StringValue(s.clone()),
        ParamValue::Bytes(b) => Value::BytesValue(b.clone()),
        ParamValue::List(items) => Value::ListValue(proto::ListValue {
            values: items.iter().map(param_value_to_graph_value).collect(),
        }),
        ParamValue::Map(entries) => Value::MapValue(proto::MapValue {
            entries: entries
                .iter()
                .map(|(k, v)| proto::MapEntry {
                    key: k.clone(),
                    value: Some(param_value_to_graph_value(v)),
                })
                .collect(),
        }),
        ParamValue::Date(s) => temporal(proto::TemporalKind::Date, s),
        ParamValue::Time(s) => temporal(proto::TemporalKind::Time, s),
        ParamValue::LocalTime(s) => temporal(proto::TemporalKind::LocalTime, s),
        ParamValue::DateTime(s) => temporal(proto::TemporalKind::Datetime, s),
        ParamValue::LocalDateTime(s) => temporal(proto::TemporalKind::LocalDatetime, s),
        ParamValue::Duration(s) => temporal(proto::TemporalKind::Duration, s),
        ParamValue::Point { srid, x, y, z } => Value::PointValue(proto::PointValue {
            srid: *srid,
            x: *x,
            y: *y,
            z: *z,
        }),
    };
    proto::GraphValue { value: Some(value) }
}

fn temporal(kind: proto::TemporalKind, iso: &str) -> proto::graph_value::Value {
    proto::graph_value::Value::TemporalValue(proto::TemporalValue {
        kind: kind as i32,
        iso: iso.to_string(),
    })
}

pub fn param_values_to_map_entries(params: &[(String, ParamValue)]) -> Vec<proto::MapEntry> {
    params
        .iter()
        .map(|(k, v)| proto::MapEntry {
            key: k.clone(),
            value: Some(param_value_to_graph_value(v)),
        })
        .collect()
}

pub fn graph_value_to_param_value(gv: &proto::GraphValue) -> ParamValue {
    use proto::graph_value::Value;
    match gv.value.as_ref() {
        Some(Value::NullValue(_)) | None => ParamValue::Null,
        Some(Value::BoolValue(b)) => ParamValue::Bool(*b),
        Some(Value::IntValue(i)) => ParamValue::Int(*i),
        Some(Value::FloatValue(f)) => ParamValue::Float(*f),
        Some(Value::StringValue(s)) => ParamValue::String(s.clone()),
        Some(Value::BytesValue(b)) => ParamValue::Bytes(b.clone()),
        Some(Value::ListValue(list)) => {
            ParamValue::List(list.values.iter().map(graph_value_to_param_value).collect())
        }
        Some(Value::MapValue(map)) => ParamValue::Map(
            map.entries
                .iter()
                .map(|e| {
                    let v = e
                        .value
                        .as_ref()
                        .map_or(ParamValue::Null, graph_value_to_param_value);
                    (e.key.clone(), v)
                })
                .collect(),
        ),
        Some(Value::TemporalValue(t)) => {
            let kind = proto::TemporalKind::try_from(t.kind)
                .unwrap_or(proto::TemporalKind::TemporalUnspecified);
            match kind {
                proto::TemporalKind::Date => ParamValue::Date(t.iso.clone()),
                proto::TemporalKind::Time => ParamValue::Time(t.iso.clone()),
                proto::TemporalKind::LocalTime => ParamValue::LocalTime(t.iso.clone()),
                proto::TemporalKind::Datetime => ParamValue::DateTime(t.iso.clone()),
                proto::TemporalKind::LocalDatetime => ParamValue::LocalDateTime(t.iso.clone()),
                proto::TemporalKind::Duration => ParamValue::Duration(t.iso.clone()),
                proto::TemporalKind::TemporalUnspecified => ParamValue::String(t.iso.clone()),
            }
        }
        Some(Value::PointValue(p)) => ParamValue::Point {
            srid: p.srid,
            x: p.x,
            y: p.y,
            z: p.z,
        },
    }
}

pub fn map_entries_to_param_values(entries: &[proto::MapEntry]) -> Vec<(String, ParamValue)> {
    entries
        .iter()
        .map(|e| {
            let value = e
                .value
                .as_ref()
                .map_or(ParamValue::Null, graph_value_to_param_value);
            (e.key.clone(), value)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(pv: ParamValue) {
        let gv = param_value_to_graph_value(&pv);
        assert_eq!(graph_value_to_param_value(&gv), pv);
    }

    #[test]
    fn test_roundtrip_scalars() {
        roundtrip(ParamValue::Null);
        roundtrip(ParamValue::Bool(true));
        roundtrip(ParamValue::Int(42));
        roundtrip(ParamValue::Float(3.14));
        roundtrip(ParamValue::String("hello".into()));
    }

    #[test]
    fn test_roundtrip_bytes() {
        roundtrip(ParamValue::Bytes(vec![0, 1, 2, 255]));
        roundtrip(ParamValue::Bytes(vec![]));
    }

    #[test]
    fn test_roundtrip_list() {
        roundtrip(ParamValue::List(vec![
            ParamValue::Int(1),
            ParamValue::Int(2),
        ]));
    }

    #[test]
    fn test_roundtrip_map() {
        roundtrip(ParamValue::Map(vec![
            ("a".into(), ParamValue::Int(1)),
            ("b".into(), ParamValue::String("s".into())),
        ]));
    }

    #[test]
    fn test_roundtrip_nested_map_in_list() {
        roundtrip(ParamValue::List(vec![ParamValue::Map(vec![(
            "k".into(),
            ParamValue::Int(1),
        )])]));
    }

    #[test]
    fn test_roundtrip_temporal() {
        roundtrip(ParamValue::Date("2024-01-15".into()));
        roundtrip(ParamValue::DateTime("2024-01-15T10:00:00+00:00".into()));
        roundtrip(ParamValue::LocalDateTime("2024-01-15T10:00:00".into()));
        roundtrip(ParamValue::Time("10:00:00+00:00".into()));
        roundtrip(ParamValue::LocalTime("10:00:00".into()));
        roundtrip(ParamValue::Duration("P1Y2M3DT4H5M6S".into()));
    }

    #[test]
    fn test_roundtrip_point() {
        roundtrip(ParamValue::Point {
            srid: 4326,
            x: 1.0,
            y: 2.0,
            z: None,
        });
        roundtrip(ParamValue::Point {
            srid: 9157,
            x: 1.0,
            y: 2.0,
            z: Some(3.0),
        });
    }

    #[test]
    fn test_map_entries_roundtrip() {
        let params = vec![
            ("name".into(), ParamValue::String("Alice".into())),
            ("age".into(), ParamValue::Int(30)),
            ("ts".into(), ParamValue::DateTime("2024-01-15T10:00:00+00:00".into())),
        ];
        let entries = param_values_to_map_entries(&params);
        let back = map_entries_to_param_values(&entries);
        assert_eq!(params, back);
    }
}
