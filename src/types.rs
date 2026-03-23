//! Typed parameter values for journal entries and query binding.
//!
//! Maps directly to protobuf GraphValue — no JSON intermediary.

use crate::graphd as proto;
use serde::{Deserialize, Serialize};

/// Strongly-typed parameter value for journal entries and query binding.
/// Maps directly to protobuf GraphValue.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ParamValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    List(Vec<ParamValue>),
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
        ParamValue::List(items) => Value::ListValue(proto::ListValue {
            values: items.iter().map(param_value_to_graph_value).collect(),
        }),
    };
    proto::GraphValue { value: Some(value) }
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
    match gv.value.as_ref() {
        Some(proto::graph_value::Value::NullValue(_)) | None => ParamValue::Null,
        Some(proto::graph_value::Value::BoolValue(b)) => ParamValue::Bool(*b),
        Some(proto::graph_value::Value::IntValue(i)) => ParamValue::Int(*i),
        Some(proto::graph_value::Value::FloatValue(f)) => ParamValue::Float(*f),
        Some(proto::graph_value::Value::StringValue(s)) => ParamValue::String(s.clone()),
        Some(proto::graph_value::Value::ListValue(list)) => {
            ParamValue::List(list.values.iter().map(graph_value_to_param_value).collect())
        }
        // Map values in proto → flatten to null fallback (params don't use maps).
        _ => ParamValue::Null,
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

    #[test]
    fn test_param_value_roundtrip_null() {
        let pv = ParamValue::Null;
        let gv = param_value_to_graph_value(&pv);
        assert_eq!(graph_value_to_param_value(&gv), pv);
    }

    #[test]
    fn test_param_value_roundtrip_bool() {
        let pv = ParamValue::Bool(true);
        let gv = param_value_to_graph_value(&pv);
        assert_eq!(graph_value_to_param_value(&gv), pv);
    }

    #[test]
    fn test_param_value_roundtrip_int() {
        let pv = ParamValue::Int(42);
        let gv = param_value_to_graph_value(&pv);
        assert_eq!(graph_value_to_param_value(&gv), pv);
    }

    #[test]
    fn test_param_value_roundtrip_float() {
        let pv = ParamValue::Float(3.14);
        let gv = param_value_to_graph_value(&pv);
        assert_eq!(graph_value_to_param_value(&gv), pv);
    }

    #[test]
    fn test_param_value_roundtrip_string() {
        let pv = ParamValue::String("hello".into());
        let gv = param_value_to_graph_value(&pv);
        assert_eq!(graph_value_to_param_value(&gv), pv);
    }

    #[test]
    fn test_param_value_roundtrip_list() {
        let pv = ParamValue::List(vec![ParamValue::Int(1), ParamValue::Int(2)]);
        let gv = param_value_to_graph_value(&pv);
        assert_eq!(graph_value_to_param_value(&gv), pv);
    }

    #[test]
    fn test_map_entries_roundtrip() {
        let params = vec![
            ("name".into(), ParamValue::String("Alice".into())),
            ("age".into(), ParamValue::Int(30)),
        ];
        let entries = param_values_to_map_entries(&params);
        let back = map_entries_to_param_values(&entries);
        assert_eq!(params, back);
    }
}
