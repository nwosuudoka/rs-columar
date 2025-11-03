//! Positions
use columnar_derive::SimpleColumnar;
use serde::de::Error as SerdeError;
use serde::{self, Deserialize, Deserializer, Serialize};
use std::fmt::Display;
use std::str::FromStr;

#[derive(SimpleColumnar, Debug, Serialize, Deserialize, Default, PartialEq)]
pub struct Position {
    // // 16 bytes
    // #[serde(skip_deserializing, default)]
    // pub id: u64,

    // 8 bytes
    #[serde(rename = "rcid", default, deserialize_with = "empty_to_default")]
    pub rcid: i32,
    #[serde(
        rename = "industry_index",
        default,
        deserialize_with = "empty_to_default"
    )]
    pub industry_id: u16,

    #[serde(rename = "company_id", default, deserialize_with = "empty_to_default")]
    pub company_id: u32,
    #[serde(
        rename = "rics_k400_index",
        default,
        deserialize_with = "empty_to_default"
    )]
    pub rics_k400_index: u16,
    #[serde(
        rename = "rics_k50_index",
        default,
        deserialize_with = "empty_to_default"
    )]
    pub rics_k50_index: u8,
    #[serde(
        rename = "rics_k10_index",
        default,
        deserialize_with = "empty_to_default"
    )]
    pub rics_k10_index: u8,

    #[serde(rename = "prev_rcid", default, deserialize_with = "empty_to_default")]
    pub prev_rcid: i32,
    #[serde(rename = "new_rcid", default, deserialize_with = "empty_to_default")]
    pub next_rcid: i32,

    #[serde(
        rename = "startdate_index",
        default,
        deserialize_with = "empty_to_default"
    )]
    pub start_index: i16,
    #[serde(
        rename = "enddate_index",
        default,
        deserialize_with = "empty_to_default"
    )]
    pub end_index: i16,
    #[serde(
        rename = "next_startdate_index",
        default,
        deserialize_with = "empty_to_default"
    )]
    pub next_start_index: i16,
    #[serde(
        rename = "prev_enddate_index",
        default,
        deserialize_with = "empty_to_default"
    )]
    pub prev_end_index: i16,

    #[serde(rename = "weight", default, deserialize_with = "empty_to_default")]
    pub weight: f32,
    #[serde(
        rename = "sample_weight",
        default,
        deserialize_with = "empty_to_default"
    )]
    pub sample_weight: f32,
    #[serde(
        rename = "inflow_weight",
        default,
        deserialize_with = "empty_to_default"
    )]
    pub inflow_weight: f32,
    #[serde(
        rename = "outflow_weight",
        default,
        deserialize_with = "empty_to_default"
    )]
    pub outflow_weight: f32,
    #[serde(
        rename = "fulltime_prob",
        default,
        deserialize_with = "empty_to_default"
    )]
    pub full_time_prob: f32,

    #[serde(
        rename = "multiplicator",
        default,
        deserialize_with = "empty_to_default"
    )]
    pub multiplicator: f32,
    #[serde(rename = "inflation", default, deserialize_with = "empty_to_default")]
    pub inflation: f32,

    #[serde(rename = "comp_ratio", default, deserialize_with = "empty_to_default")]
    pub total_compensation_ratio: f32,
    #[serde(
        rename = "work_hours_per_year",
        default,
        deserialize_with = "empty_to_default"
    )]
    pub full_time_hours: f32,

    #[serde(
        rename = "estimated_us_log_salary",
        default,
        deserialize_with = "empty_to_default"
    )]
    pub estimated_us_log_salary: f32,
    #[serde(rename = "f_prob", default, deserialize_with = "empty_to_default")]
    pub f_prob: f32,

    #[serde(rename = "white_prob", default, deserialize_with = "empty_to_default")]
    pub white_prob: f32,
    #[serde(
        rename = "multiple_prob",
        default,
        deserialize_with = "empty_to_default"
    )]
    pub multiple_prob: f32,

    #[serde(rename = "black_prob", default, deserialize_with = "empty_to_default")]
    pub black_prob: f32,
    #[serde(rename = "api_prob", default, deserialize_with = "empty_to_default")]
    pub api_prob: f32,

    #[serde(
        rename = "hispanic_prob",
        default,
        deserialize_with = "empty_to_default"
    )]
    pub hispanic_prob: f32,
    #[serde(rename = "native_prob", default, deserialize_with = "empty_to_default")]
    pub native_prob: f32,

    #[serde(rename = "role_v3_id", default, deserialize_with = "empty_to_default")]
    pub role_v3_index: u16,
    #[serde(rename = "state_index", default, deserialize_with = "empty_to_default")]
    pub state: i16,
    #[serde(rename = "msa_index", default, deserialize_with = "empty_to_default")]
    pub msa: i16,
    #[serde(skip_serializing, skip_deserializing)]
    pub mapped_role: i16,
    #[serde(
        rename = "country_index",
        default,
        deserialize_with = "empty_to_default"
    )]
    pub country: i16,
    #[serde(
        rename = "region_index",
        default,
        deserialize_with = "empty_to_default"
    )]
    pub region: i16,
    #[serde(rename = "seniority", default, deserialize_with = "empty_to_default")]
    pub seniority: i16,
    #[serde(
        rename = "highest_degree_index",
        default,
        deserialize_with = "empty_to_default"
    )]
    pub highest_degree: i16,
    #[serde(skip_serializing, skip_deserializing)]
    pub internal_outflow: bool,
    #[serde(skip_serializing, skip_deserializing)]
    pub internal_inflow: bool,

    #[serde(
        rename = "skill_v3_id_list",
        deserialize_with = "deserialize_skill_list",
        default
    )]
    pub mapped_skills_v3: Vec<u16>,

    #[serde(rename = "description", default, deserialize_with = "empty_to_default")]
    pub description: String,
    #[serde(rename = "title_raw", default, deserialize_with = "empty_to_default")]
    pub raw_title: String,
}

/// Converts an empty string to the default value.
pub fn empty_to_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + FromStr,
    T::Err: Display,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    match opt.as_deref().map(str::trim) {
        Some("") | None => Ok(T::default()),
        Some(raw) => raw.parse::<T>().map_err(SerdeError::custom),
    }
}

pub fn deserialize_skill_list<'de, D>(deserializer: D) -> Result<Vec<u16>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(parse_skill_list::<u16>(&s))
}

pub fn parse_skill_list<T>(s: &str) -> Vec<T>
where
    T: FromStr,
{
    s.trim_matches(['[', ']'].as_ref())
        .split("|")
        .map(str::trim)
        .filter_map(|num| num.trim().parse::<T>().ok())
        .collect::<Vec<T>>()
}
